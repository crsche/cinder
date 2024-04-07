#![feature(let_chains, generic_arg_infer, exit_status_error)]
#[macro_use]
extern crate tracing;
use std::{
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Result};
use async_zip::base::read::stream::ZipFileReader;
use clap::Parser;
use deadpool_postgres::{Config, ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime};
use futures::{
    stream::{self, TryStreamExt},
    StreamExt,
};
use human_bytes::human_bytes;
use lazy_static::lazy_static;
use reqwest::{Client, ClientBuilder, Url};
use scraper::{Html, Selector};
use tempfile::{tempdir, TempDir};
use tokio::{
    fs::{create_dir_all, OpenOptions},
    io,
    io::{AsyncWriteExt, BufReader as TokioBufReader, BufWriter as TokioBufWriter},
    process::Command,
};
use tokio_postgres::NoTls;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing_subscriber::EnvFilter;
use which::which;

lazy_static! {
    static ref NCES: Url = Url::parse("https://nces.ed.gov").unwrap();
    static ref IPEDS: Url = NCES
        .join("ipeds/use-the-data/download-access-database")
        .unwrap();
    static ref SEL_MDB_LINK: Selector =
        Selector::parse("table.ipeds-table a[href$='.zip']").unwrap();
    static ref MDB_EXPORT: PathBuf =
        which("mdb-export").expect("'mdb-export' command not in $PATH");
    static ref MDB_TABLES: PathBuf =
        which("mdb-tables").expect("'mdb-tables' command not in $PATH");
    static ref MDB_SCHEMA: PathBuf =
        which("mdb-schema").expect("'mdb-schema' command not in $PATH");
}

#[derive(Debug, Parser)]
struct Args {
    #[clap(short, long, default_value = "postgres://localhost/ipeds")]
    /// URI of Postgres database to insert the IPEDS data (should include the
    /// database name)
    pg:            String,
    #[clap(short, long, default_value = "8")]
    /// Number of IPEDS yearly datasets to download and process in parallel
    concurrency:   usize,
    #[clap(short, long, default_value = "out/")]
    /// Directory to store the raw IPEDS data
    out:           PathBuf,
    #[clap(long)]
    /// Drop all existing IPEDS tables in the database before inserting the new
    /// ones
    drop_existing: bool,
    #[clap(long)]
    /// Vacuum and analyze the database after inserting the IPEDS data
    optimize:      bool,
}

async fn get_mdb_convert(
    url: Url,
    docs_out: Arc<PathBuf>,
    tmp: Arc<TempDir>,
    db: Pool,
    drop_existing: bool,
    client: Client,
) -> Result<()> {
    // TODO: Make the path conversion/sanatization less cluttered
    let name = url
        .path_segments()
        .ok_or(anyhow!("'{}': no path segments found", &url))?
        .last()
        .ok_or(anyhow!("'{}': no last path segment found", &url))?;
    info!("UNZIP: START '{}' -> '{}'", name, docs_out.display());
    let resp = client.get(url.clone()).send().await?.error_for_status()?;

    let stream = resp
        .bytes_stream()
        .map_err(|e| Error::new(ErrorKind::Other, e))
        .into_async_read();

    let mut convert_handle = None;
    let mut dcdr = ZipFileReader::new(stream);
    loop {
        let reading = dcdr.next_with_entry().await?;
        if let Some(mut reading) = reading {
            let rdr = reading.reader_mut();

            let raw_name = rdr.entry().filename().as_str()?.to_owned(); // Raw name of the file according to the zip archive
            let raw_path = Path::new(&raw_name);
            if let Some(ext) = raw_path.extension() {
                // Nested folders are not supported
                let filename = raw_path
                    .file_name()
                    .ok_or(anyhow!("'{}': no file name found", raw_name))?;

                let is_mdb = ext == "accdb";

                let filepath = if !is_mdb {
                    let type_docs_out = docs_out.join(ext);
                    if !type_docs_out.exists() {
                        warn!("CREATE: '{}'", type_docs_out.display());
                        create_dir_all(&type_docs_out).await?;
                    }
                    type_docs_out.join(filename)
                } else {
                    tmp.path().join(filename)
                };

                let f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&filepath)
                    .await?;
                let mut wrtr = TokioBufWriter::new(f);
                let mut rdr = TokioBufReader::new(rdr.compat());
                let bytes = io::copy(&mut rdr, &mut wrtr).await?;

                info!(
                    "WRITE: '{}'({})",
                    filepath.display(),
                    human_bytes(bytes as f64),
                );

                if is_mdb {
                    if convert_handle.is_none() {
                        // Immediately begin converting the MDB file to SQL while still decoding the
                        // rest of the archrive
                        convert_handle = Some(tokio::spawn(convert_mdb(
                            filepath,
                            docs_out.clone(),
                            db.clone(),
                            drop_existing,
                        )));
                    } else {
                        bail!("'{}': multiple MDB files found", url.as_ref());
                    }
                }
            }
            dcdr = reading.skip().await?;
        } else {
            break;
        }
    }
    info!("UNZIP: FINISH '{}' -> '{}'", name, docs_out.display());
    if let Some(handle) = convert_handle {
        handle.await??;
    }
    Ok(())
}

async fn convert_mdb(
    mdb_in: PathBuf,
    docs_out: Arc<PathBuf>,
    db: Pool,
    drop_existing: bool,
) -> Result<()> {
    let mdbname = mdb_in
        .file_stem()
        .ok_or(anyhow!("'{}': no file stem found", mdb_in.display()))?
        .to_str()
        .ok_or(anyhow!("'{}': invalid path", mdb_in.display()))?
        .to_owned();
    info!("CONVERT: START '{}' -> SQL", mdbname);
    let schema_dir = docs_out.join("schema/");
    if !schema_dir.exists() {
        warn!("CREATE: '{}'", schema_dir.display());
        create_dir_all(schema_dir.as_path()).await?;
    }
    // Get schema with mdb-schema
    let mut cmd = Command::new(MDB_SCHEMA.as_path());
    cmd.arg("--no-relations");
    if drop_existing {
        cmd.arg("--drop-table");
    }
    cmd.arg(&mdb_in).arg("postgres");
    trace!("{:?}", cmd);
    let raw_schema = cmd.output().await?.stdout;
    let schema_sql = std::str::from_utf8(&raw_schema)?;

    let schema_path = schema_dir.join(&mdbname).with_extension("sql");
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&schema_path)
        .await?;
    let mut wrtr = TokioBufWriter::new(f);
    wrtr.write_all(schema_sql.as_bytes()).await?;
    wrtr.flush().await?;

    info!(
        "WRITE: '{}'({})",
        schema_path.display(),
        human_bytes(schema_sql.len() as f64)
    );

    let conn = db.get().await?;
    conn.batch_execute(schema_sql).await?;
    drop(conn);
    info!("CONVERT: SCHEMA '{}' -> SQL", mdbname);

    // Get tables with mdb-tables. TODO: Make this less verbose?
    let mut cmd = Command::new(MDB_TABLES.as_path());
    cmd.arg("-1").arg(&mdb_in);
    trace!("{:?}", cmd);
    let raw_tables = cmd.output().await?.stdout.clone();
    let tables = std::str::from_utf8(&raw_tables)?;

    // Spawn tasks for exporting the tables with mdb-export
    let mdb_in = Arc::new(mdb_in);
    let mdbname = Arc::new(mdbname);
    let export_handles = tables
        .lines()
        .map(|table| {
            let db = db.clone();
            let mdb_in = mdb_in.clone();
            let mdbname = mdbname.clone();
            let table = table.to_owned();
            tokio::spawn(async move {
                let cmd = format!(
                    "{} -H {} {}", // -H: no header row
                    MDB_EXPORT.canonicalize()?.display(),
                    mdb_in.display(),
                    // .ok_or(anyhow!("'{}': invalid path", mdb_in.display()))?,
                    table
                );
                let sql = format!("COPY {} FROM PROGRAM '{}' (FORMAT csv);", table, cmd);
                let conn = db.get().await?;
                trace!("EXEC: '{}'", sql);
                conn.batch_execute(&sql).await?;
                drop(conn);
                debug!("COPY: {}.{} -> SQL", mdbname, table);
                anyhow::Ok(())
            })
        })
        .collect::<Vec<_>>();
    for handle in export_handles {
        handle.await??;
    }
    info!("CONVERT: FINISHED '{}' -> SQL", mdbname);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let mut args = Args::parse();
    let client = ClientBuilder::new()
        // .http3_prior_knowledge() Reqwest support for HTTP3 p
        .brotli(true)
        .deflate(true)
        .gzip(true)
        .https_only(true)
        .build()?;

    if !args.out.exists() {
        // args.out = args.out.canonicalize()?;
        warn!("CREATE: '{}'", args.out.display());
        create_dir_all(args.out.as_path()).await?;
        args.out = args.out.canonicalize()?;

        let poolcfg = PoolConfig::default(); // We don't use args.concurrency here because
        let mut cfg = Config::new();
        cfg.pool = Some(poolcfg);
        cfg.url = Some(args.pg.clone());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

        if args.drop_existing {
            error!(
                "!!! THIS WILL DROP EXISTING IPEDS TABLES IN '{}' !!!",
                args.pg
            );
        }

        let raw_html = client.get(IPEDS.clone()).send().await?.text().await?;
        let html = Html::parse_document(&raw_html);

        let tmp = Arc::new(tempdir()?); // Closes temp dir when dropped!
        let out = Arc::new(args.out);
        let mut results = stream::iter(html.select(&SEL_MDB_LINK).map(|el| {
            let href = el.value().attr("href").unwrap().to_owned();
            let pool = pool.clone();
            let client = client.clone();
            let out = out.clone();
            let tmp = tmp.clone();
            tokio::spawn(async move {
                let url = IPEDS.join(&href)?;
                get_mdb_convert(url, out, tmp, pool, args.drop_existing, client).await
            })
        }))
        .buffer_unordered(args.concurrency);

        while let Some(res) = results.next().await {
            res??;
        }
        if args.optimize {
            let conn = pool.get().await?;
            warn!("EXEC: 'VACUUM(FULL, ANALYZE);' - THIS MAY TAKE A WHILE!");
            conn.batch_execute("VACUUM(FULL, ANALYZE);").await?;
        }
    } else {
        warn!(
            "{} already exists! continuing to conversion",
            args.out.display()
        );
        unimplemented!("a separate way to convert the mdb files");
    }
    Ok(())
}
