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
    AsyncReadExt, StreamExt,
};
use human_bytes::human_bytes;
use lazy_static::lazy_static;
use reqwest::{Client, ClientBuilder, Url};
use scraper::{Html, Selector};
use tokio::{
    fs::{create_dir_all, OpenOptions},
    io::{AsyncWriteExt, BufWriter as TokioBufWriter},
    process::Command,
};
use tokio_postgres::NoTls;
use tracing_subscriber::EnvFilter;
use which::which;

const REQUIRED_TOOLS: [&str; 3] = ["mdb-tables", "mdb-export", "mdb-schema"];
const COPY_BUF_SIZE: usize = 1024 * 256;
// const COMMAND: &'static str = "mdb-export -H -Q -D %Y-%m-%d %H:%M:%S {} {}";

lazy_static! {
    static ref NCES: Url = Url::parse("https://nces.ed.gov").unwrap();
    static ref IPEDS: Url = NCES
        .join("ipeds/use-the-data/download-access-database")
        .unwrap();
    static ref SEL_MDB_LINK: Selector =
        Selector::parse("table.ipeds-table a[href$='.zip']").unwrap();
    static ref MDB_EXPORT: String = which("mdb-export").unwrap().to_str().unwrap().to_owned();
}

#[derive(Debug, Parser)]
struct Args {
    #[clap(short, long, default_value = "postgres://localhost")]
    /// URI of Postgres database to insert the IPEDS data (should include the
    /// database name)
    pg:            String,
    #[clap(short, long, default_value = "4")]
    /// Number of IPEDS yearly datasets to download and process in parallel
    concurrency:   usize,
    #[clap(short, long, default_value = "out/")]
    /// Directory to store the raw IPEDS data
    out:           PathBuf,
    #[clap(long, default_value = "false")]
    /// Drop all existing IPEDS tables in the database before inserting the new
    /// ones
    drop_existing: bool,
    #[clap(long, default_value = "false")]
    /// Vacuum and analyze the database after inserting the IPEDS data
    clean:         bool,
}

async fn get_mdb_convert(
    url: Url,
    out_dir: Arc<PathBuf>,
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
    info!("UNZIP: START '{}' -> '{}'", name, out_dir.display());
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
                let type_out_dir = out_dir.join(ext);
                if !type_out_dir.exists() {
                    warn!("CREATE: '{}'", type_out_dir.display());
                    create_dir_all(&type_out_dir).await?;
                }
                let filepath = type_out_dir.join(filename);
                let f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&filepath)
                    .await?;
                let mut wrtr = TokioBufWriter::new(f);

                let mut total_bytes = 0;
                let mut buf = [0u8; COPY_BUF_SIZE];
                loop {
                    let n = rdr.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_bytes += n;
                    wrtr.write_all(&buf[..n]).await?;
                }
                wrtr.flush().await?;
                info!(
                    "WRITE: '{}'({})",
                    filepath.display(),
                    human_bytes(total_bytes as f64),
                );

                if let Some(ext) = filepath.extension()
                    && ext == "accdb"
                {
                    if convert_handle.is_none() {
                        // Immediately begin converting the MDB file to SQL while still decoding the
                        // rest of the archrive
                        convert_handle = Some(tokio::spawn(convert_mdb(
                            filepath,
                            out_dir.clone(),
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
    info!("UNZIP: FINISH '{}' -> '{}'", name, out_dir.display());
    if let Some(handle) = convert_handle {
        handle.await??;
    }
    Ok(())
}

fn check_tools(tools: &[&str]) -> Result<()> {
    for tool in tools {
        if which(tool).is_err() {
            bail!("'{}' not found in $PATH", tool);
        }
    }
    Ok(())
}

async fn convert_mdb(
    mdb_in: PathBuf,
    out_dir: Arc<PathBuf>,
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
    let schema_dir = out_dir.join("schema/");
    if !schema_dir.exists() {
        warn!("CREATE: '{}'", schema_dir.display());
        create_dir_all(schema_dir.as_path()).await?;
    }
    // Get schema with mdb-schema
    let mut cmd = Command::new("mdb-schema");
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
    info!("CONVERT: SCHEMA {}", mdbname);

    // Get tables with mdb-tables. TODO: Make this less verbose?
    let mut cmd = Command::new("mdb-tables");
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
                    MDB_EXPORT.as_str(),
                    mdb_in
                        .to_str()
                        .ok_or(anyhow!("'{}': invalid path", mdb_in.display()))?,
                    table
                );
                let sql = format!("COPY {} FROM PROGRAM '{}' (FORMAT csv);", table, cmd);
                let conn = db.get().await?;
                trace!("EXEC: '{}'", sql);
                conn.batch_execute(&sql).await?;
                drop(conn);
                debug!("COPY: {}/{} -> SQL", mdbname, table);
                anyhow::Ok(())
            })
        })
        .collect::<Vec<_>>();
    for handle in export_handles {
        handle.await??;
    }
    info!("CONVERT: FINISHED {} -> SQL", mdbname);
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
        check_tools(&REQUIRED_TOOLS)?;

        warn!("CREATE: '{}'", args.out.display());
        create_dir_all(args.out.as_path()).await?;
        args.out = args.out.canonicalize()?;

        let poolcfg = PoolConfig::new(args.concurrency);
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

        let out = Arc::new(args.out);
        let mut results = stream::iter(html.select(&SEL_MDB_LINK).map(|el| {
            let href = el.value().attr("href").unwrap().to_owned();
            let pool = pool.clone();
            let client = client.clone();
            let out = out.clone();
            tokio::spawn(async move {
                let url = IPEDS.join(&href)?;
                get_mdb_convert(url, out, pool, args.drop_existing, client).await
            })
        }))
        .buffer_unordered(args.concurrency);

        while let Some(res) = results.next().await {
            res??;
        }
        if args.clean {
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
