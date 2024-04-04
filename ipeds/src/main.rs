#![feature(let_chains, generic_arg_infer, exit_status_error)]
#[macro_use]
extern crate tracing;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Result};
use async_zip::base::read::stream::ZipFileReader;
use clap::Parser;
use deadpool_postgres::{Config, ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime};
use futures::{
    io::{Error, ErrorKind},
    stream::{self, TryStreamExt},
    StreamExt,
};
use human_bytes::human_bytes;
use lazy_static::lazy_static;
use reqwest::{Client, ClientBuilder, Url};
use scraper::{Html, Selector};
use tokio::{
    fs::OpenOptions,
    io::{self, BufReader as TokioBufReader},
    process::Command,
};
use tokio_postgres::NoTls;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use which::which;

const REQUIRED_TOOLS: [&str; 3] = ["mdb-tables", "mdb-export", "mdb-schema"];

lazy_static! {
    static ref NCES: Url = Url::parse("https://nces.ed.gov").unwrap();
    static ref IPEDS: Url = NCES
        .join("ipeds/use-the-data/download-access-database")
        .unwrap();
    static ref SEL_MDB_LINK: Selector =
        Selector::parse("table.ipeds-table a[href$='.zip']").unwrap();
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
    #[clap(short, long, default_value = "out/rust-raw/")]
    /// Directory to store the raw IPEDS data
    out:           PathBuf,
    #[clap(short, long, default_value = "false")]
    /// Drop all existing IPEDS tables in the database before inserting the new
    /// ones
    drop_existing: bool,
}

async fn get_mdb_convert(
    url: Url,
    out_dir: PathBuf,
    db: Pool,
    drop_existing: bool,
    client: Client,
) -> Result<()> {
    // TODO: Make the path conversion/sanatization less cluttered
    let name = url
        .path_segments()
        .ok_or(anyhow!("`{}`: no path segments found", url.as_ref()))?
        .last()
        .ok_or(anyhow!("`{}`: no last path segment found", url.as_ref()))?;
    info!("`{}`->`{}` - START", name, out_dir.display());
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
            if raw_path.extension().is_some() {
                // Nested folders are not supported
                let filename = raw_path
                    .file_name()
                    .ok_or(anyhow!("`{}`: no file name found", raw_name))?;
                let filepath = out_dir.join(filename);
                let f = OpenOptions::new()
                    // .truncate(true)
                    .write(true)
                    .create_new(true)
                    .open(&filepath)
                    .await?;
                let mut wrtr = TokioBufReader::new(f);
                let mut compat_rdr = rdr.compat();
                let bytes = io::copy(&mut compat_rdr, &mut wrtr).await?;
                info!(
                    "`{}` - {} bytes written",
                    filename.to_string_lossy(),
                    human_bytes(bytes as f64),
                );

                if let Some(ext) = filepath.extension()
                    && ext == "accdb"
                {
                    if convert_handle.is_none() {
                        // Immediately begin converting the MDB file to SQL while still decoding the
                        // rest of the archrive
                        convert_handle = Some(tokio::spawn(convert_mdb(
                            filepath,
                            db.clone(),
                            drop_existing,
                        )));
                    } else {
                        bail!("`{}`: multiple MDB files found", url.as_ref());
                    }
                }
            }
            dcdr = reading.skip().await?;
        } else {
            break;
        }
    }
    info!("`{}`->`{}` - DONE", name, out_dir.display());
    if let Some(handle) = convert_handle {
        handle.await??;
    }
    Ok(())
}

fn check_tools(tools: &[&str]) -> Result<()> {
    for tool in tools {
        if which(tool).is_err() {
            bail!("`{}` not found in PATH", tool);
        }
    }
    Ok(())
}

async fn convert_mdb(mdb_in: PathBuf, db: Pool, drop_existing: bool) -> Result<()> {
    let filename = mdb_in.file_name().unwrap().to_str().unwrap().to_string();
    info!("`{}`->SQL - START", filename);
    // Get schema with mdb-schema
    let mut get_schema = Command::new("mdb-schema");
    get_schema.arg("--no-relations");
    if drop_existing {
        get_schema.arg("--drop-tables");
    }
    get_schema.arg(&mdb_in).arg("postgres");
    let raw_schema = get_schema.output().await?.stdout;
    let schema_sql = std::str::from_utf8(&raw_schema)?;
    let conn = db.get().await?;
    conn.batch_execute(schema_sql).await?;
    drop(conn);

    // Get tables with mdb-tables. TODO: Make this less verbose?
    let mut get_tables = Command::new("mdb-tables");
    get_tables.arg("-1").arg(&mdb_in);
    let raw_tables = get_tables.output().await?.stdout.clone();
    let tables = std::str::from_utf8(&raw_tables)?;

    // Spawn tasks for exporting the tables with mdb-export
    let mdb_in = Arc::new(mdb_in);
    let filename = Arc::new(filename);
    let export_handles = tables
        .lines()
        .map(|table| {
            // Export each table with mdbtools (doesn't support exporting everything)
            let db = db.clone();
            let mdb_in = mdb_in.clone();
            let filename = filename.clone();
            let table = table.to_owned();
            tokio::spawn(async move {
                let mut export = Command::new("mdb-export");
                export
                    .arg("-H")
                    .arg("-D")
                    .arg("%Y-%m-%d %H:%M:%S")
                    .args(&["-I", "postgres"])
                    .arg(mdb_in.as_ref()) // Pass the reference to OsStr
                    .arg(&table);
                let raw_export = export.output().await?.stdout;
                let sql = std::str::from_utf8(&raw_export)?;
                let conn = db.get().await?;
                conn.batch_execute(sql).await?;
                debug!("`{}` / `{}`->SQL", filename, table);
                drop(conn);
                anyhow::Ok(())
            })
        })
        .collect::<Vec<_>>();
    for handle in export_handles {
        handle.await??;
    }
    info!("`{}`->SQL - DONE", mdb_in.display(),);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let client = ClientBuilder::new()
        // .http3_prior_knowledge() Reqwest support for HTTP3 paused
        .use_rustls_tls()
        .https_only(true)
        .gzip(true)
        .deflate(true)
        .brotli(true)
        .build()?;

    let html_str = client.get(IPEDS.clone()).send().await?.text().await?;
    let html = Html::parse_document(&html_str);

    if !args.out.exists() {
        check_tools(&REQUIRED_TOOLS)?;

        warn!("creating `{}`", args.out.display());
        std::fs::create_dir_all(args.out.as_path())?;

        let poolcfg = PoolConfig::new(args.concurrency);
        let mut cfg = Config::new();
        cfg.pool = Some(poolcfg);
        cfg.url = Some(args.pg);
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

        if args.drop_existing {
            warn!("THIS WILL DROP ALL TABLES IN THE DATABASE");
        }

        let mut results = stream::iter(html.select(&SEL_MDB_LINK).map(|el| {
            let href = el.value().attr("href").unwrap().to_owned();
            let pool = pool.clone();
            let client = client.clone();
            let out = args.out.clone();
            tokio::spawn(async move {
                let url = IPEDS.join(&href)?;
                get_mdb_convert(url, out, pool, args.drop_existing, client).await
            })
        }))
        .buffer_unordered(args.concurrency);

        while let Some(res) = results.next().await {
            res??;
        }
        let conn = pool.get().await?;
        warn!("fully vacuuming and analyzing database - this will take a while");
        conn.batch_execute("VACUUM(FULL, ANALYZE, VERBOSE);")
            .await?;
    } else {
        warn!(
            "`{}` already exists! continuing to conversion",
            args.out.display()
        );
        unimplemented!("a separate way to convert the mdb files");
    }
    Ok(())
}
