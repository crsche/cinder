#![feature(let_chains, generic_arg_infer, exit_status_error)]
#[macro_use]
extern crate tracing;
use std::path::{Path, PathBuf};

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
    pg:          String,
    #[clap(short, long, default_value = "cinder")]
    dbname:      String,
    #[clap(short, long, default_value = "32")]
    concurrency: usize,
    #[clap(short, long, default_value = "out/rust-raw/")]
    out:         PathBuf,
}

async fn get_mdb_convert(url: Url, out_dir: PathBuf, db: Pool, client: Client) -> Result<()> {
    // TODO: Make the path conversion/sanatization less cluttered
    info!("downloading {} to `{}`", url.as_ref(), out_dir.display());
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
                    .ok_or(anyhow!("{}: no file name found", raw_name))?;
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
                    "wrote {} to `{}`",
                    human_bytes(bytes as f64),
                    filepath.display()
                );

                if let Some(ext) = filepath.extension()
                    && ext == "accdb"
                {
                    if convert_handle.is_none() {
                        // Immediately begin converting the MDB file to SQL while still decoding the
                        // rest of the archrive
                        convert_handle = Some(tokio::spawn(convert_mdb(filepath, db.clone())));
                    } else {
                        bail!("multiple MDB files found in `{}`", url.as_ref());
                    }
                }
            }
            dcdr = reading.skip().await?;
        } else {
            break;
        }
    }
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

async fn convert_mdb(mdb_in: PathBuf, db: Pool) -> Result<()> {
    info!("converting `{}` to SQL", mdb_in.display());
    // Get schema with mdb-schema
    let mut get_schema = Command::new("mdb-schema");
    get_schema
        .args(&["--drop-table", "--no-relations"])
        .arg(&mdb_in)
        .arg("postgres");
    let schema_handle = get_schema.output();

    // Get tables with mdb-tables. TODO: Make this less verbose?
    let mut get_tables = Command::new("mdb-tables");
    get_tables.arg("-1").arg(&mdb_in);
    let tables_handle = get_tables.output();
    let tables_output = tables_handle.await?;
    tables_output.status.exit_ok()?;
    let stdout = String::from_utf8(tables_output.stdout)?;
    let tables = stdout.split_whitespace();

    // Spawn tasks for exporting the tables with mdb-export
    let export_handles = tables
        .map(|table| -> Result<_> {
            // Export each table with mdbtools (doesn't support exporting everything)
            let mut export = Command::new("mdb-export");
            export
                .arg("-H")
                .arg("-D")
                .arg("%Y-%m-%d %H:%M:%S")
                .args(&["-I", "postgres"])
                .arg(&mdb_in)
                .arg(&table);
            Ok(export.output())
        })
        .collect::<Vec<_>>();

    // Begin creating a transaction string from all of the mdbtools outputs
    let mut transaction = String::from("BEGIN;").into_bytes(); // We use bytes here for performance

    let schema_res = schema_handle.await?;
    schema_res.status.exit_ok()?;
    transaction.extend(schema_res.stdout);

    // Export statements for each table
    for handle in export_handles {
        let export_res = handle?.await?;
        export_res.status.exit_ok()?;
        transaction.extend(export_res.stdout);
    }
    transaction.extend("COMMIT;".as_bytes());
    let transaction_str = std::str::from_utf8(&transaction)?;

    let conn = db.get().await?;
    info!(
        "sending {} conversion from {}",
        human_bytes(transaction.len() as f64),
        mdb_in.display()
    );
    conn.batch_execute(transaction_str).await?;
    info!("finished converting `{}`", mdb_in.display(),);
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
        cfg.dbname = Some("cinder".to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

        warn!("THIS WILL DROP ALL TABLES IN THE DATABASE");

        let mut results = stream::iter(html.select(&SEL_MDB_LINK).map(|el| {
            let href = el.value().attr("href").unwrap().to_owned();
            let pool = pool.clone();
            let client = client.clone();
            let out = args.out.clone();
            tokio::spawn(async move {
                let url = IPEDS.join(&href)?;
                get_mdb_convert(url, out, pool, client).await
            })
        }))
        .buffer_unordered(args.concurrency);

        while let Some(res) = results.next().await {
            res??;
        }
    } else {
        warn!(
            "`{}` already exists! continuing to conversion",
            args.out.display()
        );
        unimplemented!("a separate way to convert the mdb files");
    }
    Ok(())
}
