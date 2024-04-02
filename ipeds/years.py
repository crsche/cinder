import uvloop
import asyncio
import aiohttp
from yarl import URL
from loguru import logger
from bs4 import BeautifulSoup
from stream_unzip import async_stream_unzip
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor

# import subprocess
from shutil import which
from pathlib import Path
import sqlite3
import argparse

REQUIRED_TOOLS = ["mdb-export", "mdb-tables", "mdb-schema", "sqlite3"]

IPEDS_URL: URL = URL("https://nces.ed.gov")
DB_SOURCE: URL = IPEDS_URL.joinpath("ipeds/use-the-data/download-access-database")
SEL_MDB: str = "table.ipeds-table a[href$='.zip']"
CHUNK_SIZE: int = 1024 * 64  # 64KB


async def get_mdb_urls(source: URL, session: aiohttp.ClientSession) -> list[URL]:
    async with session.get(source) as response:
        response.raise_for_status()
        text = await response.text()
        logger.debug(f"got response from {source}")
    soup = BeautifulSoup(text, "lxml")
    elements = soup.select(SEL_MDB)

    urls = []

    for element in elements:
        href = element.get("href")
        if not isinstance(href, str):
            logger.error("href was not a string", href=href)
        else:
            href = URL(href)
            if href.is_absolute():
                url = href
            else:
                url = source.join(href)
            urls.append(url)
    return urls


async def download_mdb(
    url: URL,
    out_dir: Path,
) -> None:
    out_dir.mkdir(exist_ok=True, parents=True)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            logger.debug(f"started processing {url}")
            chunks = response.content.iter_chunked(CHUNK_SIZE)
            unzipped = async_stream_unzip(chunks)
            async for fname, fsize, content in unzipped:
                name: str = Path(
                    fname.decode()
                ).name  # We have to do this because 2020-21 nested folder

                file = out_dir.joinpath(name)
                async for chunk in content:
                    with open(file, "ab") as f:
                        f.write(chunk)
    logger.success(f"finished processing {url}")


def process_mdb(f: Path, dst: Path) -> None:
    logger.debug(f"processing {f} to {dst}")
    try:
        proc = subprocess.run(
            ["mdb-tables", "-1", f], capture_output=True, check=True, text=True
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Error while subprocess was called! {e}")

    # It's better to explicitly control error flow because we can run specific code for this error if needed rather
    # Than using a broad check of return code. We can also see exactly what went wrong instead of just knowing the command failed
    # This is important because multiple factors can affect an error code
    # Not only that but also logger can run specific code on the error
    
    # proc.check_returncode()  # Errors on nonzero exit code
    
    tables = proc.stdout.splitlines()

    # Create the tables in the new sqlite3 database
    try:
        proc = subprocess.run(
            ["mdb-schema", f, "sqlite"], text=True, check=True, capture_output=True
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Error while subprocess was called! {e}")

    # Read above to know why
    #proc.check_returncode()
    
    cmds = ["BEGIN;"]
    cmds.append(proc.stdout)  # Create table commands

    for table in tables:
        proc = subprocess.run(
            [
                "mdb-export",
                "-H",
                "-D",
                "%Y-%m-%d %H:%M:%S",
                "-I",
                "sqlite",
                f,
                table,
            ],
            capture_output=True,
            check=True,
            text=True,
        )
        proc.check_returncode()
        cmds.append(proc.stdout)  # Insert data for each table
    cmds.append("COMMIT;")
    logger.debug(f"generated transaction for {f} to {dst}")

    transaction = "".join(cmds)
    with sqlite3.connect(dst) as db:
        cursor = db.executescript(transaction)

    logger.success(f"processed {len(tables)} tables from {f} to {dst}")


async def main() -> None:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-o",
        "--out",
        help="where to put the output files",
        default="out/",
        type=Path,
    )
    args = parser.parse_args()
    out_dir = args.out
    mdb_output = out_dir.joinpath("mdb")
    processed_output = out_dir.joinpath("processed")

    # Downloading the Microsoft Access Database files
    async with aiohttp.ClientSession() as session:
        if not out_dir.exists():
            urls = await get_mdb_urls(DB_SOURCE, session)
            logger.debug(f"got mdb sources: {urls}")
            logger.warning(f"downloading to `{mdb_output}`")
            tasks = [download_mdb(url, mdb_output) for url in urls]
            await asyncio.gather(*tasks)
        else:
            logger.warning(
                f"`{out_dir}` already exists, continuing without downloading data"
            )

    # Processing the Microsoft Access Database files
    logger.warning(f"processing data from `{mdb_output}` to `{processed_output}`")
    for tool in REQUIRED_TOOLS:
        if which(tool) is None:
            logger.error(f"required tool `{tool}` not found in PATH")
            sys.exit(1)
    mdbs = list(mdb_output.glob("**/*.accdb"))
    if len(mdbs) == 0:
        logger.error(f"no .accdb files found in `{mdb_output}`")
        sys.exit(1)
    processed_output.mkdir(exist_ok=True, parents=True)
    with ThreadPoolExecutor() as pool:
        for f in mdbs:
            pool.submit(process_mdb, f, processed_output.joinpath(f.stem + ".sqlite3"))
        pool.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(main())
