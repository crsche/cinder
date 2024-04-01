# IPEDS Data Scraper

## Python

`years.py` 

## Rust

The rust code in [src/](./src/) gathers all of the IPEDS data from all possible years (see the [IPEDS Access Database homepage](https://nces.ed.gov/ipeds/use-the-data/download-access-database)) into a single postgres database. It also outputs the raw Access DBs and associated documenation at the directory specified by `--out` (default is `out/rust-raw/`).

### Arguments

```sh
Usage: ipeds [OPTIONS]

Options:
  -p, --pg <PG>                    [default: postgres://localhost]
  -d, --dbname <DBNAME>            [default: cinder]
  -c, --concurrency <CONCURRENCY>  [default: 32]
  -o, --out <OUT>                  [default: out/rust-raw/]
  -h, --help                       Print help
```