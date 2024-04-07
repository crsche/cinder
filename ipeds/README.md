# IPEDS Data Scraper

## Python

`years.py`

### Usage

```sh
usage: years.py [-h] [-o OUT]

options:
  -h, --help         show this help message and exit
  -o OUT, --out OUT  where to put the output files (default: out/)
```

## Rust

The rust code in [src/](./src/) gathers all of the IPEDS data from all possible years (see the [IPEDS Access Database homepage](https://nces.ed.gov/ipeds/use-the-data/download-access-database)) into a single postgres database. It also outputs the raw Access DBs and associated documenation at the directory specified by `--out` (default is `out/rust-raw/`).

### NOTE

You might have to give postgres access to `/tmp` so it has permission to import the `accdb` files. You can do this by setting `PrivateTmp=true` in `postgresql.service` (`/var/lib/systemd/system/postgresql.service`).

### Usage

```sh
Usage: ipeds [OPTIONS]

Options:
  -p, --pg <PG>                    [default: postgres://localhost]
  -d, --dbname <DBNAME>            [default: cinder]
  -c, --concurrency <CONCURRENCY>  [default: 32]
  -o, --out <OUT>                  [default: out/rust-raw/]
  -h, --help                       Print help
```
