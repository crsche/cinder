# Cinder Backend

This is the server that manages querying the IPEDs database and other misc API functionality.

## To Hot Reload

Install `cargo-watch` and `systemfd`:

```sh
$ cargo install cargo-watch systemfd
```

```sh
$ RUST_LOG=debug systemfd --no-pid -s https::8080 -- cargo watch -x ru
```

Note that changing the port above will cause errors with redirection from HTTP to HTTPs 

## To Run

```sh
$ RUST_LOG=debug cargo run [--release]
```

Go to [localhost:8080](https://localhost:8080) to see it in action.

## Feature Set

- Compression - `br`, `zstd`, `deflate`
