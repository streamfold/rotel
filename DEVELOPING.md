# Developing

Rotel depends on the latest Rust toolchain, we recommend [rustup](https://rustup.rs/). 

# Additional Dependencies
* gcc or another compiler with linker for libc
* cmake
* openssl
* protoc
* libzstd-dev

## Building and running

To build:
```shell
cargo build
```

To run:
```shell
cargo run start <opts>
```

Add the `--release` flag to build a release version.

## Running tests

```shell
cargo test
```

## Profiling

Profiling with pprof is not enabled by default, you must build with the cargo feature `pprof`. 

When you have rebuilt with pprof support, the following options are available. You can set at most one.

* `--pprof-flame-graph`
* `--pprof-call-graph`

