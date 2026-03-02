# Developing

Rotel depends on the latest Rust toolchain, we recommend [rustup](https://rustup.rs/). 

# Additional Dependencies
* gcc or another compiler with linker for libc
* cmake
* openssl
* protobuf-compiler
* libzstd-dev
* libclang-dev

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

We prefer to use [nextest](https://nexte.st/) to run tests in Rotel, follow their install
instructions and run as:

```shell
cargo nextest run
```

## Supported Platforms

Rotel supports the following build targets:

| Target                          | Architecture               | Notes                                      |
|---------------------------------|----------------------------|--------------------------------------------|
| `x86_64-unknown-linux-gnu`      | x86_64 Linux               | Full feature support                       |
| `aarch64-unknown-linux-gnu`     | ARM64 Linux                | Full feature support                       |
| `aarch64-apple-darwin`          | ARM64 macOS                | Full feature support                       |
| `armv5te-unknown-linux-gnueabi` | ARMv5 Linux (soft float)   | No Kafka support (`--no-default-features`) |
| `armv7-unknown-linux-gnueabihf` | ARMv7-A Linux (hard float) | No Kafka support (`--no-default-features`) |

### Building for 32-bit ARM

32-bit ARM targets require cross-compilation and have some limitations:

1. Install the cross-compilation toolchain:
   ```shell
   # Ubuntu/Debian
   sudo apt-get install gcc-arm-linux-gnueabi gcc-arm-linux-gnueabihf
   ```

2. Add the Rust target:
   ```shell
   rustup target add armv5te-unknown-linux-gnueabi
   rustup target add armv7-unknown-linux-gnueabihf
   ```

3. Build with reduced features (Kafka/rdkafka is not supported on 32-bit ARM):
   ```shell
   cargo build --target armv5te-unknown-linux-gnueabi --no-default-features --features file_receiver
   cargo build --target armv7-unknown-linux-gnueabihf --no-default-features --features file_receiver
   ```

## Profiling

Profiling with pprof is not enabled by default, you must build with the cargo feature `pprof`. 

When you have rebuilt with pprof support, the following options are available. You can set at most one.

* `--pprof-flame-graph`
* `--pprof-call-graph`

