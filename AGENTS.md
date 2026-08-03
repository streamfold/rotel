# AGENTS.md

## Cursor Cloud specific instructions

Rotel is a single Rust workspace (an OpenTelemetry collector); there is no separate frontend/backend.
Standard dev commands live in `DEVELOPING.md` and `.github/workflows/ci.yml`; prefer those.

- Toolchain is pinned by `rust-toolchain.toml` (Rust 1.91.x). System deps (`protobuf-compiler`,
  `libssl-dev`, `libzstd-dev`, `libclang-dev`, `cmake`, gcc) are pre-provisioned in the VM image.
- Build: `cargo build`. Lint: `cargo fmt --check` (CI only checks formatting; there is no clippy gate).
- Tests use nextest: `cargo nextest run`. `cargo-nextest` is preinstalled in the image; if missing,
  `cargo install cargo-nextest --locked`.
- Kafka/kmsg/rust-processor integration tests are gated behind env flags
  (`KAFKA_INTEGRATION_TESTS`, `KMSG_INTEGRATION_TESTS`, `RUST_PROCESSOR_INTEGRATION_TESTS`) and are
  skipped by default; the default `cargo nextest run` does not require Docker. See
  `KAFKA_INTEGRATION_TESTS.md` for the Kafka harness.

### Running the collector (hello world)

- Start: `cargo run -- start --debug-log traces --exporter blackhole`. It listens on
  `127.0.0.1:4317` (OTLP gRPC) and `127.0.0.1:4318` (OTLP HTTP). The subcommand is `start`.
- Send test data with the built-in generator:
  `cargo run --bin generate-otlp -- traces --http-endpoint localhost:4318`.
  With `--debug-log traces`, rotel logs `Received traces. ... spans=1` on receipt.
- CLI flags can also be passed as env vars prefixed with `ROTEL_` (e.g. `ROTEL_OTLP_GRPC_ENDPOINT`).
