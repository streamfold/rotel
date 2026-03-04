# Rotel Rust Processor SDK

Build custom telemetry processors for [Rotel](https://github.com/streamfold/rotel) in Rust. Processors are compiled as dynamic libraries (`.dylib`/`.so`) and loaded at runtime, allowing you to transform traces, logs, and metrics with native performance.

## Quick Start

### 1. Create a new library crate

```bash
cargo new --lib my_processor
cd my_processor
```

### 2. Configure `Cargo.toml`

```toml
[package]
name = "my_processor"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
rotel_rust_processor_sdk = { path = "../rotel_rust_processor_sdk" }
abi_stable = "0.11"
```

The `crate-type = ["cdylib"]` is required — this tells Cargo to produce a dynamic library that Rotel can load at runtime.

### 3. Implement a processor

```rust
use rotel_rust_processor_sdk::prelude::*;

#[derive(Default)]
pub struct MyProcessor;

impl RotelProcessor for MyProcessor {
    fn process_spans(
        &self,
        spans: &mut RResourceSpans,
        _context: &ROption<RRequestContext>,
    ) {
        for scope_spans in spans.scope_spans.iter_mut() {
            for span in scope_spans.spans.iter_mut() {
                span.attributes.push(RKeyValue::string("processed_by", "my_processor"));
            }
        }
    }
}

export_processor!(MyProcessor);
```

### 4. Build

```bash
cargo build --release
```

The compiled library will be at `target/release/libmy_processor.dylib` (macOS) or `target/release/libmy_processor.so` (Linux).

### 5. Run with Rotel

```bash
rotel start \
    --rust-trace-processor ./target/release/libmy_processor.dylib \
    --debug-log traces \
    --debug-log-verbosity detailed \
    --exporter blackhole
```

## Sync vs Async Processors

The SDK provides two processor traits:

### Sync Processors (`RotelProcessor`)

Best for simple, CPU-bound transformations. Data is modified in-place via mutable references.

```rust
use rotel_rust_processor_sdk::prelude::*;

#[derive(Default)]
pub struct TagProcessor;

impl RotelProcessor for TagProcessor {
    fn process_spans(
        &self,
        spans: &mut RResourceSpans,
        _context: &ROption<RRequestContext>,
    ) {
        for scope_spans in spans.scope_spans.iter_mut() {
            for span in scope_spans.spans.iter_mut() {
                span.attributes.push(RKeyValue::string("env", "production"));
            }
        }
    }

    fn process_logs(
        &self,
        logs: &mut RResourceLogs,
        _context: &ROption<RRequestContext>,
    ) {
        for scope_logs in logs.scope_logs.iter_mut() {
            for log in scope_logs.log_records.iter_mut() {
                log.attributes.push(RKeyValue::string("env", "production"));
            }
        }
    }

    fn process_metrics(
        &self,
        metrics: &mut RResourceMetrics,
        _context: &ROption<RRequestContext>,
    ) {
        // Modify metrics in place
    }
}

export_processor!(TagProcessor);
```

### Async Processors (`AsyncProcessor`)

Best for processors that need I/O — calling external APIs, database lookups, etc. The SDK manages a tokio runtime automatically. Data is passed by ownership and returned.

Add `tokio` to your dependencies if you need async utilities:

```toml
[dependencies]
rotel_rust_processor_sdk = { path = "../rotel_rust_processor_sdk" }
abi_stable = "0.11"
tokio = { version = "1", features = ["time"] }
```

```rust
use rotel_rust_processor_sdk::prelude::*;

#[derive(Default)]
pub struct EnrichmentProcessor;

impl AsyncProcessor for EnrichmentProcessor {
    async fn process_spans(
        &self,
        mut spans: RResourceSpans,
        _context: ROption<RRequestContext>,
    ) -> RResourceSpans {
        let region = lookup_region().await;

        for scope_spans in spans.scope_spans.iter_mut() {
            for span in scope_spans.spans.iter_mut() {
                span.attributes.push(RKeyValue::string("region", region.as_str()));
            }
        }

        spans
    }
}

async fn lookup_region() -> String {
    // In a real processor, call an external service here
    "us-east-1".to_string()
}

export_async_processor!(EnrichmentProcessor);
```

Async processors also support lifecycle hooks:

```rust
impl AsyncProcessor for MyProcessor {
    fn start(&self) {
        // Called once when the processor is loaded
    }

    fn shutdown(&self) {
        // Called once when the pipeline is shutting down
    }

    // ... process methods
}
```

## CLI Flags

Processors are loaded per telemetry type. Multiple processors can be specified and will execute in order.

### Sync processors

| Flag | Env Var | Description |
|------|---------|-------------|
| `--rust-trace-processor` | `ROTEL_RUST_TRACE_PROCESSOR` | Path to trace processor dylib |
| `--rust-logs-processor` | `ROTEL_RUST_LOGS_PROCESSOR` | Path to logs processor dylib |
| `--rust-metrics-processor` | `ROTEL_RUST_METRICS_PROCESSOR` | Path to metrics processor dylib |

### Async processors

| Flag | Env Var | Description |
|------|---------|-------------|
| `--async-rust-trace-processor` | `ROTEL_ASYNC_RUST_TRACE_PROCESSOR` | Path to async trace processor dylib |
| `--async-rust-logs-processor` | `ROTEL_ASYNC_RUST_LOGS_PROCESSOR` | Path to async logs processor dylib |
| `--async-rust-metrics-processor` | `ROTEL_ASYNC_RUST_METRICS_PROCESSOR` | Path to async metrics processor dylib |
| `--async-processor-preserve-on-panic` | `ROTEL_ASYNC_PROCESSOR_PRESERVE_ON_PANIC` | Preserve data on async processor panic (default: `false`) |

### Chaining multiple processors

```bash
rotel start \
    --rust-trace-processor ./libfirst.dylib,./libsecond.dylib \
    --async-rust-trace-processor ./libenrichment.dylib \
    --exporter blackhole
```

Execution order: sync processors run first (in order specified), then async processors (in order specified).

## Working with Data Types

The SDK provides FFI-safe types that mirror the OpenTelemetry protobuf definitions. All types are prefixed with `R` (e.g., `RSpan`, `RLogRecord`, `RMetric`).

### Attributes

Use the `RKeyValue` helper constructors:

```rust
// String attribute
span.attributes.push(RKeyValue::string("key", "value"));

// Integer attribute
span.attributes.push(RKeyValue::int("retry_count", 3));

// Boolean attribute
span.attributes.push(RKeyValue::bool("sampled", true));

// Double attribute
span.attributes.push(RKeyValue::double("duration_ms", 42.5));

// Bytes attribute
span.attributes.push(RKeyValue::bytes("payload", RVec::from(vec![0x01, 0x02])));

// Array attribute
let values = RVec::from(vec![
    RAnyValue::String("a".into()),
    RAnyValue::String("b".into()),
]);
span.attributes.push(RKeyValue::array("tags", values));

// Nested key-value list
let kvs = RVec::from(vec![
    RKeyValue::string("nested_key", "nested_value"),
]);
span.attributes.push(RKeyValue::key_value_list("metadata", kvs));
```

### Request Context

Processors receive the request context from the incoming OTLP request, which contains transport-level metadata:

```rust
fn process_spans(
    &self,
    spans: &mut RResourceSpans,
    context: &ROption<RRequestContext>,
) {
    if let ROption::RSome(ctx) = context {
        match ctx {
            RRequestContext::HttpContext(http) => {
                // Access HTTP headers
                if let Some(auth) = http.headers.get(&RString::from("authorization")) {
                    // Use header value
                }
            }
            RRequestContext::GrpcContext(grpc) => {
                // Access gRPC metadata
                if let Some(val) = grpc.metadata.get(&RString::from("x-custom-header")) {
                    // Use metadata value
                }
            }
        }
    }
}
```

## End-to-End Example

Build the included example processor and run it with telemetrygen:

```bash
# Build the example processor
cd examples/rust_processors/trivial_processor
cargo build
cd ../../..

# Start Rotel with the processor, debug logging, and blackhole exporter
cargo run --features rust_processor -- start \
    --rust-trace-processor ./examples/rust_processors/trivial_processor/target/debug/libtrivial_processor.dylib \
    --debug-log traces \
    --debug-log-verbosity detailed \
    --exporter blackhole
```

In another terminal, send test traces:

```bash
telemetrygen traces --otlp-insecure --traces 1
```

Rotel will log the payload before and after processing. You should see the `rotel.rust_processor: trivial_processor` attribute added to every span in the "after processing" output.

## Panic Safety

Both `export_processor!` and `export_async_processor!` wrap your processor methods in `catch_unwind` to prevent panics from crossing the FFI boundary (which would be undefined behavior). In all cases, an error message is printed to stderr.

- **Sync processors**: If a panic occurs, the data passes through in whatever state it was in at the time of the panic (potentially partially modified).
- **Async processors**: Because async processors take ownership of the data (required for safe `.await` across the FFI boundary), the default behavior on panic is to **drop the data** — it is removed from the pipeline. This avoids the cost of cloning every item on every call.

### Preserving data on async processor panic

If you prefer to preserve the original data when an async processor panics, enable the `--async-processor-preserve-on-panic` flag:

```bash
rotel start \
    --async-rust-trace-processor ./libenrichment.dylib \
    --async-processor-preserve-on-panic \
    --exporter blackhole
```

When enabled, Rotel clones each telemetry item before passing it to the async processor. If the processor panics, the original unmodified clone is returned instead. This provides safety at the cost of a full deep copy of every item on every processor invocation, which may impact throughput and memory usage under high load.

## Requirements

- Your processor type must implement `Default`
- Call `export_processor!` or `export_async_processor!` exactly once per library
- The library must be compiled as `cdylib`
- Rotel must be built with `--features rust_processor` to enable loading Rust processors
