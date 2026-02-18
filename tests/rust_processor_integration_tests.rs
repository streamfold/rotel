// SPDX-License-Identifier: Apache-2.0

//! Rust Processor Integration Tests
//!
//! These tests build real processor dylibs and load them to verify end-to-end
//! functionality of the Rust processor loading and execution pipeline.
//!
//! To run these tests:
//! ```
//! cargo test --test rust_processor_integration_tests --features "rust_processor,integration-tests"
//! ```

#![cfg(all(feature = "integration-tests", feature = "rust_processor"))]

use rotel::topology::generic_pipeline::Inspect;
use rotel::topology::payload::Message;
use rotel::topology::processors::{AsyncRustProcessor, Processors, RustProcessor};
use std::path::{Path, PathBuf};
use std::process::Command;
use utilities::otlp::FakeOTLP;

/// Build an example processor dylib and return the path to the built library.
fn build_example_processor(name: &str) -> PathBuf {
    let example_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("rust_processors")
        .join(name);

    let output = Command::new("cargo")
        .args(["build", "--manifest-path"])
        .arg(example_dir.join("Cargo.toml"))
        .output()
        .unwrap_or_else(|e| panic!("Failed to run cargo build for {}: {}", name, e));

    if !output.status.success() {
        panic!(
            "Failed to build example processor {}:\nstdout: {}\nstderr: {}",
            name,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let lib_name = dylib_filename(name);
    let lib_path = example_dir.join("target").join("debug").join(&lib_name);
    assert!(
        lib_path.exists(),
        "Built library not found at {}",
        lib_path.display()
    );
    lib_path
}

/// Get the platform-specific dylib filename
fn dylib_filename(name: &str) -> String {
    if cfg!(target_os = "macos") {
        format!("lib{}.dylib", name)
    } else if cfg!(target_os = "windows") {
        format!("{}.dll", name)
    } else {
        format!("lib{}.so", name)
    }
}

struct NoOpInspector;
impl Inspect<opentelemetry_proto::tonic::trace::v1::ResourceSpans> for NoOpInspector {
    fn inspect(&self, _value: &[opentelemetry_proto::tonic::trace::v1::ResourceSpans]) {}
    fn inspect_with_prefix(
        &self,
        _prefix: Option<String>,
        _value: &[opentelemetry_proto::tonic::trace::v1::ResourceSpans],
    ) {
    }
}

#[test]
fn test_load_sync_processor_from_dylib() {
    let lib_path = build_example_processor("trivial_processor");
    let processor = RustProcessor::load(&lib_path);
    assert!(processor.is_ok(), "Failed to load: {:?}", processor.err());
    assert!(processor.unwrap().name().contains("trivial_processor"));
}

#[test]
fn test_load_async_processor_from_dylib() {
    let lib_path = build_example_processor("async_enrichment_processor");
    let processor = AsyncRustProcessor::load(&lib_path);
    assert!(processor.is_ok(), "Failed to load: {:?}", processor.err());
    assert!(
        processor
            .unwrap()
            .name()
            .contains("async_enrichment_processor")
    );
}

#[test]
fn test_load_processor_nonexistent_path() {
    let result = RustProcessor::load(Path::new("/nonexistent/path/libfoo.dylib"));
    assert!(result.is_err());
}

#[tokio::test]
async fn test_sync_processor_adds_attributes() {
    let lib_path = build_example_processor("trivial_processor");

    let processors = Processors::initialize(vec![])
        .unwrap()
        .initialize_rust(vec![lib_path.to_string_lossy().to_string()])
        .unwrap();

    assert!(processors.has_rust_processors());

    let request = FakeOTLP::trace_service_request_with_spans(1, 2);
    let original_attr_count = request.resource_spans[0].scope_spans[0].spans[0]
        .attributes
        .len();
    let message = Message::new(None, request.resource_spans, None);

    let result = processors.run(message, &NoOpInspector).await;

    assert_eq!(result.payload.len(), 1);
    for span in &result.payload[0].scope_spans[0].spans {
        // trivial_processor adds "rotel.rust_processor" = "trivial_processor"
        assert_eq!(span.attributes.len(), original_attr_count + 1);
        let added = span
            .attributes
            .iter()
            .find(|a| a.key == "rotel.rust_processor");
        assert!(
            added.is_some(),
            "Expected 'rotel.rust_processor' attribute not found"
        );
    }
}

#[tokio::test]
async fn test_async_processor_adds_attributes() {
    let lib_path = build_example_processor("async_enrichment_processor");

    let processors = Processors::initialize(vec![])
        .unwrap()
        .initialize_async_rust(vec![lib_path.to_string_lossy().to_string()])
        .unwrap();

    assert!(processors.has_async_rust_processors());

    let request = FakeOTLP::trace_service_request_with_spans(1, 2);
    let original_attr_count = request.resource_spans[0].scope_spans[0].spans[0]
        .attributes
        .len();
    let message = Message::new(None, request.resource_spans, None);

    let result = processors.run(message, &NoOpInspector).await;

    assert_eq!(result.payload.len(), 1);
    for span in &result.payload[0].scope_spans[0].spans {
        // async_enrichment_processor adds "enrichment.source" and "rotel.async_processor"
        assert_eq!(span.attributes.len(), original_attr_count + 2);
        let enrichment = span
            .attributes
            .iter()
            .find(|a| a.key == "enrichment.source");
        assert!(
            enrichment.is_some(),
            "Expected 'enrichment.source' attribute not found"
        );
        let processor_marker = span
            .attributes
            .iter()
            .find(|a| a.key == "rotel.async_processor");
        assert!(
            processor_marker.is_some(),
            "Expected 'rotel.async_processor' attribute not found"
        );
    }
}

#[tokio::test]
async fn test_full_pipeline_with_both_processor_types() {
    let sync_lib = build_example_processor("trivial_processor");
    let async_lib = build_example_processor("async_enrichment_processor");

    let processors = Processors::initialize(vec![])
        .unwrap()
        .initialize_rust(vec![sync_lib.to_string_lossy().to_string()])
        .unwrap()
        .initialize_async_rust(vec![async_lib.to_string_lossy().to_string()])
        .unwrap();

    let request = FakeOTLP::trace_service_request_with_spans(1, 1);
    let original_attr_count = request.resource_spans[0].scope_spans[0].spans[0]
        .attributes
        .len();
    let message = Message::new(None, request.resource_spans, None);

    let result = processors.run(message, &NoOpInspector).await;

    let span = &result.payload[0].scope_spans[0].spans[0];
    // sync adds 1 attr, async adds 2 attrs
    assert_eq!(span.attributes.len(), original_attr_count + 3);
    assert!(
        span.attributes
            .iter()
            .any(|a| a.key == "rotel.rust_processor")
    );
    assert!(span.attributes.iter().any(|a| a.key == "enrichment.source"));
    assert!(
        span.attributes
            .iter()
            .any(|a| a.key == "rotel.async_processor")
    );

    // Verify shutdown doesn't panic
    processors.shutdown();
}
