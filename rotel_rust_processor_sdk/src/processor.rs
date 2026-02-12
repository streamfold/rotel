// SPDX-License-Identifier: Apache-2.0

//! Processor trait definition for Rust-based Rotel processors.

use abi_stable::std_types::ROption;
use abi_stable::{sabi_trait, StableAbi};

use crate::types::{RRequestContext, RResourceLogs, RResourceMetrics, RResourceSpans};

/// Trait that Rust processors must implement.
///
/// Modify telemetry data in place. Default implementations do nothing.
///
/// # Panics
///
/// Panics in processor methods are caught by the `export_processor!` macro
/// before they can cross the FFI boundary. If a panic occurs, the telemetry
/// data passes through in whatever state it was in at the time of the panic
/// (potentially partially modified), and an error is printed to stderr.
///
/// # Example
///
/// ```ignore
/// use rotel_rust_processor_sdk::prelude::*;
///
/// #[derive(Default)]
/// pub struct MyProcessor;
///
/// impl RotelProcessor for MyProcessor {
///     fn process_spans(
///         &self,
///         spans: &mut RResourceSpans,
///         _context: &ROption<RRequestContext>,
///     ) {
///         for scope_spans in spans.scope_spans.iter_mut() {
///             for span in scope_spans.spans.iter_mut() {
///                 span.attributes.push(RKeyValue::string("processed", "true"));
///             }
///         }
///     }
/// }
///
/// export_processor!(MyProcessor);
/// ```
#[sabi_trait]
pub trait RotelProcessor: Send + Sync {
    /// Process trace spans in place.
    fn process_spans(&self, _spans: &mut RResourceSpans, _context: &ROption<RRequestContext>) {}

    /// Process logs in place.
    fn process_logs(&self, _logs: &mut RResourceLogs, _context: &ROption<RRequestContext>) {}

    /// Process metrics in place.
    fn process_metrics(
        &self,
        _metrics: &mut RResourceMetrics,
        _context: &ROption<RRequestContext>,
    ) {
    }
}

/// FFI-safe async processor trait (internal).
///
/// This is the low-level trait that crosses the FFI boundary via `#[sabi_trait]`.
/// **Processor authors should implement `AsyncProcessor` instead**, which provides
/// `async fn` methods and automatic runtime management via `export_async_processor!`.
///
/// The host invokes these methods via `tokio::task::spawn_blocking`, so blocking is safe.
#[sabi_trait]
pub trait AsyncRotelProcessor: Send + Sync {
    /// Called once when the processor is loaded. Use for initialization.
    fn start(&self) {}

    /// Called once when the pipeline is shutting down. Use for cleanup.
    fn shutdown(&self) {}

    /// Process trace spans. Takes ownership and returns processed data.
    fn process_spans(
        &self,
        spans: RResourceSpans,
        _context: ROption<RRequestContext>,
    ) -> RResourceSpans {
        spans
    }

    /// Process logs. Takes ownership and returns processed data.
    fn process_logs(
        &self,
        logs: RResourceLogs,
        _context: ROption<RRequestContext>,
    ) -> RResourceLogs {
        logs
    }

    /// Process metrics. Takes ownership and returns processed data.
    fn process_metrics(
        &self,
        metrics: RResourceMetrics,
        _context: ROption<RRequestContext>,
    ) -> RResourceMetrics {
        metrics
    }
}

/// Processor information returned by plugins
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct ProcessorInfo {
    /// Name of the processor for logging
    pub name: abi_stable::std_types::RString,
    /// Version of the processor
    pub version: abi_stable::std_types::RString,
}

/// User-facing async processor trait with `async fn` methods.
///
/// Implement this trait to build async-capable processors. The `export_async_processor!`
/// macro handles runtime creation and bridges your async methods to the FFI boundary
/// automatically — you never need to manage a tokio runtime yourself.
///
/// # Lifecycle
///
/// - `start()` is called once when the processor is loaded (before any processing)
/// - `shutdown()` is called when the pipeline is shutting down
///
/// # Example
///
/// ```ignore
/// use rotel_rust_processor_sdk::prelude::*;
///
/// #[derive(Default)]
/// pub struct MyProcessor;
///
/// impl AsyncProcessor for MyProcessor {
///     async fn process_spans(
///         &self,
///         mut spans: RResourceSpans,
///         _context: ROption<RRequestContext>,
///     ) -> RResourceSpans {
///         let data = fetch_enrichment().await;
///         for scope_spans in spans.scope_spans.iter_mut() {
///             for span in scope_spans.spans.iter_mut() {
///                 span.attributes.push(RKeyValue::string("enriched", data.as_str()));
///             }
///         }
///         spans
///     }
/// }
///
/// export_async_processor!(MyProcessor);
/// ```
pub trait AsyncProcessor: Send + Sync + 'static {
    /// Called once when the processor is loaded. Use for initialization.
    fn start(&self) {}

    /// Called once when the pipeline is shutting down. Use for cleanup.
    fn shutdown(&self) {}

    /// Process trace spans asynchronously.
    fn process_spans(
        &self,
        spans: RResourceSpans,
        _context: ROption<RRequestContext>,
    ) -> impl std::future::Future<Output = RResourceSpans> + Send {
        async { spans }
    }

    /// Process logs asynchronously.
    fn process_logs(
        &self,
        logs: RResourceLogs,
        _context: ROption<RRequestContext>,
    ) -> impl std::future::Future<Output = RResourceLogs> + Send {
        async { logs }
    }

    /// Process metrics asynchronously.
    fn process_metrics(
        &self,
        metrics: RResourceMetrics,
        _context: ROption<RRequestContext>,
    ) -> impl std::future::Future<Output = RResourceMetrics> + Send {
        async { metrics }
    }
}
