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

/// Processor information returned by plugins
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct ProcessorInfo {
    /// Name of the processor for logging
    pub name: abi_stable::std_types::RString,
    /// Version of the processor
    pub version: abi_stable::std_types::RString,
}
