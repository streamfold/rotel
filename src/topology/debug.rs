// SPDX-License-Identifier: Apache-2.0

use crate::topology::batch::BatchSizer;
use crate::topology::generic_pipeline::Inspect;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use tracing::{event, Level};

// Basic debug logger, future thoughts on additional configurability:
//  - support detail levels beyond basic (normal, detailed) (see: https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/debugexporter/README.md)
//  - configurable log level (defaults to info, but could log to debug, etc)
//  - sampling, only log at a given RPS or percentage
//
// Additionally, once we support multiple exporters per pipeline this logging functionality can be moved into
// a debug exporter, allowing you to install it on any pipeline.

#[derive(Clone)]
pub struct DebugLogger(bool);

impl DebugLogger {
    pub fn new(should_log: bool) -> Self {
        Self(should_log)
    }
}

trait DebugLoggable {
    // This is similar to the Basic verbosity level of the debug exporter
    fn log_basic(&self);
}

impl<T> Inspect<T> for DebugLogger
where
    T: BatchSizer,
    [T]: DebugLoggable + Send,
{
    fn inspect(&self, payload: &[T]) {
        if !self.0 {
            return;
        }

        DebugLoggable::log_basic(payload)
    }
}

impl DebugLoggable for [ResourceSpans] {
    fn log_basic(&self) {
        event!(
            Level::INFO,
            data_type = "traces",
            resource_spans = self.len(),
            spans = self.size_of(),
            "Received traces."
        );
    }
}

impl DebugLoggable for [ResourceMetrics] {
    fn log_basic(&self) {
        event!(
            Level::INFO,
            data_type = "metrics",
            resource_metrics = self.len(),
            metrics = self.size_of(),
            "Received metrics."
        );
    }
}

impl DebugLoggable for [ResourceLogs] {
    fn log_basic(&self) {
        event!(
            Level::INFO,
            data_type = "logs",
            resource_logs = self.len(),
            logs = self.size_of(),
            "Received logs."
        );
    }
}
