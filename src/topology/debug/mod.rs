mod debug_buffer;
mod logs;
mod metrics;
mod traces;

use crate::topology::batch::BatchSizer;
use crate::topology::debug::debug_buffer::DebugBuffer;
use crate::topology::generic_pipeline::Inspect;
use tracing::{Level, event};
// Basic debug logger, future thoughts on additional configurability:
//  - support `normal` verbosity level (see: https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/debugexporter/README.md)
//  - configurable log level (defaults to info, but could log to debug, etc)
//  - sampling, only log at a given RPS or percentage
//
// Additionally, once we support multiple exporters per pipeline this logging functionality can be moved into
// a debug exporter, allowing you to install it on any pipeline.
//
// This module is heavily inspired by the OpenTelemetry collector's debug exporter.

#[derive(Clone)]
pub enum DebugVerbosity {
    Basic,
    Detailed,
}

#[derive(Clone)]
pub struct DebugLogger(Option<DebugVerbosity>);

impl DebugLogger {
    pub fn new(level: Option<DebugVerbosity>) -> Self {
        Self(level)
    }
}

pub(crate) trait DebugLoggable {
    // This is similar to the Basic verbosity level of the debug exporter
    fn log_basic(&self);

    // This is similar to the Detailed verbosity level of the debug exporter
    fn log_detailed(&self) -> DebugBuffer;
}

impl<T> Inspect<T> for DebugLogger
where
    T: BatchSizer,
    [T]: DebugLoggable + Send,
{
    fn inspect(&self, payload: &[T]) {
        self.inspect_with_prefix(None, payload);
    }
    fn inspect_with_prefix(&self, prefix: Option<String>, payload: &[T]) {
        match self.0 {
            None => {}
            Some(DebugVerbosity::Basic) => {
                prefix.map(|message| event!(Level::INFO, message));
                DebugLoggable::log_basic(payload)
            }
            Some(DebugVerbosity::Detailed) => {
                prefix.map(|message| event!(Level::INFO, message));
                let out = DebugLoggable::log_detailed(payload);

                // we forego the tracing library because we want to maintain multi-line output
                print!("{}", out)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utilities::otlp::FakeOTLP;

    #[test]
    fn test_detailed_logs() {
        let logs = FakeOTLP::logs_service_request();
        let out = DebugLoggable::log_detailed(logs.resource_logs.as_slice()).to_string();

        assert!(out.contains("ResourceLog #0"));
        assert!(out.contains("service.name: Str(test-service)"));
        assert!(out.contains("Str(This is a log message)"));
    }

    #[test]
    fn test_detailed_traces() {
        let traces = FakeOTLP::trace_service_request();
        let out = DebugLoggable::log_detailed(traces.resource_spans.as_slice()).to_string();

        assert!(out.contains("ResourceSpans #0"));
        assert!(out.contains("service.name: Str(test-service)"));
        assert!(out.contains("SPAN_KIND_INTERNAL"));
    }

    #[test]
    fn test_detailed_metrics() {
        let metrics = FakeOTLP::metrics_service_request();
        let out = DebugLoggable::log_detailed(metrics.resource_metrics.as_slice()).to_string();

        assert!(out.contains("ResourceMetrics #0"));
        assert!(out.contains("service.name: Str(test-service)"));
        assert!(out.contains("Name: test-metric"));
    }
}
