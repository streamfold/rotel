use std::fs::File;
use std::path::Path;

use serde::Serialize;

use crate::exporters::file::{FileExporterError, Result, TypedFileExporter};

use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

/// A JSON file exporter for native OTLP protobuf structures (ResourceSpans,
/// ResourceMetrics, ResourceLogs).  Each exported file contains a JSON array of
/// the received resources, matching the official OTLP/HTTP JSON encoding.
pub struct JsonExporter;

impl JsonExporter {
    /// Creates a new `JsonExporter` instance.
    pub fn new() -> Self {
        Self
    }

    /// Serialize and write any `Serialize` payload to the given path.
    fn export_payload<T: Serialize + ?Sized>(&self, payload: &T, path: &Path) -> Result<()> {
        let file = File::create(path).map_err(FileExporterError::Io)?;
        serde_json::to_writer_pretty(file, payload)
            .map_err(|e| FileExporterError::Export(format!("Failed to write JSON: {}", e)))
    }

    /// Export traces (`Vec<ResourceSpans>`) as JSON.
    pub fn export_traces(&self, traces: &[ResourceSpans], path: &Path) -> Result<()> {
        self.export_payload(traces, path)
    }

    /// Export metrics (`Vec<ResourceMetrics>`) as JSON.
    pub fn export_metrics(&self, metrics: &[ResourceMetrics], path: &Path) -> Result<()> {
        self.export_payload(metrics, path)
    }

    /// Export logs (`Vec<ResourceLogs>`) as JSON.
    pub fn export_logs(&self, logs: &[ResourceLogs], path: &Path) -> Result<()> {
        self.export_payload(logs, path)
    }
}

impl TypedFileExporter for JsonExporter {
    type SpanData = ResourceSpans;
    type MetricData = ResourceMetrics;
    type LogData = ResourceLogs;

    fn convert_spans(&self, resource_spans: &ResourceSpans) -> Result<Vec<Self::SpanData>> {
        Ok(vec![resource_spans.clone()])
    }

    fn convert_metrics(&self, resource_metrics: &ResourceMetrics) -> Result<Vec<Self::MetricData>> {
        Ok(vec![resource_metrics.clone()])
    }

    fn convert_logs(&self, resource_logs: &ResourceLogs) -> Result<Vec<Self::LogData>> {
        Ok(vec![resource_logs.clone()])
    }

    fn export_spans(&self, data: &[Self::SpanData], path: &Path) -> Result<()> {
        self.export_traces(data, path)
    }

    fn export_metrics(&self, data: &[Self::MetricData], path: &Path) -> Result<()> {
        self.export_metrics(data, path)
    }

    fn export_logs(&self, data: &[Self::LogData], path: &Path) -> Result<()> {
        self.export_logs(data, path)
    }

    fn file_extension(&self) -> &'static str {
        ".json"
    }
}

impl Default for JsonExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_export_traces_empty() {
        // Ensure exporter can handle empty slice (writes []).
        let exporter = JsonExporter::new();
        let dir = tempdir().unwrap();
        let path = dir.path().join("traces.json");
        let traces: Vec<ResourceSpans> = Vec::new();
        assert!(exporter.export_traces(&traces, &path).is_ok());
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents.trim(), "[]");
    }
}
