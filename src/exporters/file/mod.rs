use thiserror::Error;

/// Errors that can occur during file export operations.
///
/// - `Io`: Underlying I/O error (e.g., file not found, permission denied).
///   Recovery: Check file system permissions and path existence.
/// - `InvalidData`: The input data is malformed or not supported.
///   Recovery: Validate and sanitize input data before export.
/// - `Config`: Configuration is invalid or missing required fields.
///   Recovery: Review and correct configuration parameters.
/// - `Export`: Error occurred during the export process (e.g., Arrow/Parquet failure).
///   Recovery: Check data types, schema compatibility, and disk space.
#[derive(Debug, Error)]
pub enum FileExporterError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid data format: {0}")]
    InvalidData(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Export error: {0}")]
    Export(String),
}

/// Result type for file exporter operations.
pub type Result<T> = std::result::Result<T, FileExporterError>;

use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::path::Path;

/// Generic trait for file exporters that can handle different data formats
pub trait TypedFileExporter {
    /// The type used to represent span data in this exporter
    type SpanData: Clone + Send;
    /// The type used to represent metric data in this exporter
    type MetricData: Clone + Send;
    /// The type used to represent log data in this exporter
    type LogData: Clone + Send;

    /// Convert ResourceSpans to the exporter's span data format
    fn convert_spans(&self, resource_spans: &ResourceSpans) -> Result<Vec<Self::SpanData>>;
    /// Convert ResourceMetrics to the exporter's metric data format
    fn convert_metrics(&self, resource_metrics: &ResourceMetrics) -> Result<Vec<Self::MetricData>>;
    /// Convert ResourceLogs to the exporter's log data format
    fn convert_logs(&self, resource_logs: &ResourceLogs) -> Result<Vec<Self::LogData>>;

    /// Export span data to a file
    fn export_spans(&self, data: &[Self::SpanData], path: &Path) -> Result<()>;
    /// Export metric data to a file
    fn export_metrics(&self, data: &[Self::MetricData], path: &Path) -> Result<()>;
    /// Export log data to a file
    fn export_logs(&self, data: &[Self::LogData], path: &Path) -> Result<()>;

    /// Get the file extension for this exporter
    fn file_extension(&self) -> &'static str;
}


pub mod config;
pub mod json;
pub mod parquet;
pub mod task;
