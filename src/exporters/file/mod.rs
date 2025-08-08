use parquet::ParquetExporter;
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

use std::path::Path;

/// Generic trait for file exporters that can handle different resource types
pub trait TypedFileExporter<Resource> {
    /// The type used to represent data in this exporter for the given Resource type
    type Data: Clone + Send;

    /// Convert a Resource to the exporter's data format
    fn convert(&self, resource: &Resource) -> Result<Vec<Self::Data>>;

    /// Export data to a file
    fn export(&self, data: &[Self::Data], path: &Path) -> Result<()>;

    /// Get the file extension for this exporter
    fn file_extension(&self) -> &'static str;
}

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::file::config::FileExporterConfig;
use crate::init::file_exporter::FileExporterFormat;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::error::Error;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// File exporter for traces
pub struct TracesFileExporter {
    output_dir: std::path::PathBuf,
    receiver: BoundedReceiver<Vec<ResourceSpans>>,
    format: FileExporterFormat,
    flush_interval: std::time::Duration,
    parquet_compression: crate::init::file_exporter::ParquetCompression,
}

impl TracesFileExporter {
    pub async fn start(
        self,
        cancel_token: CancellationToken,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        match self.format {
            FileExporterFormat::Parquet => {
                let exporter = Arc::new(
                    ParquetExporter::with_compression(self.parquet_compression.into()),
                );
                crate::exporters::file::task::run_traces_loop(
                    exporter,
                    self.output_dir,
                    self.receiver,
                    self.flush_interval,
                    cancel_token,
                )
                .await
                .map_err(|e| e.into())
            }
            FileExporterFormat::Json => {
                let exporter = Arc::new(crate::exporters::file::json::JsonExporter::new());
                crate::exporters::file::task::run_traces_loop(
                    exporter,
                    self.output_dir,
                    self.receiver,
                    self.flush_interval,
                    cancel_token,
                )
                .await
                .map_err(|e| e.into())
            }
        }
    }
}

/// File exporter for metrics
pub struct MetricsFileExporter {
    output_dir: std::path::PathBuf,
    receiver: BoundedReceiver<Vec<ResourceMetrics>>,
    format: FileExporterFormat,
    flush_interval: std::time::Duration,
    parquet_compression: crate::init::file_exporter::ParquetCompression,
}

impl MetricsFileExporter {
    pub async fn start(
        self,
        cancel_token: CancellationToken,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        match self.format {
            FileExporterFormat::Parquet => {
                let exporter = Arc::new(
                    ParquetExporter::with_compression(self.parquet_compression.into()),
                );
                crate::exporters::file::task::run_metrics_loop(
                    exporter,
                    self.output_dir,
                    self.receiver,
                    self.flush_interval,
                    cancel_token,
                )
                .await
                .map_err(|e| e.into())
            }
            FileExporterFormat::Json => {
                let exporter = Arc::new(crate::exporters::file::json::JsonExporter::new());
                crate::exporters::file::task::run_metrics_loop(
                    exporter,
                    self.output_dir,
                    self.receiver,
                    self.flush_interval,
                    cancel_token,
                )
                .await
                .map_err(|e| e.into())
            }
        }
    }
}

/// File exporter for logs
pub struct LogsFileExporter {
    output_dir: std::path::PathBuf,
    receiver: BoundedReceiver<Vec<ResourceLogs>>,
    format: FileExporterFormat,
    flush_interval: std::time::Duration,
    parquet_compression: crate::init::file_exporter::ParquetCompression,
}

impl LogsFileExporter {
    pub async fn start(
        self,
        cancel_token: CancellationToken,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        match self.format {
            FileExporterFormat::Parquet => {
                let exporter = Arc::new(
                    ParquetExporter::with_compression(self.parquet_compression.into()),
                );
                crate::exporters::file::task::run_logs_loop(
                    exporter,
                    self.output_dir,
                    self.receiver,
                    self.flush_interval,
                    cancel_token,
                )
                .await
                .map_err(|e| e.into())
            }
            FileExporterFormat::Json => {
                let exporter = Arc::new(crate::exporters::file::json::JsonExporter::new());
                crate::exporters::file::task::run_logs_loop(
                    exporter,
                    self.output_dir,
                    self.receiver,
                    self.flush_interval,
                    cancel_token,
                )
                .await
                .map_err(|e| e.into())
            }
        }
    }
}

/// Generic file exporter builder that handles setup for all telemetry types
pub struct FileExporterBuilder;

impl FileExporterBuilder {
    /// Create a traces file exporter
    pub fn build_traces_exporter(
        config: &FileExporterConfig,
        receiver: BoundedReceiver<Vec<ResourceSpans>>,
    ) -> std::result::Result<TracesFileExporter, Box<dyn Error + Send + Sync>> {
        let output_dir = config.output_dir.join("spans");
        std::fs::create_dir_all(&output_dir)
            .map_err(|e| format!("Failed to create traces directory: {}", e))?;

        Ok(TracesFileExporter {
            output_dir,
            receiver,
            format: config.format,
            flush_interval: config.flush_interval,
            parquet_compression: config.parquet_compression,
        })
    }

    /// Create a metrics file exporter
    pub fn build_metrics_exporter(
        config: &FileExporterConfig,
        receiver: BoundedReceiver<Vec<ResourceMetrics>>,
    ) -> std::result::Result<MetricsFileExporter, Box<dyn Error + Send + Sync>> {
        let output_dir = config.output_dir.join("metrics");
        std::fs::create_dir_all(&output_dir)
            .map_err(|e| format!("Failed to create metrics directory: {}", e))?;

        Ok(MetricsFileExporter {
            output_dir,
            receiver,
            format: config.format,
            flush_interval: config.flush_interval,
            parquet_compression: config.parquet_compression,
        })
    }

    /// Create a logs file exporter
    pub fn build_logs_exporter(
        config: &FileExporterConfig,
        receiver: BoundedReceiver<Vec<ResourceLogs>>,
    ) -> std::result::Result<LogsFileExporter, Box<dyn Error + Send + Sync>> {
        let output_dir = config.output_dir.join("logs");
        std::fs::create_dir_all(&output_dir)
            .map_err(|e| format!("Failed to create logs directory: {}", e))?;

        Ok(LogsFileExporter {
            output_dir,
            receiver,
            format: config.format,
            flush_interval: config.flush_interval,
            parquet_compression: config.parquet_compression,
        })
    }
}

pub mod config;
pub mod json;
pub mod parquet;
pub mod task;
