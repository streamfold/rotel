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


pub mod config;
pub mod json;
pub mod parquet;
pub mod task;
