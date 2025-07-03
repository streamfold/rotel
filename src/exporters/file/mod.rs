use std::path::Path;
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

/// Trait defining the core functionality for file exporters.
///
/// All methods return a `Result` with detailed error context. Implementations should provide
/// actionable error messages and propagate context for easier debugging and recovery.
pub trait FileExporter: Send + Sync {
    /// Export data to a file at the specified path.
    ///
    /// # Errors
    ///
    /// Returns `FileExporterError::Io` for file system errors,
    /// `FileExporterError::InvalidData` for malformed data,
    /// or `FileExporterError::Export` for Parquet/Arrow failures.
    fn export(&self, data: &[u8], path: &Path) -> Result<()>;

    /// Validate the data before export.
    ///
    /// # Errors
    ///
    /// Returns `FileExporterError::InvalidData` if the data is not valid for export.
    fn validate(&self, data: &[u8]) -> Result<()>;

    /// Get list of supported file formats.
    fn get_supported_formats(&self) -> Vec<String>;
}

pub mod config;
pub mod json;
pub mod parquet;
pub mod task;
