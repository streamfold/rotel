use crate::init::file_exporter::{FileExporterFormat, ParquetCompression};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Errors that can occur during configuration parsing and validation.
///
/// - `InvalidFormat`: The format field is missing or not supported.
///   Recovery: Set a valid format (e.g., "parquet").
/// - `InvalidPath`: The output path does not exist or is not a directory.
///   Recovery: Create the directory or correct the path.
/// - `InvalidFlushInterval`: The flush interval is zero or invalid.
///   Recovery: Set a non-zero flush interval (e.g., "5s").
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Invalid flush interval: {0}")]
    InvalidFlushInterval(String),
}

/// Configuration for the file exporter
#[derive(Debug, Clone)]
pub struct FileExporterConfig {
    /// The format to use for exporting files
    pub format: FileExporterFormat,

    /// The directory where files will be written
    pub output_dir: PathBuf,

    /// How often to flush data to disk (e.g., "5s")
    pub flush_interval: Duration,

    /// Compression type for Parquet files
    pub parquet_compression: ParquetCompression,
}

impl FileExporterConfig {
    /// Creates a new configuration with the required fields
    pub fn new(
        format: FileExporterFormat,
        output_dir: PathBuf,
        flush_interval: Duration,
        parquet_compression: ParquetCompression,
    ) -> Self {
        Self {
            format,
            output_dir,
            flush_interval,
            parquet_compression,
        }
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError::InvalidPath` if the path does not exist or is not a directory,
    /// or `ConfigError::InvalidFlushInterval` if the flush interval is zero.
    ///
    /// # Recovery
    /// - Create the output directory if it does not exist.
    /// - Set a non-zero flush interval.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate path
        if !self.output_dir.exists() {
            // Recovery: Suggest creating the directory
            return Err(ConfigError::InvalidPath(format!(
                "Path does not exist: {}. Create the directory or set a valid path.",
                self.output_dir.display()
            )));
        }

        if !self.output_dir.is_dir() {
            return Err(ConfigError::InvalidPath(format!(
                "Path is not a directory: {}",
                self.output_dir.display()
            )));
        }

        // Validate flush interval
        if self.flush_interval.is_zero() {
            return Err(ConfigError::InvalidFlushInterval(
                "Flush interval cannot be zero. Set a non-zero value (e.g., '5s').".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_valid_config() {
        let temp_dir = tempdir().unwrap();
        let config = FileExporterConfig {
            format: FileExporterFormat::Parquet,
            output_dir: temp_dir.path().to_path_buf(),
            flush_interval: Duration::from_secs(5),
            parquet_compression: ParquetCompression::Snappy,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_flush_interval() {
        let temp_dir = tempdir().unwrap();
        let config = FileExporterConfig {
            format: FileExporterFormat::Parquet,
            output_dir: temp_dir.path().to_path_buf(),
            flush_interval: Duration::from_secs(0),
            parquet_compression: ParquetCompression::Snappy,
        };

        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidFlushInterval(_))
        ));
    }
}
