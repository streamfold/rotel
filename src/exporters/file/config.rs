use crate::init::file_exporter::FileExporterFormat;
use serde::{Deserialize, Serialize};
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileExporterConfig {
    /// The format to use for exporting files
    pub format: FileExporterFormat,

    /// The directory where files will be written
    pub path: PathBuf,

    /// How often to flush data to disk (e.g., "5s")
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
}

impl FileExporterConfig {
    /// Creates a new configuration with the required fields
    pub fn new(format: FileExporterFormat, path: PathBuf, flush_interval: Duration) -> Self {
        Self {
            format,
            path,
            flush_interval,
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
        // Format validation is now handled by the enum type itself, no need to validate

        // Validate path
        if !self.path.exists() {
            // Recovery: Suggest creating the directory
            return Err(ConfigError::InvalidPath(format!(
                "Path does not exist: {}. Create the directory or set a valid path.",
                self.path.display()
            )));
        }

        if !self.path.is_dir() {
            return Err(ConfigError::InvalidPath(format!(
                "Path is not a directory: {}",
                self.path.display()
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
            path: temp_dir.path().to_path_buf(),
            flush_interval: Duration::from_secs(5),
        };

        assert!(config.validate().is_ok());
    }

    // Note: Format validation test removed since enum prevents invalid formats at compile time

    #[test]
    fn test_invalid_flush_interval() {
        let temp_dir = tempdir().unwrap();
        let config = FileExporterConfig {
            format: FileExporterFormat::Parquet,
            path: temp_dir.path().to_path_buf(),
            flush_interval: Duration::from_secs(0),
        };

        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidFlushInterval(_))
        ));
    }
}
