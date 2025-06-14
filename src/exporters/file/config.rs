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
    /// The format to use for exporting files (e.g., "parquet")
    pub format: String,
    
    /// The directory where files will be written
    pub path: PathBuf,
    
    /// How often to flush data to disk (e.g., "5s")
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
}

impl FileExporterConfig {
    /// Creates a new configuration with the required fields
    pub fn new(format: String, path: PathBuf, flush_interval: Duration) -> Self {
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
    /// Returns `ConfigError::InvalidFormat` if the format is empty or unsupported,
    /// `ConfigError::InvalidPath` if the path does not exist or is not a directory,
    /// or `ConfigError::InvalidFlushInterval` if the flush interval is zero.
    ///
    /// # Recovery
    /// - Ensure the format is set to a supported value (e.g., "parquet" or "json").
    /// - Create the output directory if it does not exist.
    /// - Set a non-zero flush interval.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate format
        if self.format.is_empty() {
            return Err(ConfigError::InvalidFormat("Format cannot be empty".to_string()));
        }
        let allowed_formats = ["parquet", "json"];
        if !allowed_formats.contains(&self.format.to_lowercase().as_str()) {
            return Err(ConfigError::InvalidFormat(format!(
                "Unsupported format: {}. Supported formats are: parquet, json",
                self.format
            )));
        }
        
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
            format: "parquet".to_string(),
            path: temp_dir.path().to_path_buf(),
            flush_interval: Duration::from_secs(5),
        };
        
        assert!(config.validate().is_ok());
    }
    
    #[test]
    fn test_invalid_format() {
        let temp_dir = tempdir().unwrap();
        let config = FileExporterConfig {
            format: "".to_string(),
            path: temp_dir.path().to_path_buf(),
            flush_interval: Duration::from_secs(5),
        };
        
        assert!(matches!(config.validate(), Err(ConfigError::InvalidFormat(_))));
    }
    
    #[test]
    fn test_invalid_flush_interval() {
        let temp_dir = tempdir().unwrap();
        let config = FileExporterConfig {
            format: "parquet".to_string(),
            path: temp_dir.path().to_path_buf(),
            flush_interval: Duration::from_secs(0),
        };
        
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidFlushInterval(_))
        ));
    }
} 