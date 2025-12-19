use serde::Deserialize;
use std::time::Duration;

/// Where to start reading from when a file is first discovered
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartAt {
    /// Start reading from the beginning of the file
    Beginning,
    /// Start reading from the end of the file (only new content)
    #[default]
    End,
}

/// Configuration for the file input operator
#[derive(Debug, Clone, Deserialize)]
pub struct FileInputConfig {
    /// Unique identifier for this operator
    #[serde(default = "default_id")]
    pub id: String,

    /// Glob patterns for files to include
    pub include: Vec<String>,

    /// Glob patterns for files to exclude
    #[serde(default)]
    pub exclude: Vec<String>,

    /// How often to poll for file changes (in milliseconds)
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Where to start reading new files from
    #[serde(default)]
    pub start_at: StartAt,

    /// Size of fingerprint to use for file identification (in bytes)
    #[serde(default = "default_fingerprint_size")]
    pub fingerprint_size: usize,

    /// Maximum size of a single log entry (in bytes)
    #[serde(default = "default_max_log_size")]
    pub max_log_size: usize,

    /// Maximum number of files to read concurrently
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,

    /// Whether to include the file name as a label
    #[serde(default = "default_true")]
    pub include_file_name: bool,

    /// Whether to include the file path as a label
    #[serde(default)]
    pub include_file_path: bool,
}

fn default_id() -> String {
    "file_input".to_string()
}

fn default_poll_interval_ms() -> u64 {
    200
}

fn default_fingerprint_size() -> usize {
    1000
}

fn default_max_log_size() -> usize {
    1024 * 1024 // 1MB
}

fn default_max_concurrent_files() -> usize {
    512
}

fn default_true() -> bool {
    true
}

impl Default for FileInputConfig {
    fn default() -> Self {
        Self {
            id: default_id(),
            include: vec![],
            exclude: vec![],
            poll_interval_ms: default_poll_interval_ms(),
            start_at: StartAt::default(),
            fingerprint_size: default_fingerprint_size(),
            max_log_size: default_max_log_size(),
            max_concurrent_files: default_max_concurrent_files(),
            include_file_name: true,
            include_file_path: false,
        }
    }
}

impl FileInputConfig {
    /// Get the poll interval as a Duration
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.include.is_empty() {
            return Err("include patterns cannot be empty".to_string());
        }

        if self.fingerprint_size < 16 {
            return Err("fingerprint_size must be at least 16 bytes".to_string());
        }

        if self.max_log_size == 0 {
            return Err("max_log_size must be positive".to_string());
        }

        if self.max_concurrent_files < 2 {
            return Err("max_concurrent_files must be at least 2".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = FileInputConfig::default();
        assert_eq!(config.poll_interval_ms, 200);
        assert_eq!(config.fingerprint_size, 1000);
        assert_eq!(config.max_log_size, 1024 * 1024);
        assert_eq!(config.start_at, StartAt::End);
    }

    #[test]
    fn test_config_validation() {
        let mut config = FileInputConfig::default();
        config.include = vec!["/var/log/*.log".to_string()];

        assert!(config.validate().is_ok());

        config.include = vec![];
        assert!(config.validate().is_err());

        config.include = vec!["/var/log/*.log".to_string()];
        config.fingerprint_size = 10;
        assert!(config.validate().is_err());
    }
}
