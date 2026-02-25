// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::time::Duration;

/// Path to the kernel message device
pub const KMSG_DEVICE_PATH: &str = "/dev/kmsg";

/// Default priority level filter (6 = LOG_INFO, includes all priorities <= 6)
pub const DEFAULT_PRIORITY_LEVEL: u8 = 6;

/// Maximum valid priority level (DEBUG)
pub const MAX_PRIORITY_LEVEL: u8 = 7;

/// Default maximum number of log records to batch before sending
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Default maximum time to wait before flushing a batch (milliseconds)
pub const DEFAULT_BATCH_TIMEOUT_MS: u64 = 250;

/// Default path for persisting kmsg read offset
pub const DEFAULT_OFFSETS_PATH: &str = "/var/lib/rotel/kmsg_offsets.json";

/// Default checkpoint interval for persisting offset to disk (milliseconds)
pub const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 5000;

/// Minimum allowed checkpoint interval (milliseconds).
pub const MIN_CHECKPOINT_INTERVAL_MS: u64 = 100;

/// Default maximum duration to tolerate consecutive read errors before exiting.
pub const DEFAULT_MAX_READ_ERROR_DURATION_SECS: u64 = 60;

/// Configuration for the kmsg receiver
#[derive(Debug, Clone)]
pub struct KmsgReceiverConfig {
    /// Maximum priority level to include (0=emerg, 1=alert, 2=crit, 3=err, 4=warn, 5=notice, 6=info, 7=debug)
    /// Messages with priority <= this value will be included.
    /// Values > 7 are clamped to 7.
    pub priority_level: u8,

    /// Whether to read existing messages from the kernel ring buffer on startup
    /// If false, only new messages will be read
    pub read_existing: bool,

    /// Maximum number of log records to batch before sending
    pub batch_size: usize,

    /// Maximum time to wait before flushing a batch (milliseconds)
    pub batch_timeout_ms: u64,

    /// Path to persist kmsg read offset for resume across restarts.
    /// The receiver saves the last-read sequence number to this file so it can
    /// skip already-processed messages on restart. Uses boot_id to detect device
    /// reboots and invalidate stale state.
    /// Set to `None` to disable offset persistence entirely.
    pub offsets_path: Option<PathBuf>,

    /// How often to checkpoint the current offset to disk (milliseconds).
    /// Only used when `offsets_path` is `Some`.
    pub checkpoint_interval_ms: u64,

    /// Maximum duration to tolerate consecutive read errors before exiting.
    /// If reads from /dev/kmsg fail continuously for this duration, the receiver
    /// will exit with an error. This prevents silent infinite retries when the
    /// device becomes inaccessible (e.g., permission changes, container issues).
    pub max_read_error_duration: Duration,
}

impl Default for KmsgReceiverConfig {
    fn default() -> Self {
        Self {
            priority_level: DEFAULT_PRIORITY_LEVEL,
            read_existing: false,
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout_ms: DEFAULT_BATCH_TIMEOUT_MS,
            offsets_path: Some(PathBuf::from(DEFAULT_OFFSETS_PATH)),
            checkpoint_interval_ms: DEFAULT_CHECKPOINT_INTERVAL_MS,
            max_read_error_duration: Duration::from_secs(DEFAULT_MAX_READ_ERROR_DURATION_SECS),
        }
    }
}

impl KmsgReceiverConfig {
    /// Create a new config with default batch and persistence settings
    pub fn new(priority_level: u8, read_existing: bool) -> Self {
        Self {
            // Clamp priority level to valid range 0-7
            priority_level: priority_level.min(MAX_PRIORITY_LEVEL),
            read_existing,
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout_ms: DEFAULT_BATCH_TIMEOUT_MS,
            offsets_path: Some(PathBuf::from(DEFAULT_OFFSETS_PATH)),
            checkpoint_interval_ms: DEFAULT_CHECKPOINT_INTERVAL_MS,
            max_read_error_duration: Duration::from_secs(DEFAULT_MAX_READ_ERROR_DURATION_SECS),
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.priority_level > MAX_PRIORITY_LEVEL {
            return Err(format!(
                "Priority level {} exceeds maximum {}",
                self.priority_level, MAX_PRIORITY_LEVEL
            ));
        }

        if self.batch_size == 0 {
            return Err("Batch size must be at least 1, got 0".to_string());
        }

        if self.batch_timeout_ms < 10 {
            return Err(format!(
                "Batch timeout must be at least 10ms, got {}ms",
                self.batch_timeout_ms
            ));
        }

        if self.offsets_path.is_some() && self.checkpoint_interval_ms < MIN_CHECKPOINT_INTERVAL_MS {
            return Err(format!(
                "Checkpoint interval must be at least {}ms, got {}ms",
                MIN_CHECKPOINT_INTERVAL_MS, self.checkpoint_interval_ms
            ));
        }

        Ok(())
    }

    /// Set custom batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set custom batch timeout in milliseconds
    pub fn with_batch_timeout_ms(mut self, batch_timeout_ms: u64) -> Self {
        self.batch_timeout_ms = batch_timeout_ms;
        self
    }

    /// Set custom offsets path for persistence, or `None` to disable
    pub fn with_offsets_path(mut self, offsets_path: Option<PathBuf>) -> Self {
        self.offsets_path = offsets_path;
        self
    }

    /// Set a custom checkpoint interval in milliseconds.
    ///
    /// Minimum value is 100ms.
    pub fn with_checkpoint_interval_ms(mut self, checkpoint_interval_ms: u64) -> Self {
        self.checkpoint_interval_ms = checkpoint_interval_ms;
        self
    }

    /// Set maximum duration to tolerate consecutive read errors before exiting.
    pub fn with_max_read_error_duration(mut self, duration: Duration) -> Self {
        self.max_read_error_duration = duration;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_level_clamping() {
        // Valid values should pass through
        let config = KmsgReceiverConfig::new(4, false);
        assert_eq!(config.priority_level, 4);

        // Max valid value
        let config = KmsgReceiverConfig::new(7, false);
        assert_eq!(config.priority_level, 7);

        // Values > 7 should be clamped to 7
        let config = KmsgReceiverConfig::new(8, false);
        assert_eq!(config.priority_level, 7);

        let config = KmsgReceiverConfig::new(255, false);
        assert_eq!(config.priority_level, 7);
    }

    #[test]
    fn test_default_config() {
        let config = KmsgReceiverConfig::default();
        assert_eq!(config.priority_level, 6);
        assert!(!config.read_existing);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.batch_timeout_ms, 250);
    }

    #[test]
    fn test_builder_methods() {
        let config = KmsgReceiverConfig::new(6, false)
            .with_batch_size(50)
            .with_batch_timeout_ms(500);

        assert_eq!(config.batch_size, 50);
        assert_eq!(config.batch_timeout_ms, 500);
    }

    #[test]
    fn test_validate_invalid_batch_size() {
        let config = KmsgReceiverConfig::new(6, false).with_batch_size(0);
        let result = config.validate();
        assert_eq!(
            result,
            Err("Batch size must be at least 1, got 0".to_string())
        );
    }

    #[test]
    fn test_validate_invalid_batch_timeout() {
        let config = KmsgReceiverConfig::new(6, false).with_batch_timeout_ms(5);
        let result = config.validate();
        assert_eq!(
            result,
            Err("Batch timeout must be at least 10ms, got 5ms".to_string())
        );
    }

    #[test]
    fn test_validate_valid_config() {
        let config = KmsgReceiverConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_priority_clamped_in_constructor() {
        // Constructor clamps priority, so validate should always pass for priority
        let config = KmsgReceiverConfig::new(255, false);
        assert_eq!(config.priority_level, 7); // Clamped
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_checkpoint_interval() {
        let config = KmsgReceiverConfig::new(6, false).with_checkpoint_interval_ms(50);
        let result = config.validate();
        assert_eq!(
            result,
            Err("Checkpoint interval must be at least 100ms, got 50ms".to_string())
        );
    }

    #[test]
    fn test_validate_checkpoint_interval_skipped_when_persistence_disabled() {
        let config = KmsgReceiverConfig::new(6, false)
            .with_offsets_path(None)
            .with_checkpoint_interval_ms(1);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_builder_offsets_path() {
        let config = KmsgReceiverConfig::new(6, false)
            .with_offsets_path(Some(PathBuf::from("/tmp/test.json")));
        assert_eq!(config.offsets_path, Some(PathBuf::from("/tmp/test.json")));
    }

    #[test]
    fn test_builder_offsets_path_disabled() {
        let config = KmsgReceiverConfig::new(6, false).with_offsets_path(None);
        assert_eq!(config.offsets_path, None);
    }

    #[test]
    fn test_default_offsets_path() {
        let config = KmsgReceiverConfig::default();
        assert_eq!(
            config.offsets_path,
            Some(PathBuf::from("/var/lib/rotel/kmsg_offsets.json"))
        );
        assert_eq!(config.checkpoint_interval_ms, 5000);
    }
}
