// SPDX-License-Identifier: Apache-2.0

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
}

impl Default for KmsgReceiverConfig {
    fn default() -> Self {
        Self {
            priority_level: DEFAULT_PRIORITY_LEVEL,
            read_existing: false,
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout_ms: DEFAULT_BATCH_TIMEOUT_MS,
        }
    }
}

impl KmsgReceiverConfig {
    /// Create a new config with default batch settings
    pub fn new(priority_level: u8, read_existing: bool) -> Self {
        Self {
            // Clamp priority level to valid range 0-7
            priority_level: priority_level.min(MAX_PRIORITY_LEVEL),
            read_existing,
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout_ms: DEFAULT_BATCH_TIMEOUT_MS,
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
}
