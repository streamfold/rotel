use thiserror::Error;

/// Errors that can occur during Kafka export operations
#[derive(Error, Debug)]
pub enum KafkaReceiverError {
    /// Configuration error
    #[error("Invalid configuration: {0}")]
    ConfigurationError(String),

    /// Send operation was canceled
    #[error("Send operation cancelled")]
    SendCancelled,

    /// Send operation failed
    #[error("Failed to send {signal_type}: {error}")]
    SendFailed { signal_type: String, error: String },
}

pub type Result<T> = std::result::Result<T, KafkaReceiverError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_configuration_error_display() {
        let error = KafkaReceiverError::ConfigurationError("Invalid broker address".to_string());
        assert_eq!(
            error.to_string(),
            "Invalid configuration: Invalid broker address"
        );
    }

    #[test]
    fn test_configuration_error_debug() {
        let error = KafkaReceiverError::ConfigurationError("Test error".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("ConfigurationError"));
        assert!(debug_str.contains("Test error"));
    }

    #[test]
    fn test_result_type_ok() {
        let result: Result<i32> = Ok(42);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_result_type_err() {
        let result: Result<i32> = Err(KafkaReceiverError::ConfigurationError("test".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test"));
    }
}
