// SPDX-License-Identifier: Apache-2.0

use rdkafka::error::KafkaError;
use thiserror::Error;

/// Errors that can occur during Kafka export operations
#[derive(Error, Debug)]
pub enum KafkaExportError {
    /// Error from Kafka producer
    #[error("Kafka producer error: {0}")]
    ProducerError(#[from] KafkaError),
    
    /// Serialization error
    #[error("Failed to serialize telemetry data: {0}")]
    SerializationError(String),
    
    /// Configuration error
    #[error("Invalid configuration: {0}")]
    ConfigurationError(String),
    
    /// Topic not configured
    #[error("Topic not configured for telemetry type: {0}")]
    TopicNotConfigured(String),
    
    /// Send timeout
    #[error("Failed to send message within timeout")]
    SendTimeout,
    
    /// JSON serialization error
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
    
    /// Protobuf encoding error
    #[error("Protobuf encoding error: {0}")]
    ProtobufError(#[from] prost::EncodeError),
}

impl From<(KafkaError, rdkafka::message::OwnedMessage)> for KafkaExportError {
    fn from((error, _): (KafkaError, rdkafka::message::OwnedMessage)) -> Self {
        KafkaExportError::ProducerError(error)
    }
}

/// Result type for Kafka export operations
pub type Result<T> = std::result::Result<T, KafkaExportError>;