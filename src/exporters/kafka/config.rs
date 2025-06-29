// SPDX-License-Identifier: Apache-2.0

use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::time::Duration;

/// Serialization format for Kafka messages
#[derive(Clone, Debug, PartialEq)]
pub enum SerializationFormat {
    /// JSON format
    Json,
    /// Protobuf format
    Protobuf,
}

impl Default for SerializationFormat {
    fn default() -> Self {
        SerializationFormat::Json
    }
}

/// Configuration for the Kafka exporter
#[derive(Clone, Debug)]
pub struct KafkaExporterConfig {
    /// Kafka broker addresses (comma-separated)
    pub brokers: String,

    /// Topic name for traces
    pub traces_topic: Option<String>,

    /// Topic name for metrics
    pub metrics_topic: Option<String>,

    /// Topic name for logs
    pub logs_topic: Option<String>,

    /// Serialization format
    pub serialization_format: SerializationFormat,

    /// Request timeout
    pub request_timeout: Duration,

    /// Producer configuration options
    pub producer_config: HashMap<String, String>,

    /// Enable compression
    pub compression: Option<String>,

    /// SASL username for authentication
    pub sasl_username: Option<String>,

    /// SASL password for authentication
    pub sasl_password: Option<String>,

    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub sasl_mechanism: Option<String>,

    /// Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
    pub security_protocol: Option<String>,
}

impl Default for KafkaExporterConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            traces_topic: Some("otlp_traces".to_string()),
            metrics_topic: Some("otlp_metrics".to_string()),
            logs_topic: Some("otlp_logs".to_string()),
            serialization_format: SerializationFormat::default(),
            request_timeout: Duration::from_secs(30),
            producer_config: HashMap::new(),
            compression: None,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: None,
        }
    }
}

impl KafkaExporterConfig {
    /// Create a new Kafka exporter configuration
    pub fn new(brokers: String) -> Self {
        Self {
            brokers,
            ..Default::default()
        }
    }

    /// Set the traces topic
    pub fn with_traces_topic(mut self, topic: String) -> Self {
        self.traces_topic = Some(topic);
        self
    }

    /// Set the metrics topic
    pub fn with_metrics_topic(mut self, topic: String) -> Self {
        self.metrics_topic = Some(topic);
        self
    }

    /// Set the logs topic
    pub fn with_logs_topic(mut self, topic: String) -> Self {
        self.logs_topic = Some(topic);
        self
    }

    /// Set the serialization format
    pub fn with_serialization_format(mut self, format: SerializationFormat) -> Self {
        self.serialization_format = format;
        self
    }

    /// Set compression type
    pub fn with_compression(mut self, compression: String) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Set SASL authentication
    pub fn with_sasl_auth(
        mut self,
        username: String,
        password: String,
        mechanism: String,
        security_protocol: String,
    ) -> Self {
        self.sasl_username = Some(username);
        self.sasl_password = Some(password);
        self.sasl_mechanism = Some(mechanism);
        self.security_protocol = Some(security_protocol);
        self
    }

    /// Build rdkafka ClientConfig from this configuration
    pub fn build_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        config.set("bootstrap.servers", &self.brokers);

        // Set compression if specified
        if let Some(ref compression) = self.compression {
            config.set("compression.type", compression);
        }

        // Set security configuration
        if let Some(ref protocol) = self.security_protocol {
            config.set("security.protocol", protocol);
        }

        // Set SASL configuration
        if let Some(ref mechanism) = self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism);
        }

        if let Some(ref username) = self.sasl_username {
            config.set("sasl.username", username);
        }

        if let Some(ref password) = self.sasl_password {
            config.set("sasl.password", password);
        }

        // Set additional producer configuration
        for (key, value) in &self.producer_config {
            config.set(key, value);
        }

        // Set some sensible defaults
        config.set("message.timeout.ms", "30000");
        config.set("request.timeout.ms", "30000");
        config.set("linger.ms", "5");
        config.set("batch.size", "16384");

        config
    }
}
