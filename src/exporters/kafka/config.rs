// SPDX-License-Identifier: Apache-2.0

use rdkafka::ClientConfig;
use std::collections::HashMap;

/// Serialization format for Kafka messages
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub enum SerializationFormat {
    /// JSON format
    Json,
    /// Protobuf format
    Protobuf,
}

/// Kafka acknowledgement configuration
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub enum AcknowledgementMode {
    /// No acknowledgement required (acks=0) - fastest but least durable
    None,
    /// Wait for leader acknowledgement only (acks=1) - middle ground
    One,
    /// Wait for all in-sync replicas to acknowledge (acks=all) - slowest but most durable
    All,
}

impl Default for SerializationFormat {
    fn default() -> Self {
        SerializationFormat::Protobuf
    }
}

impl Default for AcknowledgementMode {
    fn default() -> Self {
        // Default to waiting for leader acknowledgement (acks=1) as a reasonable balance
        AcknowledgementMode::One
    }
}

impl AcknowledgementMode {
    /// Convert to the string value expected by librdkafka
    pub fn to_kafka_value(&self) -> &'static str {
        match self {
            AcknowledgementMode::None => "0",
            AcknowledgementMode::One => "1",
            AcknowledgementMode::All => "all",
        }
    }
}

/// Kafka partitioner type
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub enum PartitionerType {
    /// Consistent hash partitioner
    Consistent,
    /// Random partitioner using consistent hashing
    ConsistentRandom,
    /// Random partitioner using murmur2 hashing
    Murmur2Random,
    /// Murmur2 hash partitioner
    Murmur2,
    /// FNV-1a hash partitioner
    Fnv1a,
    /// Random partitioner using FNV-1a hashing
    Fnv1aRandom,
}

impl PartitionerType {
    /// Convert to the string value expected by librdkafka
    pub fn to_kafka_value(&self) -> &'static str {
        match self {
            PartitionerType::Consistent => "consistent",
            PartitionerType::ConsistentRandom => "consistent_random",
            PartitionerType::Murmur2Random => "murmur2_random",
            PartitionerType::Murmur2 => "murmur2",
            PartitionerType::Fnv1a => "fnv1a",
            PartitionerType::Fnv1aRandom => "fnv1a_random",
        }
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

    /// Acknowledgement mode for producer
    pub acks: AcknowledgementMode,

    /// Client ID for the Kafka producer
    pub client_id: String,

    /// Maximum message size in bytes
    pub max_message_bytes: usize,

    /// Linger time in milliseconds (queue.buffering.max.ms)
    pub linger_ms: u32,

    /// Number of retries (message.send.max.retries)
    pub retries: u32,

    /// Retry backoff time in milliseconds
    pub retry_backoff_ms: u32,

    /// Maximum retry backoff time in milliseconds
    pub retry_backoff_max_ms: u32,

    /// Message timeout in milliseconds
    pub message_timeout_ms: u32,

    /// Request timeout in milliseconds  
    pub request_timeout_ms: u32,

    /// Batch size in bytes
    pub batch_size: u32,

    /// Partitioner type
    pub partitioner: Option<PartitionerType>,

    /// Partition metrics by resource attributes for better consumer organization
    pub partition_metrics_by_resource_attributes: bool,

    /// Partition logs by resource attributes for better consumer organization
    pub partition_logs_by_resource_attributes: bool,

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
            acks: AcknowledgementMode::default(),
            client_id: "rotel".to_string(),
            max_message_bytes: 1000000,
            linger_ms: 5,
            retries: 2147483647,
            retry_backoff_ms: 100,
            retry_backoff_max_ms: 1000,
            message_timeout_ms: 300000,
            request_timeout_ms: 30000,
            batch_size: 1000000,
            partitioner: Some(PartitionerType::ConsistentRandom),
            partition_metrics_by_resource_attributes: false,
            partition_logs_by_resource_attributes: false,
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

    /// Set acknowledgement mode
    pub fn with_acks(mut self, acks: AcknowledgementMode) -> Self {
        self.acks = acks;
        self
    }

    /// Set client ID
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = client_id;
        self
    }

    /// Set maximum message size in bytes
    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    /// Set linger time in milliseconds
    pub fn with_linger_ms(mut self, linger_ms: u32) -> Self {
        self.linger_ms = linger_ms;
        self
    }

    /// Set number of retries
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set retry backoff time in milliseconds
    pub fn with_retry_backoff_ms(mut self, retry_backoff_ms: u32) -> Self {
        self.retry_backoff_ms = retry_backoff_ms;
        self
    }

    /// Set maximum retry backoff time in milliseconds
    pub fn with_retry_backoff_max_ms(mut self, retry_backoff_max_ms: u32) -> Self {
        self.retry_backoff_max_ms = retry_backoff_max_ms;
        self
    }

    /// Set message timeout in milliseconds
    pub fn with_message_timeout_ms(mut self, message_timeout_ms: u32) -> Self {
        self.message_timeout_ms = message_timeout_ms;
        self
    }

    /// Set request timeout in milliseconds
    pub fn with_request_timeout_ms(mut self, request_timeout_ms: u32) -> Self {
        self.request_timeout_ms = request_timeout_ms;
        self
    }

    /// Set batch size in bytes
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set partitioner type
    pub fn with_partitioner(mut self, partitioner: PartitionerType) -> Self {
        self.partitioner = Some(partitioner);
        self
    }

    /// Enable partitioning metrics by resource attributes
    pub fn with_partition_metrics_by_resource_attributes(mut self, enabled: bool) -> Self {
        self.partition_metrics_by_resource_attributes = enabled;
        self
    }

    /// Enable partitioning logs by resource attributes
    pub fn with_partition_logs_by_resource_attributes(mut self, enabled: bool) -> Self {
        self.partition_logs_by_resource_attributes = enabled;
        self
    }

    /// Set custom producer configuration parameters
    pub fn with_custom_config(mut self, custom_config: Vec<(String, String)>) -> Self {
        for (key, value) in custom_config {
            self.producer_config.insert(key, value);
        }
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

        // Set client ID
        config.set("client.id", &self.client_id);

        // Set maximum message size
        config.set("message.max.bytes", &self.max_message_bytes.to_string());

        // Set linger time (queue.buffering.max.ms)
        config.set("linger.ms", &self.linger_ms.to_string());

        // Set retry configuration
        config.set("retries", &self.retries.to_string());
        config.set("retry.backoff.ms", &self.retry_backoff_ms.to_string());
        config.set(
            "retry.backoff.max.ms",
            &self.retry_backoff_max_ms.to_string(),
        );

        // Set acknowledgement mode
        config.set("acks", self.acks.to_kafka_value());

        // Set partitioner if specified
        if let Some(ref partitioner) = self.partitioner {
            config.set("partitioner", partitioner.to_kafka_value());
        }

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

        // Set timeout and batch configuration
        config.set("message.timeout.ms", &self.message_timeout_ms.to_string());
        config.set("request.timeout.ms", &self.request_timeout_ms.to_string());
        config.set("batch.size", &self.batch_size.to_string());

        // Set custom producer configuration (overrides built-in options if conflicts exist)
        for (key, value) in &self.producer_config {
            config.set(key, value);
        }

        config
    }
}
