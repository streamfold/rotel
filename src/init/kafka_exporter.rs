// SPDX-License-Identifier: Apache-2.0

use crate::exporters::kafka::config::{
    AcknowledgementMode, Compression, KafkaExporterConfig, PartitionerType, SaslMechanism,
    SecurityProtocol, SerializationFormat,
};
use crate::init::parse::parse_key_val;
use clap::{Args, ValueEnum};
use serde::Deserialize;

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaExporterArgs {
    /// Kafka broker addresses (comma-separated)
    #[arg(
        id("KAFKA_EXPORTER_BROKERS"),
        long("kafka-exporter-brokers"),
        env = "ROTEL_KAFKA_EXPORTER_BROKERS",
        default_value = "localhost:9092"
    )]
    pub brokers: String,

    /// Topic name for traces
    #[arg(
        id("KAFKA_EXPORTER_TRACES_TOPIC"),
        long("kafka-exporter-traces-topic"),
        env = "ROTEL_KAFKA_EXPORTER_TRACES_TOPIC",
        default_value = "otlp_traces"
    )]
    pub traces_topic: String,

    /// Topic name for metrics
    #[arg(
        id("KAFKA_EXPORTER_METRICS_TOPIC"),
        long("kafka-exporter-metrics-topic"),
        env = "ROTEL_KAFKA_EXPORTER_METRICS_TOPIC",
        default_value = "otlp_metrics"
    )]
    pub metrics_topic: String,

    /// Topic name for logs
    #[arg(
        id("KAFKA_EXPORTER_LOGS_TOPIC"),
        long("kafka-exporter-logs-topic"),
        env = "ROTEL_KAFKA_EXPORTER_LOGS_TOPIC",
        default_value = "otlp_logs"
    )]
    pub logs_topic: String,

    /// Serialization format
    #[arg(
        id("KAFKA_EXPORTER_FORMAT"),
        value_enum,
        long("kafka-exporter-format"),
        env = "ROTEL_KAFKA_EXPORTER_FORMAT",
        default_value = "protobuf"
    )]
    pub format: KafkaSerializationFormat,

    /// Compression type
    #[arg(
        id("KAFKA_EXPORTER_COMPRESSION"),
        long("kafka-exporter-compression"),
        env = "ROTEL_KAFKA_EXPORTER_COMPRESSION",
        default_value = "none"
    )]
    pub compression: KafkaCompression,

    /// Acknowledgement mode
    #[arg(
        value_enum,
        long("kafka-exporter-acks"),
        env = "ROTEL_KAFKA_EXPORTER_ACKS",
        default_value = "one"
    )]
    pub acks: KafkaAcknowledgementMode,

    /// Client ID for the Kafka producer
    #[arg(
        id("KAFKA_EXPORTER_CLIENT_ID"),
        long("kafka-exporter-client-id"),
        env = "ROTEL_KAFKA_EXPORTER_CLIENT_ID",
        default_value = "rotel"
    )]
    #[serde(deserialize_with = "crate::init::parse::deser_into_string")]
    pub client_id: String,

    /// Maximum message size in bytes
    #[arg(
        long("kafka-exporter-max-message-bytes"),
        env = "ROTEL_KAFKA_EXPORTER_MAX_MESSAGE_BYTES",
        default_value = "1000000"
    )]
    pub max_message_bytes: usize,

    /// Linger time in milliseconds
    #[arg(
        long("kafka-exporter-linger-ms"),
        env = "ROTEL_KAFKA_EXPORTER_LINGER_MS",
        default_value = "5"
    )]
    pub linger_ms: u32,

    /// Number of retries for message sending
    #[arg(
        long("kafka-exporter-retries"),
        env = "ROTEL_KAFKA_EXPORTER_RETRIES",
        default_value = "2147483647"
    )]
    pub retries: u32,

    /// Retry backoff time in milliseconds
    #[arg(
        long("kafka-exporter-retry-backoff-ms"),
        env = "ROTEL_KAFKA_EXPORTER_RETRY_BACKOFF_MS",
        default_value = "100"
    )]
    pub retry_backoff_ms: u32,

    /// Maximum retry backoff time in milliseconds
    #[arg(
        long("kafka-exporter-retry-backoff-max-ms"),
        env = "ROTEL_KAFKA_EXPORTER_RETRY_BACKOFF_MAX_MS",
        default_value = "1000"
    )]
    pub retry_backoff_max_ms: u32,

    /// Message timeout in milliseconds
    #[arg(
        long("kafka-exporter-message-timeout-ms"),
        env = "ROTEL_KAFKA_EXPORTER_MESSAGE_TIMEOUT_MS",
        default_value = "300000"
    )]
    pub message_timeout_ms: u32,

    /// Request timeout in milliseconds
    #[arg(
        id("KAFKA_EXPORTER_REQUEST_TIMEOUT_MS"),
        long("kafka-exporter-request-timeout-ms"),
        env = "ROTEL_KAFKA_EXPORTER_REQUEST_TIMEOUT_MS",
        default_value = "30000"
    )]
    pub request_timeout_ms: u32,

    /// Batch size in bytes
    #[arg(
        long("kafka-exporter-batch-size"),
        env = "ROTEL_KAFKA_EXPORTER_BATCH_SIZE",
        default_value = "1000000"
    )]
    pub batch_size: u32,

    /// Partitioner type
    #[arg(
        value_enum,
        long("kafka-exporter-partitioner"),
        env = "ROTEL_KAFKA_EXPORTER_PARTITIONER",
        default_value = "consistent-random"
    )]
    pub partitioner: KafkaPartitionerType,

    /// Partition metrics by resource attributes for better consumer organization
    #[arg(
        long("kafka-exporter-partition-metrics-by-resource-attributes"),
        env = "ROTEL_KAFKA_EXPORTER_PARTITION_METRICS_BY_RESOURCE_ATTRIBUTES",
        default_value = "false"
    )]
    pub partition_metrics_by_resource_attributes: bool,

    /// Partition logs by resource attributes for better consumer organization
    #[arg(
        long("kafka-exporter-partition-logs-by-resource-attributes"),
        env = "ROTEL_KAFKA_EXPORTER_PARTITION_LOGS_BY_RESOURCE_ATTRIBUTES",
        default_value = "false"
    )]
    pub partition_logs_by_resource_attributes: bool,

    /// Custom Kafka producer configuration parameters (key=value pairs). These will override built-in options if conflicts exist.
    #[arg(
        id("KAFKA_EXPORTER_CUSTOM_CONFIG"),
        long("kafka-exporter-custom-config"),
        env = "ROTEL_KAFKA_EXPORTER_CUSTOM_CONFIG",
        value_parser = parse_key_val::<String, String>,
        value_delimiter = ','
    )]
    #[serde(deserialize_with = "crate::init::parse::deserialize_key_value_pairs")]
    pub custom_config: Vec<(String, String)>,

    /// SASL username for authentication
    #[arg(
        id("KAFKA_EXPORTER_SASL_USERNAME"),
        long("kafka-exporter-sasl-username"),
        env = "ROTEL_KAFKA_EXPORTER_SASL_USERNAME"
    )]
    #[serde(deserialize_with = "crate::init::parse::deser_into_string_opt")]
    pub sasl_username: Option<String>,

    /// SASL password for authentication
    #[arg(
        id("KAFKA_EXPORTER_SASL_PASSWORD"),
        long("kafka-exporter-sasl-password"),
        env = "ROTEL_KAFKA_EXPORTER_SASL_PASSWORD"
    )]
    #[serde(deserialize_with = "crate::init::parse::deser_into_string_opt")]
    pub sasl_password: Option<String>,

    /// SASL mechanism
    #[arg(
        id("KAFKA_EXPORTER_SASL_MECHANISM"),
        long("kafka-exporter-sasl-mechanism"),
        env = "ROTEL_KAFKA_EXPORTER_SASL_MECHANISM"
    )]
    pub sasl_mechanism: Option<KafkaSaslMechanism>,

    /// Security protocol
    #[arg(
        id("KAFKA_EXPORTER_SECURITY_PROTOCOL"),
        long("kafka-exporter-security-protocol"),
        env = "ROTEL_KAFKA_EXPORTER_SECURITY_PROTOCOL",
        default_value = "plaintext"
    )]
    pub security_protocol: KafkaSecurityProtocol,
}

impl Default for KafkaExporterArgs {
    fn default() -> Self {
        KafkaExporterArgs {
            brokers: "localhost:9092".to_string(),
            traces_topic: "otlp_traces".to_string(),
            metrics_topic: "otlp_metrics".to_string(),
            logs_topic: "otlp_logs".to_string(),
            format: Default::default(),
            acks: Default::default(),
            client_id: "rotel".to_string(),
            max_message_bytes: 1000000,
            linger_ms: 5,
            retries: 2147483647,
            retry_backoff_ms: 100,
            retry_backoff_max_ms: 1000,
            message_timeout_ms: 300000,
            request_timeout_ms: 30000,
            batch_size: 1000000,
            partitioner: Default::default(),
            partition_metrics_by_resource_attributes: false,
            partition_logs_by_resource_attributes: false,
            custom_config: vec![],
            compression: Default::default(),
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: Default::default(),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkaSerializationFormat {
    Json,
    Protobuf,
}

impl Default for KafkaSerializationFormat {
    fn default() -> Self {
        KafkaSerializationFormat::Protobuf
    }
}

#[derive(Copy, Clone, PartialEq, Debug, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkaCompression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Default for KafkaCompression {
    fn default() -> Self {
        KafkaCompression::None
    }
}

#[derive(Copy, Clone, PartialEq, Debug, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkaAcknowledgementMode {
    /// No acknowledgement required (acks=0) - fastest but least durable
    None,
    /// Wait for leader acknowledgement only (acks=1) - balanced
    One,
    /// Wait for all in-sync replicas (acks=all) - slowest but most durable
    All,
}

impl Default for KafkaAcknowledgementMode {
    fn default() -> Self {
        KafkaAcknowledgementMode::One
    }
}

#[derive(Copy, Clone, PartialEq, Debug, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KafkaSaslMechanism {
    /// SASL/PLAIN mechanism
    Plain,
    /// SASL/SCRAM-SHA-256 mechanism
    ScramSha256,
    /// SASL/SCRAM-SHA-512 mechanism
    ScramSha512,
}

impl Default for KafkaSaslMechanism {
    fn default() -> Self {
        KafkaSaslMechanism::Plain
    }
}

#[derive(Copy, Clone, PartialEq, Debug, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KafkaSecurityProtocol {
    /// Plaintext protocol
    Plaintext,
    /// SSL/TLS protocol
    Ssl,
    /// SASL/PLAINTEXT protocol
    SaslPlaintext,
    /// SASL/SSL protocol
    SaslSsl,
}

impl Default for KafkaSecurityProtocol {
    fn default() -> Self {
        KafkaSecurityProtocol::Plaintext
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KafkaPartitionerType {
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

impl Default for KafkaPartitionerType {
    fn default() -> Self {
        KafkaPartitionerType::ConsistentRandom
    }
}

impl From<KafkaCompression> for Compression {
    fn from(value: KafkaCompression) -> Self {
        match value {
            KafkaCompression::None => Compression::None,
            KafkaCompression::Gzip => Compression::Gzip,
            KafkaCompression::Snappy => Compression::Snappy,
            KafkaCompression::Lz4 => Compression::Lz4,
            KafkaCompression::Zstd => Compression::Zstd,
        }
    }
}

impl From<KafkaSaslMechanism> for SaslMechanism {
    fn from(value: KafkaSaslMechanism) -> Self {
        match value {
            KafkaSaslMechanism::Plain => SaslMechanism::Plain,
            KafkaSaslMechanism::ScramSha256 => SaslMechanism::ScramSha256,
            KafkaSaslMechanism::ScramSha512 => SaslMechanism::ScramSha512,
        }
    }
}

impl From<KafkaSecurityProtocol> for SecurityProtocol {
    fn from(value: KafkaSecurityProtocol) -> Self {
        match value {
            KafkaSecurityProtocol::Plaintext => SecurityProtocol::Plaintext,
            KafkaSecurityProtocol::Ssl => SecurityProtocol::Ssl,
            KafkaSecurityProtocol::SaslPlaintext => SecurityProtocol::SaslPlaintext,
            KafkaSecurityProtocol::SaslSsl => SecurityProtocol::SaslSsl,
        }
    }
}

impl From<KafkaSerializationFormat> for SerializationFormat {
    fn from(value: KafkaSerializationFormat) -> Self {
        match value {
            KafkaSerializationFormat::Json => SerializationFormat::Json,
            KafkaSerializationFormat::Protobuf => SerializationFormat::Protobuf,
        }
    }
}

impl From<KafkaAcknowledgementMode> for AcknowledgementMode {
    fn from(value: KafkaAcknowledgementMode) -> Self {
        match value {
            KafkaAcknowledgementMode::None => AcknowledgementMode::None,
            KafkaAcknowledgementMode::One => AcknowledgementMode::One,
            KafkaAcknowledgementMode::All => AcknowledgementMode::All,
        }
    }
}

impl From<KafkaPartitionerType> for PartitionerType {
    fn from(value: KafkaPartitionerType) -> Self {
        match value {
            KafkaPartitionerType::Consistent => PartitionerType::Consistent,
            KafkaPartitionerType::ConsistentRandom => PartitionerType::ConsistentRandom,
            KafkaPartitionerType::Murmur2Random => PartitionerType::Murmur2Random,
            KafkaPartitionerType::Murmur2 => PartitionerType::Murmur2,
            KafkaPartitionerType::Fnv1a => PartitionerType::Fnv1a,
            KafkaPartitionerType::Fnv1aRandom => PartitionerType::Fnv1aRandom,
        }
    }
}

impl KafkaExporterArgs {
    pub fn build_config(&self) -> KafkaExporterConfig {
        let mut config = KafkaExporterConfig::new(self.brokers.clone())
            .with_traces_topic(self.traces_topic.clone())
            .with_metrics_topic(self.metrics_topic.clone())
            .with_logs_topic(self.logs_topic.clone())
            .with_serialization_format(self.format.into())
            .with_acks(self.acks.into())
            .with_client_id(self.client_id.clone())
            .with_compression(self.compression.into())
            .with_max_message_bytes(self.max_message_bytes)
            .with_linger_ms(self.linger_ms)
            .with_retries(self.retries)
            .with_retry_backoff_ms(self.retry_backoff_ms)
            .with_retry_backoff_max_ms(self.retry_backoff_max_ms)
            .with_message_timeout_ms(self.message_timeout_ms)
            .with_request_timeout_ms(self.request_timeout_ms)
            .with_batch_size(self.batch_size)
            .with_partitioner(self.partitioner.into())
            .with_partition_metrics_by_resource_attributes(
                self.partition_metrics_by_resource_attributes,
            )
            .with_partition_logs_by_resource_attributes(self.partition_logs_by_resource_attributes)
            .with_custom_config(self.custom_config.clone());

        // Configure SASL if credentials are provided
        if let (Some(username), Some(password), Some(mechanism)) = (
            &self.sasl_username,
            &self.sasl_password,
            self.sasl_mechanism,
        ) {
            config = config.with_sasl_auth(
                username.clone(),
                password.clone(),
                mechanism.into(),
                self.security_protocol.into(),
            );
        } else {
            config.security_protocol = Some(self.security_protocol.into());
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_sasl_mechanism_deserialization() {
        let plain = serde_json::from_str::<KafkaSaslMechanism>("\"plain\"").unwrap();
        assert_eq!(plain, KafkaSaslMechanism::Plain);

        let scram_sha_256 = serde_json::from_str::<KafkaSaslMechanism>("\"scram-sha256\"").unwrap();
        assert_eq!(scram_sha_256, KafkaSaslMechanism::ScramSha256);

        let scram_sha_512 = serde_json::from_str::<KafkaSaslMechanism>("\"scram-sha512\"").unwrap();
        assert_eq!(scram_sha_512, KafkaSaslMechanism::ScramSha512);
    }

    #[test]
    fn test_kafka_partitioner_type_deserialization() {
        let consistent = serde_json::from_str::<KafkaPartitionerType>("\"consistent\"").unwrap();
        assert_eq!(consistent, KafkaPartitionerType::Consistent);

        let consistent_random =
            serde_json::from_str::<KafkaPartitionerType>("\"consistent-random\"").unwrap();
        assert_eq!(consistent_random, KafkaPartitionerType::ConsistentRandom);

        let murmur2_random =
            serde_json::from_str::<KafkaPartitionerType>("\"murmur2-random\"").unwrap();
        assert_eq!(murmur2_random, KafkaPartitionerType::Murmur2Random);

        let murmur2 = serde_json::from_str::<KafkaPartitionerType>("\"murmur2\"").unwrap();
        assert_eq!(murmur2, KafkaPartitionerType::Murmur2);

        let fnv1a = serde_json::from_str::<KafkaPartitionerType>("\"fnv1a\"").unwrap();
        assert_eq!(fnv1a, KafkaPartitionerType::Fnv1a);

        let fnv1a_random =
            serde_json::from_str::<KafkaPartitionerType>("\"fnv1a-random\"").unwrap();
        assert_eq!(fnv1a_random, KafkaPartitionerType::Fnv1aRandom);
    }
}
