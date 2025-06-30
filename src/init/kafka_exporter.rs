// SPDX-License-Identifier: Apache-2.0

use crate::exporters::kafka::config::{
    AcknowledgementMode, KafkaExporterConfig, SerializationFormat,
};
use crate::init::args;
use clap::{Args, ValueEnum};

#[derive(Debug, Args, Clone)]
pub struct KafkaExporterArgs {
    /// Kafka broker addresses (comma-separated)
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_BROKERS",
        default_value = "localhost:9092"
    )]
    pub kafka_exporter_brokers: String,

    /// Topic name for traces
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_TRACES_TOPIC",
        default_value = "otlp_traces"
    )]
    pub kafka_exporter_traces_topic: String,

    /// Topic name for metrics
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_METRICS_TOPIC",
        default_value = "otlp_metrics"
    )]
    pub kafka_exporter_metrics_topic: String,

    /// Topic name for logs
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_LOGS_TOPIC",
        default_value = "otlp_logs"
    )]
    pub kafka_exporter_logs_topic: String,

    /// Serialization format
    #[arg(
        value_enum,
        long,
        env = "ROTEL_KAFKA_EXPORTER_FORMAT",
        default_value = "json"
    )]
    pub kafka_exporter_format: KafkaSerializationFormat,

    /// Compression type (gzip, snappy, lz4, zstd, none)
    #[arg(long, env = "ROTEL_KAFKA_EXPORTER_COMPRESSION")]
    pub kafka_exporter_compression: Option<String>,

    /// Request timeout
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_REQUEST_TIMEOUT",
        default_value = "30s"
    )]
    pub kafka_exporter_request_timeout: humantime::Duration,

    /// Acknowledgement mode (none, one, all)
    #[arg(
        value_enum,
        long,
        env = "ROTEL_KAFKA_EXPORTER_ACKS",
        default_value = "one"
    )]
    pub kafka_exporter_acks: KafkaAcknowledgementMode,

    /// Client ID for the Kafka producer
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_CLIENT_ID",
        default_value = "rotel"
    )]
    pub kafka_exporter_client_id: String,

    /// Maximum message size in bytes
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_MAX_MESSAGE_BYTES",
        default_value = "1000000"
    )]
    pub kafka_exporter_max_message_bytes: usize,

    /// Linger time in milliseconds
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_LINGER_MS",
        default_value = "5"
    )]
    pub kafka_exporter_linger_ms: u32,

    /// Number of retries for message sending
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_RETRIES",
        default_value = "2147483647"
    )]
    pub kafka_exporter_retries: u32,

    /// Retry backoff time in milliseconds
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_RETRY_BACKOFF_MS",
        default_value = "100"
    )]
    pub kafka_exporter_retry_backoff_ms: u32,

    /// Maximum retry backoff time in milliseconds
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_RETRY_BACKOFF_MAX_MS",
        default_value = "1000"
    )]
    pub kafka_exporter_retry_backoff_max_ms: u32,

    /// Message timeout in milliseconds
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_MESSAGE_TIMEOUT_MS",
        default_value = "300000"
    )]
    pub kafka_exporter_message_timeout_ms: u32,

    /// Request timeout in milliseconds
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_REQUEST_TIMEOUT_MS",
        default_value = "30000"
    )]
    pub kafka_exporter_request_timeout_ms: u32,

    /// Batch size in bytes
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_BATCH_SIZE",
        default_value = "1000000"
    )]
    pub kafka_exporter_batch_size: u32,

    /// Custom Kafka producer configuration parameters (key=value pairs). These will override built-in options if conflicts exist.
    #[arg(
        long("kafka-exporter-custom-config"),
        env = "ROTEL_KAFKA_EXPORTER_CUSTOM_CONFIG",
        value_parser = args::parse_key_val::<String, String>,
        value_delimiter = ','
    )]
    pub kafka_exporter_custom_config: Vec<(String, String)>,

    /// SASL username for authentication
    #[arg(long, env = "ROTEL_KAFKA_EXPORTER_SASL_USERNAME")]
    pub kafka_exporter_sasl_username: Option<String>,

    /// SASL password for authentication
    #[arg(long, env = "ROTEL_KAFKA_EXPORTER_SASL_PASSWORD")]
    pub kafka_exporter_sasl_password: Option<String>,

    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    #[arg(long, env = "ROTEL_KAFKA_EXPORTER_SASL_MECHANISM")]
    pub kafka_exporter_sasl_mechanism: Option<String>,

    /// Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
    #[arg(
        long,
        env = "ROTEL_KAFKA_EXPORTER_SECURITY_PROTOCOL",
        default_value = "PLAINTEXT"
    )]
    pub kafka_exporter_security_protocol: String,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum KafkaSerializationFormat {
    Json,
    Protobuf,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum KafkaAcknowledgementMode {
    /// No acknowledgement required (acks=0) - fastest but least durable
    None,
    /// Wait for leader acknowledgement only (acks=1) - balanced
    One,
    /// Wait for all in-sync replicas (acks=all) - slowest but most durable
    All,
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

impl KafkaExporterArgs {
    pub fn build_config(&self) -> KafkaExporterConfig {
        let mut config = KafkaExporterConfig::new(self.kafka_exporter_brokers.clone())
            .with_traces_topic(self.kafka_exporter_traces_topic.clone())
            .with_metrics_topic(self.kafka_exporter_metrics_topic.clone())
            .with_logs_topic(self.kafka_exporter_logs_topic.clone())
            .with_serialization_format(self.kafka_exporter_format.into())
            .with_acks(self.kafka_exporter_acks.into())
            .with_client_id(self.kafka_exporter_client_id.clone())
            .with_max_message_bytes(self.kafka_exporter_max_message_bytes)
            .with_linger_ms(self.kafka_exporter_linger_ms)
            .with_retries(self.kafka_exporter_retries)
            .with_retry_backoff_ms(self.kafka_exporter_retry_backoff_ms)
            .with_retry_backoff_max_ms(self.kafka_exporter_retry_backoff_max_ms)
            .with_message_timeout_ms(self.kafka_exporter_message_timeout_ms)
            .with_request_timeout_ms(self.kafka_exporter_request_timeout_ms)
            .with_batch_size(self.kafka_exporter_batch_size)
            .with_custom_config(self.kafka_exporter_custom_config.clone());

        config.request_timeout = self.kafka_exporter_request_timeout.into();

        if let Some(ref compression) = self.kafka_exporter_compression {
            config = config.with_compression(compression.clone());
        }

        // Configure SASL if credentials are provided
        if let (Some(username), Some(password), Some(mechanism)) = (
            &self.kafka_exporter_sasl_username,
            &self.kafka_exporter_sasl_password,
            &self.kafka_exporter_sasl_mechanism,
        ) {
            config = config.with_sasl_auth(
                username.clone(),
                password.clone(),
                mechanism.clone(),
                self.kafka_exporter_security_protocol.clone(),
            );
        } else {
            config.security_protocol = Some(self.kafka_exporter_security_protocol.clone());
        }

        config
    }
}
