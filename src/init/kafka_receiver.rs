// SPDX-License-Identifier: Apache-2.0

use crate::init::parse::parse_key_val;
use crate::receivers::kafka::config::KafkaReceiverConfig;
use clap::Args;
use serde::Deserialize;

#[derive(Default, Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaReceiverArgs {
    /// Kafka broker addresses (comma-separated)
    #[arg(
        id("KAFKA_RECEIVER_BROKERS"),
        long("kafka-receiver-brokers"),
        env = "ROTEL_KAFKA_RECEIVER_BROKERS",
        default_value = "localhost:9092"
    )]
    pub brokers: String,

    /// Topic name for traces
    #[arg(
        id("KAFKA_RECEIVER_TRACES_TOPIC"),
        long("kafka-receiver-traces-topic"),
        env = "ROTEL_KAFKA_RECEIVER_TRACES_TOPIC",
        default_value = "otlp_traces"
    )]
    pub traces_topic: Option<String>,

    /// Topic name for metrics
    #[arg(
        id("KAFKA_RECEIVER_METRICS_TOPIC"),
        long("kafka-receiver-metrics-topic"),
        env = "ROTEL_KAFKA_RECEIVER_METRICS_TOPIC",
        default_value = "otlp_metrics"
    )]
    pub metrics_topic: Option<String>,

    /// Topic name for logs
    #[arg(
        id("KAFKA_RECEIVER_LOGS_TOPIC"),
        long("kafka-receiver-logs-topic"),
        env = "ROTEL_KAFKA_RECEIVER_LOGS_TOPIC",
        default_value = "otlp_logs"
    )]
    pub logs_topic: Option<String>,

    /// Boolean flags to control what to consume
    #[arg(
        long("kafka-receiver-traces"),
        env = "ROTEL_KAFKA_RECEIVER_TRACES",
        default_value = "false"
    )]
    pub traces: bool,

    #[arg(
        long("kafka-receiver-metrics"),
        env = "ROTEL_KAFKA_RECEIVER_METRICS",
        default_value = "false"
    )]
    pub metrics: bool,

    #[arg(
        long("kafka-receiver-logs"),
        env = "ROTEL_KAFKA_RECEIVER_LOGS",
        default_value = "false"
    )]
    pub logs: bool,

    /// Deserialization format
    #[arg(
        id("KAFKA_RECEIVER_FORMAT"),
        value_enum,
        long("kafka-receiver-format"),
        env = "ROTEL_KAFKA_RECEIVER_FORMAT",
        default_value = "protobuf"
    )]
    pub format: crate::receivers::kafka::config::DeserializationFormat,

    /// Consumer group ID for coordinated consumption
    #[arg(
        long("kafka-receiver-group-id"),
        env = "ROTEL_KAFKA_RECEIVER_GROUP_ID",
        default_value = "rotel-consumer"
    )]
    pub group_id: String,

    /// Client ID for the Kafka consumer
    #[arg(
        id("KAFKA_RECEIVER_CLIENT_ID"),
        long("kafka-receiver-client-id"),
        env = "ROTEL_KAFKA_RECEIVER_CLIENT_ID",
        default_value = "rotel"
    )]
    pub client_id: String,

    /// Enable auto commit of offsets
    #[arg(
        long("kafka-receiver-enable-auto-commit"),
        env = "ROTEL_KAFKA_RECEIVER_ENABLE_AUTO_COMMIT",
        default_value = "true"
    )]
    pub enable_auto_commit: bool,

    /// Auto commit interval in milliseconds
    #[arg(
        long("kafka-receiver-auto-commit-interval-ms"),
        env = "ROTEL_KAFKA_RECEIVER_AUTO_COMMIT_INTERVAL_MS",
        default_value = "5000"
    )]
    pub auto_commit_interval_ms: u32,

    /// Auto offset reset behavior when no offset is found
    #[arg(
        value_enum,
        long("kafka-receiver-auto-offset-reset"),
        env = "ROTEL_KAFKA_RECEIVER_AUTO_OFFSET_RESET",
        default_value = "latest"
    )]
    pub auto_offset_reset: crate::receivers::kafka::config::AutoOffsetReset,

    /// Session timeout in milliseconds
    #[arg(
        long("kafka-receiver-session-timeout-ms"),
        env = "ROTEL_KAFKA_RECEIVER_SESSION_TIMEOUT_MS",
        default_value = "30000"
    )]
    pub session_timeout_ms: u32,

    /// Heartbeat interval in milliseconds
    #[arg(
        long("kafka-receiver-heartbeat-interval-ms"),
        env = "ROTEL_KAFKA_RECEIVER_HEARTBEAT_INTERVAL_MS",
        default_value = "3000"
    )]
    pub heartbeat_interval_ms: u32,

    /// Maximum poll interval in milliseconds
    #[arg(
        long("kafka-receiver-max-poll-interval-ms"),
        env = "ROTEL_KAFKA_RECEIVER_MAX_POLL_INTERVAL_MS",
        default_value = "300000"
    )]
    pub max_poll_interval_ms: u32,

    /// Maximum number of bytes per partition the consumer will buffer
    #[arg(
        long("kafka-receiver-max-partition-fetch-bytes"),
        env = "ROTEL_KAFKA_RECEIVER_MAX_PARTITION_FETCH_BYTES",
        default_value = "1048576"
    )]
    pub max_partition_fetch_bytes: u32,

    /// Minimum number of bytes for fetch requests
    #[arg(
        long("kafka-receiver-fetch-min-bytes"),
        env = "ROTEL_KAFKA_RECEIVER_FETCH_MIN_BYTES",
        default_value = "1"
    )]
    pub fetch_min_bytes: u32,

    /// Maximum wait time for fetch requests in milliseconds
    #[arg(
        long("kafka-receiver-fetch-max-wait-ms"),
        env = "ROTEL_KAFKA_RECEIVER_FETCH_MAX_WAIT_MS",
        default_value = "500"
    )]
    pub fetch_max_wait_ms: u32,

    /// Socket timeout in milliseconds
    #[arg(
        long("kafka-receiver-socket-timeout-ms"),
        env = "ROTEL_KAFKA_RECEIVER_SOCKET_TIMEOUT_MS",
        default_value = "60000"
    )]
    pub socket_timeout_ms: u32,

    /// Maximum age of metadata in milliseconds
    #[arg(
        long("kafka-receiver-metadata-max-age-ms"),
        env = "ROTEL_KAFKA_RECEIVER_METADATA_MAX_AGE_MS",
        default_value = "300000"
    )]
    pub metadata_max_age_ms: u32,

    /// Consumer isolation level
    #[arg(
        value_enum,
        long("kafka-receiver-isolation-level"),
        env = "ROTEL_KAFKA_RECEIVER_ISOLATION_LEVEL",
        default_value = "read-committed"
    )]
    pub isolation_level: crate::receivers::kafka::config::IsolationLevel,

    /// Enable partition EOF notifications
    #[arg(
        long("kafka-receiver-enable-partition-eof"),
        env = "ROTEL_KAFKA_RECEIVER_ENABLE_PARTITION_EOF",
        default_value = "false"
    )]
    pub enable_partition_eof: bool,

    /// Check CRC32 of consumed messages
    #[arg(
        long("kafka-receiver-check-crcs"),
        env = "ROTEL_KAFKA_RECEIVER_CHECK_CRCS",
        default_value = "true"
    )]
    pub check_crcs: bool,

    /// SASL username for authentication
    #[arg(
        id("KAFKA_RECEIVER_SASL_USERNAME"),
        long("kafka-receiver-sasl-username"),
        env = "ROTEL_KAFKA_RECEIVER_SASL_USERNAME",
        default_value = None,
    )]
    pub sasl_username: Option<String>,

    /// SASL password for authentication
    #[arg(
        id("KAFKA_RECEIVER_SASL_PASSWORD"),
        long("kafka-receiver-sasl-password"),
        env = "ROTEL_KAFKA_RECEIVER_SASL_PASSWORD",
        default_value = None,
    )]
    pub sasl_password: Option<String>,

    /// SASL mechanism
    #[arg(
        id("KAFKA_RECEIVER_SASL_MECHANISM"),
        long("kafka-receiver-sasl-mechanism"),
        env = "ROTEL_KAFKA_RECEIVER_SASL_MECHANISM",
        default_value = None,
    )]
    pub sasl_mechanism: Option<crate::receivers::kafka::config::SaslMechanism>,

    /// Security protocol
    #[arg(
        id("KAFKA_RECEIVER_SECURITY_PROTOCOL"),
        long("kafka-receiver-security-protocol"),
        env = "ROTEL_KAFKA_RECEIVER_SECURITY_PROTOCOL", 
        default_value = None,
    )]
    pub security_protocol: Option<crate::receivers::kafka::config::SecurityProtocol>,

    /// SSL CA certificate location
    #[arg(
        long("kafka-receiver-ssl-ca-location"),
        env = "ROTEL_KAFKA_RECEIVER_SSL_CA_LOCATION",
        default_value = None,
    )]
    pub ssl_ca_location: Option<String>,

    /// SSL certificate location
    #[arg(
        long("kafka-receiver-ssl-certificate-location"),
        env = "ROTEL_KAFKA_RECEIVER_SSL_CERTIFICATE_LOCATION",
        default_value = None,
    )]
    pub ssl_certificate_location: Option<String>,

    /// SSL key location
    #[arg(
        long("kafka-receiver-ssl-key-location"),
        env = "ROTEL_KAFKA_RECEIVER_SSL_KEY_LOCATION",
        default_value = None,
    )]
    pub ssl_key_location: Option<String>,

    /// SSL key password
    #[arg(
        long("kafka-receiver-ssl-key-password"),
        env = "ROTEL_KAFKA_RECEIVER_SSL_KEY_PASSWORD",
        default_value = None,
    )]
    pub ssl_key_password: Option<String>,

    /// Custom Kafka consumer configuration parameters (key=value pairs). These will override built-in options if conflicts exist.
    #[arg(
        id("KAFKA_RECEIVER_CUSTOM_CONFIG"),
        long("kafka-receiver-custom-config"),
        env = "ROTEL_KAFKA_RECEIVER_CUSTOM_CONFIG",
        value_parser = parse_key_val::<String, String>,
        value_delimiter = ','
    )]
    #[serde(deserialize_with = "crate::init::parse::deserialize_key_value_pairs")]
    pub custom_config: Vec<(String, String)>,
}

impl KafkaReceiverArgs {
    pub fn build_config(&self) -> KafkaReceiverConfig {
        let mut config = KafkaReceiverConfig::new(self.brokers.clone(), self.group_id.clone())
            .with_traces(self.traces)
            .with_metrics(self.metrics)
            .with_logs(self.logs)
            .with_deserialization_format(self.format)
            .with_client_id(self.client_id.clone())
            .with_auto_commit(self.enable_auto_commit, self.auto_commit_interval_ms)
            .with_auto_offset_reset(self.auto_offset_reset)
            .with_session_timeout_ms(self.session_timeout_ms)
            .with_heartbeat_interval_ms(self.heartbeat_interval_ms)
            .with_max_poll_interval_ms(self.max_poll_interval_ms)
            .with_fetch_config(
                self.fetch_min_bytes,
                self.fetch_max_wait_ms,
                self.max_partition_fetch_bytes,
            )
            .with_isolation_level(self.isolation_level)
            .with_custom_config(self.custom_config.clone());

        // Set topics if provided
        if let Some(ref topic) = self.traces_topic {
            config = config.with_traces_topic(topic.clone());
        }
        if let Some(ref topic) = self.metrics_topic {
            config = config.with_metrics_topic(Some(topic.clone()));
        }
        if let Some(ref topic) = self.logs_topic {
            config = config.with_logs_topic(topic.clone());
        }

        // Set timeout configurations
        config.socket_timeout_ms = self.socket_timeout_ms;
        config.metadata_max_age_ms = self.metadata_max_age_ms;

        // Set other boolean flags
        config.enable_partition_eof = self.enable_partition_eof;
        config.check_crcs = self.check_crcs;

        // Configure SASL if credentials are provided
        if let (Some(username), Some(password), Some(mechanism), Some(protocol)) = (
            &self.sasl_username,
            &self.sasl_password,
            self.sasl_mechanism,
            self.security_protocol,
        ) {
            config = config.with_sasl_auth(username.clone(), password.clone(), mechanism, protocol);
        } else if let Some(protocol) = self.security_protocol {
            config.security_protocol = Some(protocol);
        }

        // Configure SSL if provided
        config = config.with_ssl_config(
            self.ssl_ca_location.clone(),
            self.ssl_certificate_location.clone(),
            self.ssl_key_location.clone(),
            self.ssl_key_password.clone(),
        );

        config
    }
}

#[cfg(test)]
mod tests {
    use crate::receivers::kafka::config::{AutoOffsetReset, IsolationLevel, SaslMechanism};

    #[test]
    fn test_kafka_sasl_mechanism_deserialization() {
        let plain = serde_json::from_str::<SaslMechanism>("\"plain\"").unwrap();
        assert_eq!(plain, SaslMechanism::Plain);

        let scram_sha_256 = serde_json::from_str::<SaslMechanism>("\"scram-sha256\"").unwrap();
        assert_eq!(scram_sha_256, SaslMechanism::ScramSha256);

        let scram_sha_512 = serde_json::from_str::<SaslMechanism>("\"scram-sha512\"").unwrap();
        assert_eq!(scram_sha_512, SaslMechanism::ScramSha512);
    }

    #[test]
    fn test_kafka_auto_offset_reset_deserialization() {
        let earliest = serde_json::from_str::<AutoOffsetReset>("\"earliest\"").unwrap();
        assert_eq!(earliest, AutoOffsetReset::Earliest);

        let latest = serde_json::from_str::<AutoOffsetReset>("\"latest\"").unwrap();
        assert_eq!(latest, AutoOffsetReset::Latest);

        let error = serde_json::from_str::<AutoOffsetReset>("\"error\"").unwrap();
        assert_eq!(error, AutoOffsetReset::Error);
    }

    #[test]
    fn test_kafka_isolation_level_deserialization() {
        let read_uncommitted =
            serde_json::from_str::<IsolationLevel>("\"read-uncommitted\"").unwrap();
        assert_eq!(read_uncommitted, IsolationLevel::ReadUncommitted);

        let read_committed = serde_json::from_str::<IsolationLevel>("\"read-committed\"").unwrap();
        assert_eq!(read_committed, IsolationLevel::ReadCommitted);
    }
}
