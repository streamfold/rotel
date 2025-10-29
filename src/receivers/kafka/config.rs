// SPDX-License-Identifier: Apache-2.0

use rdkafka::ClientConfig;
use std::collections::HashMap;

/// Deserialization format for Kafka messages
#[derive(Default, Copy, Clone, PartialEq, Debug, clap::ValueEnum, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeserializationFormat {
    /// JSON format
    Json,
    /// Protobuf format
    #[default]
    Protobuf,
}

/// SASL mechanism configuration (same as exporter)
#[derive(Copy, Clone, PartialEq, Debug, clap::ValueEnum, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

/// Security protocol configuration (same as exporter)
#[derive(Copy, Clone, PartialEq, Debug, clap::ValueEnum, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

/// Auto offset reset behavior
#[derive(Default, Copy, Clone, PartialEq, Debug, clap::ValueEnum, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AutoOffsetReset {
    /// Start from the beginning of the topic
    Earliest,
    /// Start from the end of the topic
    #[default]
    Latest,
    /// Throw error if no offset is found
    Error,
}

/// Consumer isolation level
#[derive(Default, Copy, Clone, PartialEq, Debug, clap::ValueEnum, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IsolationLevel {
    /// Read all messages (including uncommitted)
    ReadUncommitted,
    /// Read only committed messages
    #[default]
    ReadCommitted,
}

impl SecurityProtocol {
    /// Convert to the string value expected by librdkafka
    pub fn to_kafka_value(&self) -> &'static str {
        match self {
            SecurityProtocol::Plaintext => "plaintext",
            SecurityProtocol::SaslPlaintext => "sasl_plaintext",
            SecurityProtocol::Ssl => "ssl",
            SecurityProtocol::SaslSsl => "sasl_ssl",
        }
    }
}

impl SaslMechanism {
    /// Convert to the string value expected by librdkafka
    pub fn to_kafka_value(&self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

impl AutoOffsetReset {
    /// Convert to the string value expected by librdkafka
    pub fn to_kafka_value(&self) -> &'static str {
        match self {
            AutoOffsetReset::Earliest => "earliest",
            AutoOffsetReset::Latest => "latest",
            AutoOffsetReset::Error => "error",
        }
    }
}

impl IsolationLevel {
    /// Convert to the string value expected by librdkafka
    pub fn to_kafka_value(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "read_uncommitted",
            IsolationLevel::ReadCommitted => "read_committed",
        }
    }
}

/// Configuration for the Kafka receiver
#[derive(Clone, Debug)]
pub struct KafkaReceiverConfig {
    /// Kafka broker addresses (comma-separated) - same name as exporter
    pub brokers: String,

    /// Topic names to subscribe to for traces
    pub traces_topic: Option<String>,

    /// Topic names to subscribe to for metrics
    pub metrics_topic: Option<String>,

    /// Topic names to subscribe to for logs
    pub logs_topic: Option<String>,

    /// Enable consuming traces
    pub traces: bool,

    /// Enable consuming metrics
    pub metrics: bool,

    /// Enable consuming logs
    pub logs: bool,

    /// Deserialization format for incoming messages
    pub deserialization_format: DeserializationFormat,

    /// Consumer group ID for coordinated consumption
    pub group_id: String,

    /// Client ID for the Kafka consumer - same name as exporter
    pub client_id: String,

    /// Enable auto commit of offsets
    pub enable_auto_commit: bool,

    /// Auto commit interval in milliseconds
    pub auto_commit_interval_ms: u32,

    /// Manual offset commit interval in seconds (when auto commit is disabled)
    pub manual_commit_interval_secs: u64,

    /// Auto offset reset behavior when no offset is found
    pub auto_offset_reset: AutoOffsetReset,

    /// Session timeout in milliseconds
    pub session_timeout_ms: u32,

    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u32,

    /// Maximum poll interval in milliseconds
    pub max_poll_interval_ms: u32,

    /// Maximum number of bytes per partition the consumer will buffer
    pub max_partition_fetch_bytes: u32,

    /// Minimum number of bytes for fetch requests
    pub fetch_min_bytes: u32,

    /// Maximum wait time for fetch requests in milliseconds
    pub fetch_max_wait_ms: u32,

    /// Socket timeout in milliseconds
    pub socket_timeout_ms: u32,

    /// Maximum number of retries for metadata requests
    pub metadata_max_age_ms: u32,

    /// Consumer isolation level
    pub isolation_level: IsolationLevel,

    /// Enable partition EOF notifications
    pub enable_partition_eof: bool,

    /// Check CRC32 of consumed messages
    pub check_crcs: bool,

    /// SASL username for authentication - same name as exporter
    pub sasl_username: Option<String>,

    /// SASL password for authentication - same name as exporter
    pub sasl_password: Option<String>,

    /// SASL mechanism - same name as exporter
    pub sasl_mechanism: Option<SaslMechanism>,

    /// Security protocol - same name as exporter
    pub security_protocol: Option<SecurityProtocol>,

    /// SSL CA certificate location
    pub ssl_ca_location: Option<String>,

    /// SSL certificate location
    pub ssl_certificate_location: Option<String>,

    /// SSL key location
    pub ssl_key_location: Option<String>,

    /// SSL key password
    pub ssl_key_password: Option<String>,

    /// Consumer configuration options for additional librdkafka settings
    pub consumer_config: HashMap<String, String>,
}

impl Default for KafkaReceiverConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            traces_topic: Some("otlp_traces".to_string()),
            metrics_topic: Some("otlp_metrics".to_string()),
            logs_topic: Some("otlp_logs".to_string()),
            traces: false,
            metrics: false,
            logs: false,
            deserialization_format: DeserializationFormat::default(),
            group_id: "rotel-consumer".to_string(),
            client_id: "rotel".to_string(),
            enable_auto_commit: false, // Disabled by default to use manual offset committing
            auto_commit_interval_ms: 5000,
            manual_commit_interval_secs: 5, // 5 seconds default for manual commits
            auto_offset_reset: AutoOffsetReset::default(),
            session_timeout_ms: 30000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
            max_partition_fetch_bytes: 1048576, // 1MB
            fetch_min_bytes: 1,
            fetch_max_wait_ms: 500,
            socket_timeout_ms: 60000,
            metadata_max_age_ms: 300000,
            isolation_level: IsolationLevel::default(),
            enable_partition_eof: false,
            check_crcs: true,
            sasl_username: None,
            sasl_password: None,
            sasl_mechanism: None,
            security_protocol: None,
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ssl_key_password: None,
            consumer_config: HashMap::new(),
        }
    }
}

impl KafkaReceiverConfig {
    /// Create a new Kafka receiver configuration
    pub fn new(brokers: String, group_id: String) -> Self {
        Self {
            brokers,
            group_id,
            ..Default::default()
        }
    }

    /// Set the traces topics
    pub fn with_traces_topic(mut self, topic: String) -> Self {
        self.traces_topic = Some(topic);
        self
    }

    /// Set the metrics topics
    pub fn with_metrics_topic(mut self, topic: Option<String>) -> Self {
        self.metrics_topic = topic;
        self
    }

    /// Set the logs topics
    pub fn with_logs_topic(mut self, topic: String) -> Self {
        self.logs_topic = Some(topic);
        self
    }

    /// Consume traces
    pub fn with_traces(mut self, traces: bool) -> Self {
        self.traces = traces;
        self
    }

    /// Consume metrics
    pub fn with_metrics(mut self, metrics: bool) -> Self {
        self.metrics = metrics;
        self
    }

    /// Consume logs
    pub fn with_logs(mut self, logs: bool) -> Self {
        self.logs = logs;
        self
    }

    /// Set the deserialization format
    pub fn with_deserialization_format(mut self, format: DeserializationFormat) -> Self {
        self.deserialization_format = format;
        self
    }

    /// Set client ID
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = client_id;
        self
    }

    /// Set auto commit configuration
    pub fn with_auto_commit(mut self, enabled: bool, interval_ms: u32) -> Self {
        self.enable_auto_commit = enabled;
        self.auto_commit_interval_ms = interval_ms;
        self
    }

    /// Set auto offset reset behavior
    pub fn with_auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Set session timeout
    pub fn with_session_timeout_ms(mut self, timeout_ms: u32) -> Self {
        self.session_timeout_ms = timeout_ms;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval_ms(mut self, interval_ms: u32) -> Self {
        self.heartbeat_interval_ms = interval_ms;
        self
    }

    /// Set max poll interval
    pub fn with_max_poll_interval_ms(mut self, interval_ms: u32) -> Self {
        self.max_poll_interval_ms = interval_ms;
        self
    }

    /// Set fetch configuration
    pub fn with_fetch_config(
        mut self,
        min_bytes: u32,
        max_wait_ms: u32,
        max_partition_bytes: u32,
    ) -> Self {
        self.fetch_min_bytes = min_bytes;
        self.fetch_max_wait_ms = max_wait_ms;
        self.max_partition_fetch_bytes = max_partition_bytes;
        self
    }

    /// Set isolation level
    pub fn with_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Set SASL authentication
    pub fn with_sasl_auth(
        mut self,
        username: String,
        password: String,
        mechanism: SaslMechanism,
        security_protocol: SecurityProtocol,
    ) -> Self {
        self.sasl_username = Some(username);
        self.sasl_password = Some(password);
        self.sasl_mechanism = Some(mechanism);
        self.security_protocol = Some(security_protocol);
        self
    }

    /// Set SSL configuration
    pub fn with_ssl_config(
        mut self,
        ca_location: Option<String>,
        cert_location: Option<String>,
        key_location: Option<String>,
        key_password: Option<String>,
    ) -> Self {
        self.ssl_ca_location = ca_location;
        self.ssl_certificate_location = cert_location;
        self.ssl_key_location = key_location;
        self.ssl_key_password = key_password;
        if self.security_protocol.is_none()
            && (self.ssl_ca_location.is_some()
                || self.ssl_certificate_location.is_some()
                || self.ssl_key_location.is_some()
                || self.ssl_key_password.is_some())
        {
            self.security_protocol = Some(SecurityProtocol::Ssl);
        }
        self
    }

    /// Set custom consumer configuration parameters
    pub fn with_custom_config(mut self, custom_config: Vec<(String, String)>) -> Self {
        for (key, value) in custom_config {
            self.consumer_config.insert(key, value);
        }
        self
    }

    /// Get the list of topics to subscribe to based on enabled signal types
    pub fn get_topics(&self) -> Vec<String> {
        let mut topics = Vec::new();

        if self.traces {
            if let Some(ref topic) = self.traces_topic {
                topics.push(topic.clone());
            }
        }

        if self.metrics {
            if let Some(ref topic) = self.metrics_topic {
                topics.push(topic.clone());
            }
        }

        if self.logs {
            if let Some(ref topic) = self.logs_topic {
                topics.push(topic.clone());
            }
        }

        topics
    }

    /// Build rdkafka ClientConfig from this configuration
    pub fn build_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();

        // Basic connection settings
        config.set("bootstrap.servers", &self.brokers);
        config.set("group.id", &self.group_id);
        config.set("client.id", &self.client_id);

        // Auto commit settings
        config.set(
            "enable.auto.commit",
            if self.enable_auto_commit {
                "true"
            } else {
                "false"
            },
        );
        config.set(
            "auto.commit.interval.ms",
            self.auto_commit_interval_ms.to_string(),
        );

        // Offset management
        config.set("auto.offset.reset", self.auto_offset_reset.to_kafka_value());

        // Session and heartbeat settings
        config.set("session.timeout.ms", self.session_timeout_ms.to_string());
        config.set(
            "heartbeat.interval.ms",
            self.heartbeat_interval_ms.to_string(),
        );
        config.set(
            "max.poll.interval.ms",
            self.max_poll_interval_ms.to_string(),
        );

        // Fetch settings
        config.set(
            "max.partition.fetch.bytes",
            self.max_partition_fetch_bytes.to_string(),
        );
        config.set("fetch.min.bytes", self.fetch_min_bytes.to_string());
        config.set("fetch.wait.max.ms", self.fetch_max_wait_ms.to_string());

        // Timeout settings
        config.set("socket.timeout.ms", self.socket_timeout_ms.to_string());
        config.set("metadata.max.age.ms", self.metadata_max_age_ms.to_string());

        // Isolation level
        config.set("isolation.level", self.isolation_level.to_kafka_value());

        // Partition EOF notifications - enable by default for debugging
        config.set(
            "enable.partition.eof",
            if self.enable_partition_eof {
                "true"
            } else {
                "false" // Enable to help debug partition issues
            },
        );

        // CRC checking
        config.set("check.crcs", if self.check_crcs { "true" } else { "false" });

        // Security configuration
        if let Some(ref protocol) = self.security_protocol {
            config.set("security.protocol", protocol.to_kafka_value());
        }

        // SASL configuration
        if let Some(ref mechanism) = self.sasl_mechanism {
            config.set("sasl.mechanism", mechanism.to_kafka_value());
        }

        if let Some(ref username) = self.sasl_username {
            config.set("sasl.username", username);
        }

        if let Some(ref password) = self.sasl_password {
            config.set("sasl.password", password);
        }

        // SSL configuration
        if let Some(ref ca_location) = self.ssl_ca_location {
            config.set("ssl.ca.location", ca_location);
        }

        if let Some(ref cert_location) = self.ssl_certificate_location {
            config.set("ssl.certificate.location", cert_location);
        }

        if let Some(ref key_location) = self.ssl_key_location {
            config.set("ssl.key.location", key_location);
        }

        if let Some(ref key_password) = self.ssl_key_password {
            config.set("ssl.key.password", key_password);
        }

        // Set custom consumer configuration (overrides built-in options if conflicts exist)
        for (key, value) in &self.consumer_config {
            config.set(key, value);
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KafkaReceiverConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.group_id, "rotel-consumer");
        assert_eq!(config.client_id, "rotel");
        assert_eq!(config.traces_topic, Some("otlp_traces".to_string()));
        assert_eq!(config.metrics_topic, Some("otlp_metrics".to_string()));
        assert_eq!(config.logs_topic, Some("otlp_logs".to_string()));
        assert!(!config.traces);
        assert!(!config.metrics);
        assert!(!config.logs);
        assert!(!config.enable_auto_commit);
        assert_eq!(config.auto_commit_interval_ms, 5000);
    }

    #[test]
    fn test_config_builder() {
        let config = KafkaReceiverConfig::new("kafka:9092".to_string(), "my-group".to_string())
            .with_traces(true)
            .with_metrics(true)
            .with_logs(true)
            .with_traces_topic("my-traces".to_string())
            .with_metrics_topic(Some("my-metrics".to_string()))
            .with_logs_topic("my-logs".to_string())
            .with_client_id("my-client".to_string())
            .with_auto_commit(false, 10000)
            .with_auto_offset_reset(AutoOffsetReset::Earliest)
            .with_session_timeout_ms(60000)
            .with_heartbeat_interval_ms(5000)
            .with_max_poll_interval_ms(600000)
            .with_fetch_config(100, 1000, 2097152)
            .with_isolation_level(IsolationLevel::ReadUncommitted)
            .with_deserialization_format(DeserializationFormat::Json);

        assert_eq!(config.brokers, "kafka:9092");
        assert_eq!(config.group_id, "my-group");
        assert_eq!(config.client_id, "my-client");
        assert!(config.traces);
        assert!(config.metrics);
        assert!(config.logs);
        assert_eq!(config.traces_topic, Some("my-traces".to_string()));
        assert_eq!(config.metrics_topic, Some("my-metrics".to_string()));
        assert_eq!(config.logs_topic, Some("my-logs".to_string()));
        assert!(!config.enable_auto_commit);
        assert_eq!(config.auto_commit_interval_ms, 10000);
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert_eq!(config.session_timeout_ms, 60000);
        assert_eq!(config.heartbeat_interval_ms, 5000);
        assert_eq!(config.max_poll_interval_ms, 600000);
        assert_eq!(config.fetch_min_bytes, 100);
        assert_eq!(config.fetch_max_wait_ms, 1000);
        assert_eq!(config.max_partition_fetch_bytes, 2097152);
        assert_eq!(config.isolation_level, IsolationLevel::ReadUncommitted);
        assert_eq!(config.deserialization_format, DeserializationFormat::Json);
    }

    #[test]
    fn test_get_topics_none_enabled() {
        let config = KafkaReceiverConfig::default();
        assert!(config.get_topics().is_empty());
    }

    #[test]
    fn test_get_topics_traces_enabled() {
        let config = KafkaReceiverConfig::default().with_traces(true);
        let topics = config.get_topics();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0], "otlp_traces");
    }

    #[test]
    fn test_get_topics_all_enabled() {
        let config = KafkaReceiverConfig::default()
            .with_traces(true)
            .with_metrics(true)
            .with_logs(true);
        let topics = config.get_topics();
        assert_eq!(topics.len(), 3);
        assert!(topics.contains(&"otlp_traces".to_string()));
        assert!(topics.contains(&"otlp_metrics".to_string()));
        assert!(topics.contains(&"otlp_logs".to_string()));
    }

    #[test]
    fn test_get_topics_enabled_but_no_topic_name() {
        let mut config = KafkaReceiverConfig::default().with_traces(true);
        config.traces_topic = None;
        let topics = config.get_topics();
        assert!(topics.is_empty());
    }

    #[test]
    fn test_sasl_auth_config() {
        let config = KafkaReceiverConfig::default().with_sasl_auth(
            "user".to_string(),
            "pass".to_string(),
            SaslMechanism::ScramSha256,
            SecurityProtocol::SaslSsl,
        );

        assert_eq!(config.sasl_username, Some("user".to_string()));
        assert_eq!(config.sasl_password, Some("pass".to_string()));
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::ScramSha256));
        assert_eq!(config.security_protocol, Some(SecurityProtocol::SaslSsl));
    }

    #[test]
    fn test_ssl_config() {
        let config = KafkaReceiverConfig::default().with_ssl_config(
            Some("/path/to/ca.pem".to_string()),
            Some("/path/to/cert.pem".to_string()),
            Some("/path/to/key.pem".to_string()),
            Some("keypass".to_string()),
        );

        assert_eq!(config.ssl_ca_location, Some("/path/to/ca.pem".to_string()));
        assert_eq!(
            config.ssl_certificate_location,
            Some("/path/to/cert.pem".to_string())
        );
        assert_eq!(
            config.ssl_key_location,
            Some("/path/to/key.pem".to_string())
        );
        assert_eq!(config.ssl_key_password, Some("keypass".to_string()));
        assert_eq!(config.security_protocol, Some(SecurityProtocol::Ssl));
    }

    #[test]
    fn test_ssl_config_preserves_security_protocol_if_set() {
        let config = KafkaReceiverConfig::default()
            .with_sasl_auth(
                "user".to_string(),
                "pass".to_string(),
                SaslMechanism::Plain,
                SecurityProtocol::SaslSsl,
            )
            .with_ssl_config(Some("/path/to/ca.pem".to_string()), None, None, None);

        assert_eq!(config.security_protocol, Some(SecurityProtocol::SaslSsl));
    }

    #[test]
    fn test_custom_config() {
        let custom = vec![
            ("socket.keepalive.enable".to_string(), "true".to_string()),
            ("connections.max.idle.ms".to_string(), "540000".to_string()),
        ];
        let config = KafkaReceiverConfig::default().with_custom_config(custom);

        assert_eq!(
            config.consumer_config.get("socket.keepalive.enable"),
            Some(&"true".to_string())
        );
        assert_eq!(
            config.consumer_config.get("connections.max.idle.ms"),
            Some(&"540000".to_string())
        );
    }

    #[test]
    fn test_build_client_config_basic() {
        let config =
            KafkaReceiverConfig::new("broker1:9092,broker2:9092".to_string(), "test".to_string());
        let client_config = config.build_client_config();

        let config_map = client_config.create_native_config().unwrap();
        assert_eq!(
            config_map.get("bootstrap.servers").unwrap(),
            "broker1:9092,broker2:9092"
        );
        assert_eq!(config_map.get("group.id").unwrap(), "test");
        assert_eq!(config_map.get("client.id").unwrap(), "rotel");
    }

    #[test]
    fn test_build_client_config_with_sasl() {
        let config = KafkaReceiverConfig::default().with_sasl_auth(
            "myuser".to_string(),
            "mypass".to_string(),
            SaslMechanism::ScramSha512,
            SecurityProtocol::SaslPlaintext,
        );
        let client_config = config.build_client_config();

        let config_map = client_config.create_native_config().unwrap();
        assert_eq!(
            config_map.get("security.protocol").unwrap(),
            "sasl_plaintext"
        );
        assert_eq!(config_map.get("sasl.mechanism").unwrap(), "SCRAM-SHA-512");
        assert_eq!(config_map.get("sasl.username").unwrap(), "myuser");
        assert_eq!(config_map.get("sasl.password").unwrap(), "mypass");
    }

    #[test]
    fn test_build_client_config_with_custom_overrides() {
        let custom = vec![("enable.auto.commit".to_string(), "false".to_string())];
        let config = KafkaReceiverConfig::default()
            .with_auto_commit(true, 5000)
            .with_custom_config(custom);
        let client_config = config.build_client_config();

        let config_map = client_config.create_native_config().unwrap();
        assert_eq!(config_map.get("enable.auto.commit").unwrap(), "false");
    }

    #[test]
    fn test_security_protocol_to_kafka_value() {
        assert_eq!(SecurityProtocol::Plaintext.to_kafka_value(), "plaintext");
        assert_eq!(SecurityProtocol::Ssl.to_kafka_value(), "ssl");
        assert_eq!(
            SecurityProtocol::SaslPlaintext.to_kafka_value(),
            "sasl_plaintext"
        );
        assert_eq!(SecurityProtocol::SaslSsl.to_kafka_value(), "sasl_ssl");
    }

    #[test]
    fn test_sasl_mechanism_to_kafka_value() {
        assert_eq!(SaslMechanism::Plain.to_kafka_value(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.to_kafka_value(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.to_kafka_value(), "SCRAM-SHA-512");
    }

    #[test]
    fn test_auto_offset_reset_to_kafka_value() {
        assert_eq!(AutoOffsetReset::Earliest.to_kafka_value(), "earliest");
        assert_eq!(AutoOffsetReset::Latest.to_kafka_value(), "latest");
        assert_eq!(AutoOffsetReset::Error.to_kafka_value(), "error");
    }

    #[test]
    fn test_isolation_level_to_kafka_value() {
        assert_eq!(
            IsolationLevel::ReadUncommitted.to_kafka_value(),
            "read_uncommitted"
        );
        assert_eq!(
            IsolationLevel::ReadCommitted.to_kafka_value(),
            "read_committed"
        );
    }

    #[test]
    fn test_deserialization_format_default() {
        assert_eq!(
            DeserializationFormat::default(),
            DeserializationFormat::Protobuf
        );
    }
}
