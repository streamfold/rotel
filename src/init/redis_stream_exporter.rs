// SPDX-License-Identifier: Apache-2.0

use crate::exporters::redis_stream::config::{
    RedisStreamExporterConfig, SerializationFormat,
};
use crate::exporters::redis_stream::stream_key::StreamKeyTemplate;
use clap::{Args, ValueEnum};
use serde::Deserialize;
use tower::BoxError;

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct RedisStreamExporterArgs {
    /// Redis endpoint URL
    #[arg(
        id("REDIS_STREAM_EXPORTER_ENDPOINT"),
        long("redis-stream-exporter-endpoint"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_ENDPOINT",
        default_value = "redis://localhost:6379"
    )]
    pub endpoint: String,

    /// Stream key template (e.g. "traces:{service.name}")
    #[arg(
        id("REDIS_STREAM_EXPORTER_STREAM_KEY_TEMPLATE"),
        long("redis-stream-exporter-stream-key-template"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_STREAM_KEY_TEMPLATE",
        default_value = "rotel:traces"
    )]
    pub stream_key_template: String,

    /// Serialization format
    #[arg(
        id("REDIS_STREAM_EXPORTER_FORMAT"),
        value_enum,
        long("redis-stream-exporter-format"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_FORMAT",
        default_value = "json"
    )]
    pub format: RedisStreamSerializationFormat,

    /// Maximum stream length (approximate trimming via MAXLEN ~N)
    #[arg(
        id("REDIS_STREAM_EXPORTER_MAXLEN"),
        long("redis-stream-exporter-maxlen"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_MAXLEN"
    )]
    pub maxlen: Option<usize>,

    /// Enable Redis Cluster mode
    #[arg(
        id("REDIS_STREAM_EXPORTER_CLUSTER_MODE"),
        long("redis-stream-exporter-cluster-mode"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_CLUSTER_MODE",
        default_value = "false"
    )]
    pub cluster_mode: bool,

    /// Path to CA certificate for TLS
    #[arg(
        id("REDIS_STREAM_EXPORTER_CA_CERT_PATH"),
        long("redis-stream-exporter-ca-cert-path"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_CA_CERT_PATH"
    )]
    pub ca_cert_path: Option<String>,

    /// Redis username for authentication
    #[arg(
        id("REDIS_STREAM_EXPORTER_USERNAME"),
        long("redis-stream-exporter-username"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_USERNAME"
    )]
    pub username: Option<String>,

    /// Redis password for authentication
    #[arg(
        id("REDIS_STREAM_EXPORTER_PASSWORD"),
        long("redis-stream-exporter-password"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_PASSWORD"
    )]
    pub password: Option<String>,

    /// Maximum number of XADD commands per pipeline batch
    #[arg(
        id("REDIS_STREAM_EXPORTER_PIPELINE_SIZE"),
        long("redis-stream-exporter-pipeline-size"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_PIPELINE_SIZE"
    )]
    pub pipeline_size: Option<usize>,

    /// Only export spans from these service names (comma-separated). Empty = all services.
    #[arg(
        id("REDIS_STREAM_EXPORTER_FILTER_SERVICE_NAMES"),
        long("redis-stream-exporter-filter-service-names"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_FILTER_SERVICE_NAMES",
        value_delimiter = ','
    )]
    pub filter_service_names: Vec<String>,

    /// TTL in seconds for stream keys. Redis will automatically expire keys after this duration.
    #[arg(
        id("REDIS_STREAM_EXPORTER_KEY_TTL_SECONDS"),
        long("redis-stream-exporter-key-ttl-seconds"),
        env = "ROTEL_REDIS_STREAM_EXPORTER_KEY_TTL_SECONDS"
    )]
    pub key_ttl_seconds: Option<u64>,
}

impl Default for RedisStreamExporterArgs {
    fn default() -> Self {
        RedisStreamExporterArgs {
            endpoint: "redis://localhost:6379".to_string(),
            stream_key_template: "rotel:traces".to_string(),
            format: Default::default(),
            maxlen: None,
            cluster_mode: false,
            ca_cert_path: None,
            username: None,
            password: None,
            pipeline_size: None,
            filter_service_names: Vec::new(),
            key_ttl_seconds: None,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, ValueEnum, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisStreamSerializationFormat {
    Json,
    Flat,
}

impl Default for RedisStreamSerializationFormat {
    fn default() -> Self {
        RedisStreamSerializationFormat::Json
    }
}

impl From<RedisStreamSerializationFormat> for SerializationFormat {
    fn from(value: RedisStreamSerializationFormat) -> Self {
        match value {
            RedisStreamSerializationFormat::Json => SerializationFormat::Json,
            RedisStreamSerializationFormat::Flat => SerializationFormat::Flat,
        }
    }
}

impl RedisStreamExporterArgs {
    pub fn build_config(&self) -> Result<RedisStreamExporterConfig, BoxError> {
        if let Some(0) = self.key_ttl_seconds {
            return Err("redis-stream-exporter-key-ttl-seconds must be greater than 0".into());
        }
        if let Some(0) = self.pipeline_size {
            return Err("redis-stream-exporter-pipeline-size must be greater than 0".into());
        }

        // Strip empty strings from service name filter
        let filter_service_names: Vec<String> = self
            .filter_service_names
            .iter()
            .filter(|s| !s.trim().is_empty())
            .cloned()
            .collect();

        let config = RedisStreamExporterConfig::new(self.endpoint.clone())
            .with_stream_key_template(StreamKeyTemplate::parse(&self.stream_key_template))
            .with_format(self.format.into())
            .with_maxlen(self.maxlen)
            .with_cluster_mode(self.cluster_mode)
            .with_ca_cert_path(self.ca_cert_path.clone())
            .with_username(self.username.clone())
            .with_password(self.password.clone())
            .with_pipeline_size(self.pipeline_size)
            .with_filter_service_names(filter_service_names)
            .with_key_ttl_seconds(self.key_ttl_seconds);

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_config_defaults() {
        let args = RedisStreamExporterArgs::default();
        let config = args.build_config().unwrap();
        assert_eq!(config.endpoint, "redis://localhost:6379");
        assert_eq!(config.format, SerializationFormat::Json);
        assert!(!config.cluster_mode);
        assert!(config.maxlen.is_none());
        assert!(config.key_ttl_seconds.is_none());
        assert!(config.pipeline_size.is_none());
        assert!(config.filter_service_names.is_empty());
    }

    #[test]
    fn test_build_config_rejects_zero_ttl() {
        let args = RedisStreamExporterArgs {
            key_ttl_seconds: Some(0),
            ..Default::default()
        };
        assert!(args.build_config().is_err());
    }

    #[test]
    fn test_build_config_rejects_zero_pipeline_size() {
        let args = RedisStreamExporterArgs {
            pipeline_size: Some(0),
            ..Default::default()
        };
        assert!(args.build_config().is_err());
    }

    #[test]
    fn test_build_config_strips_empty_filter_names() {
        let args = RedisStreamExporterArgs {
            filter_service_names: vec![
                "api-gateway".to_string(),
                "".to_string(),
                "  ".to_string(),
                "payment".to_string(),
            ],
            ..Default::default()
        };
        let config = args.build_config().unwrap();
        assert_eq!(config.filter_service_names, vec!["api-gateway", "payment"]);
    }

    #[test]
    fn test_build_config_valid_ttl() {
        let args = RedisStreamExporterArgs {
            key_ttl_seconds: Some(3600),
            ..Default::default()
        };
        let config = args.build_config().unwrap();
        assert_eq!(config.key_ttl_seconds, Some(3600));
    }
}
