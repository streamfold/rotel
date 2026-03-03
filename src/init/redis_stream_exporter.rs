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
        let config = RedisStreamExporterConfig::new(self.endpoint.clone())
            .with_stream_key_template(StreamKeyTemplate::parse(&self.stream_key_template))
            .with_format(self.format.into())
            .with_maxlen(self.maxlen)
            .with_cluster_mode(self.cluster_mode)
            .with_ca_cert_path(self.ca_cert_path.clone())
            .with_username(self.username.clone())
            .with_password(self.password.clone())
            .with_pipeline_size(self.pipeline_size);

        Ok(config)
    }
}
