use crate::exporters::clickhouse;
use clap::{Args, ValueEnum};
use serde::Deserialize;

crate::define_exporter_retry_args!(
    ClickhouseRetryArgs,
    "clickhouse-exporter",
    "ROTEL_CLICKHOUSE_EXPORTER",
    "Clickhouse Exporter"
);

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct ClickhouseExporterArgs {
    /// Clickhouse Exporter endpoint
    #[arg(
        id("CLICKHOUSE_ENDPOINT"),
        long("clickhouse-exporter-endpoint"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_ENDPOINT"
    )]
    pub endpoint: Option<String>,

    /// Clickhouse Exporter database
    #[arg(
        long("clickhouse-exporter-database"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_DATABASE",
        default_value = "otel"
    )]
    pub database: String,

    /// Clickhouse Exporter table prefix (e.g., "otel" prefix will become "otel_traces" for traces)
    #[arg(
        long("clickhouse-exporter-table-prefix"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_TABLE_PREFIX",
        default_value = "otel"
    )]
    pub table_prefix: String,

    /// Clickhouse Exporter compression (lz4 or none)
    #[arg(
        id("CLICKHOUSE_COMPRESSION"),
        value_enum,
        long("clickhouse-exporter-compression"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_COMPRESSION",
        default_value = "lz4"
    )]
    pub compression: Compression,

    /// Clickhouse Exporter user
    #[arg(
        long("clickhouse-exporter-user"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_USER"
    )]
    #[serde(deserialize_with = "crate::init::parse::deser_into_string_opt")]
    pub user: Option<String>,

    /// Clickhouse Exporter password
    #[arg(
        long("clickhouse-exporter-password"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_PASSWORD"
    )]
    #[serde(deserialize_with = "crate::init::parse::deser_into_string_opt")]
    pub password: Option<String>,

    /// Clickhouse Exporter async insert
    #[arg(
        long("clickhouse-exporter-async-insert"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_ASYNC_INSERT",
        default_value = "true"
    )]
    pub async_insert: String,

    /// Clickhouse Exporter enable JSON column type
    #[arg(
        long("clickhouse-exporter-enable-json"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_ENABLE_JSON",
        default_value = "false"
    )]
    pub enable_json: bool,

    /// Clickhouse Exporter nested KV max depth for JSON columns.
    /// When set to a value > 0, nested KeyValueList structures are converted to JSON objects
    /// up to the specified depth. When unset or 0, nested KV is flattened (backwards compatible).
    /// Recommended value: 10 for GenAI attributes.
    #[arg(
        long("clickhouse-exporter-nested-kv-max-depth"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_NESTED_KV_MAX_DEPTH"
    )]
    pub nested_kv_max_depth: Option<usize>,

    /// Clickhouse Exporter request timeout
    #[arg(
        id("CLICKHOUSE_EXPORTER_REQUEST_TIMEOUT"),
        long("clickhouse-exporter-request-timeout"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_REQUEST_TIMEOUT",
        default_value = "5s",
        value_parser = humantime::parse_duration
    )]
    #[serde(with = "humantime_serde")]
    pub request_timeout: std::time::Duration,

    /// Clickhouse Exporter retry configuration
    #[command(flatten)]
    #[serde(flatten)]
    pub retry: ClickhouseRetryArgs,
}

impl Default for ClickhouseExporterArgs {
    fn default() -> Self {
        Self {
            endpoint: None,
            database: "otel".to_string(),
            table_prefix: "otel".to_string(),
            compression: Compression::Lz4,
            user: None,
            password: None,
            async_insert: "true".to_string(),
            enable_json: false,
            nested_kv_max_depth: None,
            request_timeout: std::time::Duration::from_secs(5),
            retry: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Copy, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    None,
    Lz4,
}

impl From<Compression> for clickhouse::Compression {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => clickhouse::Compression::None,
            Compression::Lz4 => clickhouse::Compression::Lz4,
        }
    }
}
