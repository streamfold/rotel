use crate::exporters::clickhouse;
use clap::{Args, ValueEnum};

#[derive(Debug, Clone, Args)]
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
        env = "ROTEL_CLICKHOUSE_TABLE_PREFIX",
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
    pub user: Option<String>,

    /// Clickhouse Exporter password
    #[arg(
        long("clickhouse-exporter-password"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_PASSWORD"
    )]
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

    /// Clickhouse Exporter replace periods in JSON keys with underscores
    #[arg(
        long("clickhouse-exporter-json-underscore"),
        env = "ROTEL_CLICKHOUSE_EXPORTER_JSON_UNDERSCORE",
        default_value = "false"
    )]
    pub json_underscore: bool,
}

#[derive(Clone, Debug, ValueEnum)]
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
