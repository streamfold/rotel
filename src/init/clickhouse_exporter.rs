use crate::exporters::clickhouse;
use clap::{Args, ValueEnum};

#[derive(Debug, Clone, Args)]
pub struct ClickhouseExporterArgs {
    /// Clickhouse Exporter endpoint
    #[arg(long, env = "ROTEL_CLICKHOUSE_EXPORTER_ENDPOINT")]
    pub clickhouse_exporter_endpoint: Option<String>,

    /// Clickhouse Exporter database
    #[arg(
        long,
        env = "ROTEL_CLICKHOUSE_EXPORTER_DATABASE",
        default_value = "otel"
    )]
    pub clickhouse_exporter_database: String,

    /// Clickhouse Exporter table prefix (e.g., "otel" prefix will become "otel_traces" for traces)
    #[arg(long, env = "ROTEL_CLICKHOUSE_TABLE_PREFIX", default_value = "otel")]
    pub clickhouse_exporter_table_prefix: String,

    /// Clickhouse Exporter compression (lz4 or none)
    #[arg(
        value_enum,
        long,
        env = "ROTEL_CLICKHOUSE_EXPORTER_COMPRESSION",
        default_value = "lz4"
    )]
    pub clickhouse_exporter_compression: Compression,

    /// Clickhouse Exporter user
    #[arg(long, env = "ROTEL_CLICKHOUSE_EXPORTER_USER")]
    pub clickhouse_exporter_user: Option<String>,

    /// Clickhouse Exporter password
    #[arg(long, env = "ROTEL_CLICKHOUSE_EXPORTER_PASSWORD")]
    pub clickhouse_exporter_password: Option<String>,

    /// Clickhouse Exporter async insert
    #[arg(
        long,
        env = "ROTEL_CLICKHOUSE_EXPORTER_ASYNC_INSERT",
        default_value = "true"
    )]
    pub clickhouse_exporter_async_insert: String,

    /// Clickhouse Exporter enable JSON column type
    #[arg(
        long,
        env = "ROTEL_CLICKHOUSE_EXPORTER_ENABLE_JSON",
        default_value = "false"
    )]
    pub clickhouse_exporter_enable_json: bool,
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
