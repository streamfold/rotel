use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct ClickhouseExporterArgs {
    /// Clickhouse Exporter endpoint
    #[arg(long, env = "ROTEL_CLICKHOUSE_EXPORTER_ENDPOINT")]
    pub clickhouse_exporter_endpoint: Option<String>,
}
