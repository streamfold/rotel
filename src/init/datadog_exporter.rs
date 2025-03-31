use clap::{Args, ValueEnum};

#[derive(Debug, Clone, Args)]
pub struct DatadogExporterArgs {
    /// Datadog Exporter Region
    #[arg(
        value_enum,
        long,
        env = "ROTEL_DATADOG_EXPORTER_REGION",
        default_value = "us1"
    )]
    pub datadog_exporter_region: DatadogRegion,

    /// Datadog Exporter custom endpoint override
    #[arg(long, env = "ROTEL_DATADOG_EXPORTER_CUSTOM_ENDPOINT")]
    pub datadog_exporter_custom_endpoint: Option<String>,

    /// Datadog Exporter API key
    #[arg(long, env = "ROTEL_DATADOG_EXPORTER_API_KEY")]
    pub datadog_exporter_api_key: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum DatadogRegion {
    US1,
    US3,
    US5,
    EU,
    AP1,
}
