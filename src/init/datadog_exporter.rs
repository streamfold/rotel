use clap::{Args, ValueEnum};
use serde::Deserialize;

// Define Datadog-specific retry arguments with proper prefixes
crate::define_exporter_retry_args!(
    DatadogRetryArgs,
    "datadog-exporter",
    "ROTEL_DATADOG_EXPORTER",
    "Datadog Exporter"
);

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct DatadogExporterArgs {
    /// Datadog Exporter Region
    #[arg(
        id("DATADOG_REGION"),
        value_enum,
        long("datadog-exporter-region"),
        env = "ROTEL_DATADOG_EXPORTER_REGION",
        default_value = "us1"
    )]
    pub region: DatadogRegion,

    /// Datadog Exporter custom endpoint override
    #[arg(
        id("DATADOG_CUSTOM_ENDPOINT"),
        long("datadog-exporter-custom-endpoint"),
        env = "ROTEL_DATADOG_EXPORTER_CUSTOM_ENDPOINT"
    )]
    pub custom_endpoint: Option<String>,

    /// Datadog Exporter API key
    #[arg(
        long("datadog-exporter-api-key"),
        env = "ROTEL_DATADOG_EXPORTER_API_KEY"
    )]
    #[serde(deserialize_with = "crate::init::parse::deser_into_string_opt")]
    pub api_key: Option<String>,

    /// Datadog Exporter retry configuration
    #[command(flatten)]
    pub retry: DatadogRetryArgs,
}

impl Default for DatadogExporterArgs {
    fn default() -> Self {
        Self {
            region: DatadogRegion::US1,
            custom_endpoint: None,
            api_key: None,
            retry: DatadogRetryArgs::default(),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum DatadogRegion {
    US1,
    US3,
    US5,
    EU,
    AP1,
}
