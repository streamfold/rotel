use clap::Args;
use serde::Deserialize;

use crate::exporters::shared::aws::Region;

crate::define_exporter_retry_args!(
    AwsEmfRetryArgs,
    "awsemf-exporter",
    "ROTEL_AWSEMF_EXPORTER",
    "AWS EMF Exporter"
);

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct AwsEmfExporterArgs {
    /// AWS EMF Exporter Region
    #[arg(
        id("AWSEMF_REGION"),
        long("awsemf-exporter-region"),
        env = "ROTEL_AWSEMF_EXPORTER_REGION",
        default_value_t
    )]
    pub region: Region,

    /// AWS EMF Exporter custom endpoint override
    #[arg(
        id("AWSEMF_CUSTOM_ENDPOINT"),
        long("awsemf-exporter-custom-endpoint"),
        env = "ROTEL_AWSEMF_EXPORTER_CUSTOM_ENDPOINT"
    )]
    pub custom_endpoint: Option<String>,

    /// CloudWatch log group name
    #[arg(
        long("awsemf-exporter-log-group-name"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_GROUP_NAME",
        default_value = "/metrics/default"
    )]
    pub log_group_name: String,

    /// CloudWatch log stream name
    #[arg(
        long("awsemf-exporter-log-stream-name"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_STREAM_NAME",
        default_value = "otel-stream"
    )]
    pub log_stream_name: String,

    /// CloudWatch log group retention
    #[arg(
        long("awsemf-exporter-log-retention"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_RETENTION"
    )]
    pub log_retention: Option<u16>,

    /// CloudWatch metrics namespace
    #[arg(
        long("awsemf-exporter-namespace"),
        env = "ROTEL_AWSEMF_EXPORTER_NAMESPACE"
    )]
    pub namespace: Option<String>,

    /// Retain initial value of delta metric
    #[arg(
        long("awsemf-exporter-retain-initial-value-of-delta-metric"),
        env = "ROTEL_AWSEMF_EXPORTER_RETAIN_INITIAL_VALUE_OF_DELTA_METRIC",
        default_value = "false"
    )]
    pub retain_initial_value_of_delta_metric: bool,

    /// Dimensions include list
    #[arg(
        long("awsemf-exporter-include-dimensions"),
        env = "ROTEL_AWSEMF_EXPORTER_INCLUDE_DIMENSIONS",
        value_delimiter = ','
    )]
    pub include_dimensions: Vec<String>,

    /// Dimensions exclude list
    #[arg(
        long("awsemf-exporter-exclude-dimensions"),
        env = "ROTEL_AWSEMF_EXPORTER_EXCLUDE_DIMENSIONS",
        value_delimiter = ','
    )]
    pub exclude_dimensions: Vec<String>,

    /// Retry configuration
    #[command(flatten)]
    pub retry: AwsEmfRetryArgs,
}

impl Default for AwsEmfExporterArgs {
    fn default() -> Self {
        Self {
            region: Region::default(),
            custom_endpoint: None,
            log_group_name: "/metrics/default".to_string(),
            log_stream_name: "otel-stream".to_string(),
            log_retention: None,
            namespace: None,
            retain_initial_value_of_delta_metric: false,
            include_dimensions: Vec::new(),
            exclude_dimensions: Vec::new(),
            retry: Default::default(),
        }
    }
}
