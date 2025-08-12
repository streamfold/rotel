use clap::Args;
use serde::Deserialize;

use crate::exporters::shared::aws::Region;

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct AwsEmfExporterArgs {
    /// AWS EMF Exporter Region
    #[arg(
        id("AWSEMF_REGION"),
        value_enum,
        long("awsemf-exporter-region"),
        env = "ROTEL_AWSEMF_EXPORTER_REGION",
        default_value = "us-east-1"
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
}

impl Default for AwsEmfExporterArgs {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            custom_endpoint: None,
            log_group_name: "/metrics/default".to_string(),
            log_stream_name: "otel-stream".to_string(),
            namespace: None,
            retain_initial_value_of_delta_metric: false,
        }
    }
}
