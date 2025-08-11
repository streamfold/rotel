use crate::exporters::awsemf::Region;
use clap::{Args, ValueEnum};
use serde::Deserialize;
use std::collections::HashMap;

use super::parse;

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct AwsEmfExporterArgs {
    /// AWS EMF Exporter Region
    #[arg(
        id("AWS_EMF_REGION"),
        value_enum,
        long("awsemf-exporter-region"),
        env = "ROTEL_AWSEMF_EXPORTER_REGION",
        default_value = "us-east-1"
    )]
    pub region: Region,

    /// AWS EMF Exporter custom endpoint override
    #[arg(
        id("AWS_EMF_CUSTOM_ENDPOINT"),
        long("awsemf-exporter-custom-endpoint"),
        env = "ROTEL_AWSEMF_EXPORTER_CUSTOM_ENDPOINT"
    )]
    pub custom_endpoint: Option<String>,

    /// CloudWatch log group name
    #[arg(
        long("awsemf-exporter-log-group-name"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_GROUP_NAME",
        default_value = "/rotel/metrics"
    )]
    pub log_group_name: String,

    /// CloudWatch log stream name
    #[arg(
        long("awsemf-exporter-log-stream-name"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_STREAM_NAME"
    )]
    pub log_stream_name: Option<String>,

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
        action = clap::ArgAction::SetTrue
    )]
    pub retain_initial_value_of_delta_metric: bool,

    /// Log retention in days (0 for never expire)
    #[arg(
        long("awsemf-exporter-log-retention"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_RETENTION",
        default_value = "0"
    )]
    pub log_retention: i32,

    /// CloudWatch Log Group tags (key=value pairs, comma-separated)
    #[arg(
        long("awsemf-exporter-tags"),
        env = "ROTEL_AWSEMF_EXPORTER_TAGS",
        value_parser = parse::parse_key_val::<String, String>,
        value_delimiter = ',',
        help = "CloudWatch Log Group tags in key=value format, comma-separated"
    )]
    pub tags: Option<HashMap<String, String>>,
}

impl Default for AwsEmfExporterArgs {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            custom_endpoint: None,
            log_group_name: "/rotel/metrics".to_string(),
            log_stream_name: None,
            namespace: None,
            retain_initial_value_of_delta_metric: false,
            log_retention: 0,
            tags: None,
        }
    }
}
