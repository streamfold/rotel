use clap::Args;
use serde::Deserialize;

use crate::exporters::shared::aws::Region;

crate::define_exporter_retry_args!(
    XRayRetryArgs,
    "awsxray-exporter",
    "ROTEL_AWSXRAY_EXPORTER",
    "AWS X-Ray Exporter"
);

#[derive(Debug, Default, Clone, Args, Deserialize)]
#[serde(default)]
pub struct XRayExporterArgs {
    /// AWS X-Ray Exporter Region
    #[arg(
        long("awsxray-exporter-region"),
        env = "ROTEL_AWSXRAY_EXPORTER_REGION",
        default_value_t
    )]
    pub region: Region,

    /// AWS X-Ray Exporter custom endpoint override
    #[arg(
        long("awsxray-exporter-custom-endpoint"),
        env = "ROTEL_AWSXRAY_EXPORTER_CUSTOM_ENDPOINT"
    )]
    pub custom_endpoint: Option<String>,

    /// AWS X-Ray Exporter retry configuration
    #[command(flatten)]
    pub retry: XRayRetryArgs,
}
