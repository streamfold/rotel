use clap::Args;
use serde::Deserialize;

use crate::exporters::shared::aws::Region;

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct XRayExporterArgs {
    /// AWS X-Ray Exporter Region
    #[arg(
        value_enum,
        long("awsxray-exporter-region"),
        env = "ROTEL_AWSXRAY_EXPORTER_REGION",
        default_value = "us-east-1"
    )]
    pub region: Region,

    /// AWS X-Ray Exporter custom endpoint override
    #[arg(
        long("awsxray-exporter-custom-endpoint"),
        env = "ROTEL_AWSXRAY_EXPORTER_CUSTOM_ENDPOINT"
    )]
    pub custom_endpoint: Option<String>,
}

impl Default for XRayExporterArgs {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            custom_endpoint: None,
        }
    }
}
