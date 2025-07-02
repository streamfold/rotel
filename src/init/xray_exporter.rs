use crate::exporters::xray::Region;
use clap::Args;
use serde::Deserialize;

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct XRayExporterArgs {
    /// X-Ray Exporter Region
    #[arg(
        value_enum,
        long("xray-exporter-region"),
        env = "ROTEL_XRAY_EXPORTER_REGION",
        default_value = "us-east-1"
    )]
    pub region: Region,

    /// X-Ray Exporter custom endpoint override
    #[arg(
        long("xray-exporter-custom-endpoint"),
        env = "ROTEL_XRAY_EXPORTER_CUSTOM_ENDPOINT"
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
