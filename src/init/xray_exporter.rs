use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct XRayExporterArgs {
    /// X-Ray Exporter Region
    #[arg(
        value_enum,
        long,
        env = "ROTEL_XRAY_EXPORTER_REGION",
        default_value = "us-east-1"
    )]
    pub xray_exporter_region: crate::exporters::xray::Region,

    /// X-Ray Exporter custom endpoint override
    #[arg(long, env = "ROTEL_XRAY_EXPORTER_CUSTOM_ENDPOINT")]
    pub xray_exporter_custom_endpoint: Option<String>,
}
