use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct XRayExporterArgs {
    /// X-Ray Exporter Region
    #[arg(
        value_enum,
        long("xray-exporter-region"),
        env = "ROTEL_XRAY_EXPORTER_REGION",
        default_value = "us-east-1"
    )]
    pub region: crate::exporters::xray::Region,

    /// X-Ray Exporter custom endpoint override
    #[arg(
        long("xray-exporter-custom-endpoint"),
        env = "ROTEL_XRAY_EXPORTER_CUSTOM_ENDPOINT"
    )]
    pub custom_endpoint: Option<String>,
}
