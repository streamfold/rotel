use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct ParquetExporterArgs {
    /// Parquet Exporter output directory
    #[arg(
        long,
        env = "ROTEL_PARQUET_EXPORTER_OUTPUT_DIR",
        default_value = "/tmp/rotel"
    )]
    pub output_dir: String,
} 