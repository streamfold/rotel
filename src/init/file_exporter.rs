use clap::Args;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Args, Clone)]
pub struct FileExporterArgs {
    /// File format for export (e.g., "parquet")
    #[arg(long, env = "ROTEL_FILE_OUTPUT_FORMAT", default_value = "parquet")]
    pub format: String,

    /// Directory where files will be written
    #[arg(long, env = "ROTEL_FILE_OUTPUT_DIR", default_value = "/tmp/rotel")]
    pub path: PathBuf,

    /// How often to flush data to disk (e.g., "5s")
    #[arg(long, env = "ROTEL_FILE_FLUSH_INTERVAL", default_value = "5s", value_parser = humantime::parse_duration)]
    pub flush_interval: Duration,
}
