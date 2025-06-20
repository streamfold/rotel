use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum FileExporterFormat {
    /// Parquet format
    Parquet,
    /// JSON format
    Json,
}

impl FileExporterFormat {
    /// Convert to lowercase string for backward compatibility
    pub fn as_str(&self) -> &'static str {
        match self {
            FileExporterFormat::Parquet => "parquet",
            FileExporterFormat::Json => "json",
        }
    }
}

impl std::fmt::Display for FileExporterFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Args, Clone)]
pub struct FileExporterArgs {
    /// File format for export
    #[arg(value_enum, long, env = "ROTEL_FILE_OUTPUT_FORMAT", default_value = "parquet")]
    pub format: FileExporterFormat,

    /// Directory where files will be written
    #[arg(long, env = "ROTEL_FILE_OUTPUT_DIR", default_value = "/tmp/rotel")]
    pub path: PathBuf,

    /// How often to flush data to disk (e.g., "5s")
    #[arg(long, env = "ROTEL_FILE_FLUSH_INTERVAL", default_value = "5s", value_parser = humantime::parse_duration)]
    pub flush_interval: Duration,
}
