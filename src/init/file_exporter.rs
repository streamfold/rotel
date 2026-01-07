use clap::{Args, ValueEnum};
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Default, Debug, Clone, Copy, PartialEq, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileExporterFormat {
    #[default]
    /// Parquet format
    Parquet,
    /// JSON format
    Json,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCompression {
    /// No compression
    None,
    #[default]
    /// Snappy compression
    Snappy,
    /// Gzip compression
    Gzip,
    /// LZ4 compression
    Lz4,
    /// ZSTD compression
    Zstd,
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

impl From<ParquetCompression> for parquet::basic::Compression {
    fn from(compression: ParquetCompression) -> Self {
        match compression {
            ParquetCompression::None => parquet::basic::Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => parquet::basic::Compression::SNAPPY,
            ParquetCompression::Gzip => parquet::basic::Compression::GZIP(Default::default()),
            ParquetCompression::Lz4 => parquet::basic::Compression::LZ4,
            ParquetCompression::Zstd => parquet::basic::Compression::ZSTD(Default::default()),
        }
    }
}

impl ParquetCompression {
    /// Convert to lowercase string for display
    pub fn as_str(&self) -> &'static str {
        match self {
            ParquetCompression::None => "none",
            ParquetCompression::Snappy => "snappy",
            ParquetCompression::Gzip => "gzip",
            ParquetCompression::Lz4 => "lz4",
            ParquetCompression::Zstd => "zstd",
        }
    }
}

impl std::fmt::Display for FileExporterFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::fmt::Display for ParquetCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct FileExporterArgs {
    /// File format for export
    #[arg(
        value_enum,
        long("file-exporter-format"),
        env = "ROTEL_FILE_EXPORTER_FORMAT",
        default_value = "parquet"
    )]
    pub file_format: FileExporterFormat,

    /// Directory where files will be written
    #[arg(
        long("file-exporter-output-dir"),
        env = "ROTEL_FILE_EXPORTER_OUTPUT_DIR",
        default_value = "/tmp/rotel"
    )]
    pub output_dir: PathBuf,

    /// How often to flush data to disk (e.g., "5s")
    #[arg(
        long("file-exporter-flush-interval"),
        env = "ROTEL_FILE_EXPORTER_FLUSH_INTERVAL",
        default_value = "5s",
        value_parser = humantime::parse_duration
    )]
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,

    /// Compression type for Parquet files (only applies when format is parquet)
    #[arg(
        value_enum,
        long("file-exporter-parquet-compression"),
        env = "ROTEL_FILE_EXPORTER_PARQUET_COMPRESSION",
        default_value = "snappy"
    )]
    pub parquet_compression: ParquetCompression,
}

impl Default for FileExporterArgs {
    fn default() -> Self {
        FileExporterArgs {
            file_format: Default::default(),
            output_dir: PathBuf::from("/tmp/rotel"),
            flush_interval: Duration::from_secs(5),
            parquet_compression: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_compression_conversion() {
        use parquet::basic::Compression;

        // Test all compression type conversions using From trait
        assert!(matches!(
            ParquetCompression::None.into(),
            Compression::UNCOMPRESSED
        ));
        assert!(matches!(
            ParquetCompression::Snappy.into(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            ParquetCompression::Gzip.into(),
            Compression::GZIP(_)
        ));
        assert!(matches!(ParquetCompression::Lz4.into(), Compression::LZ4));
        assert!(matches!(
            ParquetCompression::Zstd.into(),
            Compression::ZSTD(_)
        ));
    }

    #[test]
    fn test_parquet_compression_display() {
        assert_eq!(ParquetCompression::None.as_str(), "none");
        assert_eq!(ParquetCompression::Snappy.as_str(), "snappy");
        assert_eq!(ParquetCompression::Gzip.as_str(), "gzip");
        assert_eq!(ParquetCompression::Lz4.as_str(), "lz4");
        assert_eq!(ParquetCompression::Zstd.as_str(), "zstd");
    }
}
