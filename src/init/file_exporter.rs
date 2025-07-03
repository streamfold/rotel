use clap::{Args, ValueEnum};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FileExporterFormat {
    /// Parquet format
    Parquet,
    /// JSON format
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ParquetCompression {
    /// No compression
    Uncompressed,
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

impl ParquetCompression {
    /// Convert to parquet::basic::Compression
    pub fn to_parquet_compression(&self) -> parquet::basic::Compression {
        match self {
            ParquetCompression::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => parquet::basic::Compression::SNAPPY,
            ParquetCompression::Gzip => parquet::basic::Compression::GZIP(Default::default()),
            ParquetCompression::Lz4 => parquet::basic::Compression::LZ4,
            ParquetCompression::Zstd => parquet::basic::Compression::ZSTD(Default::default()),
        }
    }

    /// Convert to lowercase string for display
    pub fn as_str(&self) -> &'static str {
        match self {
            ParquetCompression::Uncompressed => "uncompressed",
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

#[derive(Debug, Args, Clone)]
pub struct FileExporterArgs {
    /// File format for export
    #[arg(
        value_enum,
        long,
        env = "ROTEL_FILE_EXPORTER_FORMAT",
        default_value = "parquet"
    )]
    pub file_exporter_format: FileExporterFormat,

    /// Directory where files will be written
    #[arg(
        long,
        env = "ROTEL_FILE_EXPORTER_OUTPUT_DIR",
        default_value = "/tmp/rotel"
    )]
    pub file_exporter_output_dir: PathBuf,

    /// How often to flush data to disk (e.g., "5s")
    #[arg(long, env = "ROTEL_FILE_EXPORTER_FLUSH_INTERVAL", default_value = "5s", value_parser = humantime::parse_duration)]
    pub file_exporter_flush_interval: Duration,

    /// Compression type for Parquet files (only applies when format is parquet)
    #[arg(
        value_enum,
        long,
        env = "ROTEL_FILE_EXPORTER_PARQUET_COMPRESSION",
        default_value = "snappy"
    )]
    pub file_exporter_parquet_compression: ParquetCompression,
}

impl Default for FileExporterArgs {
    fn default() -> Self {
        FileExporterArgs {
            file_exporter_format: FileExporterFormat::Parquet,
            file_exporter_output_dir: PathBuf::from("/tmp/rotel"),
            file_exporter_flush_interval: Duration::from_secs(5),
            file_exporter_parquet_compression: ParquetCompression::Snappy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_compression_conversion() {
        use parquet::basic::Compression;

        // Test all compression type conversions
        assert!(matches!(
            ParquetCompression::Uncompressed.to_parquet_compression(),
            Compression::UNCOMPRESSED
        ));
        assert!(matches!(
            ParquetCompression::Snappy.to_parquet_compression(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            ParquetCompression::Gzip.to_parquet_compression(),
            Compression::GZIP(_)
        ));
        assert!(matches!(
            ParquetCompression::Lz4.to_parquet_compression(),
            Compression::LZ4
        ));
        assert!(matches!(
            ParquetCompression::Zstd.to_parquet_compression(),
            Compression::ZSTD(_)
        ));
    }

    #[test]
    fn test_parquet_compression_display() {
        assert_eq!(ParquetCompression::Uncompressed.as_str(), "uncompressed");
        assert_eq!(ParquetCompression::Snappy.as_str(), "snappy");
        assert_eq!(ParquetCompression::Gzip.as_str(), "gzip");
        assert_eq!(ParquetCompression::Lz4.as_str(), "lz4");
        assert_eq!(ParquetCompression::Zstd.as_str(), "zstd");
    }
}
