use super::{LogRecordRow, MetricRow, SpanRow, ToRecordBatch};
use crate::exporters::file::{FileExporterError, Result, TypedFileExporter};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::path::Path;

/// A Parquet file exporter implementation
pub struct ParquetExporter {
    /// Writer properties for Parquet file generation
    writer_properties: WriterProperties,
}

impl ParquetExporter {
    /// Creates a new ParquetExporter with default settings (SNAPPY compression)
    pub fn new() -> Self {
        Self::with_compression(Compression::SNAPPY)
    }

    /// Creates a new ParquetExporter with specified compression
    pub fn with_compression(compression: Compression) -> Self {
        let writer_properties = WriterProperties::builder()
            .set_compression(compression)
            .build();

        Self { writer_properties }
    }

    /// Creates a new ParquetExporter with custom writer properties
    pub fn with_properties(writer_properties: WriterProperties) -> Self {
        Self { writer_properties }
    }

    /// Export a batch of SpanRow to Parquet
    pub fn export_span_rows(&self, rows: &[SpanRow], path: &Path) -> Result<()> {
        let batch = SpanRow::to_record_batch(rows.to_vec())?;
        self.export_record_batch(&batch, path)
    }

    /// Export a batch of MetricRow to Parquet
    pub fn export_metric_rows(&self, rows: &[MetricRow], path: &Path) -> Result<()> {
        let batch = MetricRow::to_record_batch(rows.to_vec())?;
        self.export_record_batch(&batch, path)
    }

    /// Export a batch of LogRecordRow to Parquet
    pub fn export_log_record_rows(&self, rows: &[LogRecordRow], path: &Path) -> Result<()> {
        let batch = LogRecordRow::to_record_batch(rows.to_vec())?;
        self.export_record_batch(&batch, path)
    }

    /// Export a generic RecordBatch to Parquet
    pub fn export_record_batch(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        let file = std::fs::File::create(path).map_err(FileExporterError::Io)?;
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), Some(self.writer_properties.clone()))
                .map_err(|e| {
                    FileExporterError::Export(format!("Failed to create ArrowWriter: {}", e))
                })?;
        writer.write(batch).map_err(|e| {
            FileExporterError::Export(format!("Failed to write RecordBatch: {}", e))
        })?;
        writer
            .close()
            .map_err(|e| FileExporterError::Export(format!("Failed to close writer: {}", e)))?;
        Ok(())
    }

    /// Validates JSON data for legacy compatibility - performs lightweight validation
    /// ensuring the payload is a **non-empty** JSON array (e.g. `[{...}, {...}]`).
    /// This is kept for testing purposes.
    pub fn validate_json(&self, data: &[u8]) -> Result<()> {
        let value: Value = serde_json::from_slice(data)
            .map_err(|e| FileExporterError::InvalidData(format!("Failed to parse JSON: {}", e)))?;

        let arr = value.as_array().ok_or_else(|| {
            FileExporterError::InvalidData("Expected JSON array of objects".to_string())
        })?;

        if arr.is_empty() {
            return Err(FileExporterError::InvalidData(
                "JSON array cannot be empty".to_string(),
            ));
        }

        Ok(())
    }
}

impl TypedFileExporter<ResourceSpans> for ParquetExporter {
    type Data = SpanRow;

    fn convert(&self, resource_spans: &ResourceSpans) -> Result<Vec<Self::Data>> {
        SpanRow::from_resource_spans(resource_spans)
    }

    fn export(&self, data: &[Self::Data], path: &std::path::Path) -> Result<()> {
        self.export_span_rows(data, path)
    }

    fn file_extension(&self) -> &'static str {
        ".parquet"
    }
}

impl TypedFileExporter<ResourceMetrics> for ParquetExporter {
    type Data = MetricRow;

    fn convert(&self, resource_metrics: &ResourceMetrics) -> Result<Vec<Self::Data>> {
        MetricRow::from_resource_metrics(resource_metrics)
    }

    fn export(&self, data: &[Self::Data], path: &std::path::Path) -> Result<()> {
        self.export_metric_rows(data, path)
    }

    fn file_extension(&self) -> &'static str {
        ".parquet"
    }
}

impl TypedFileExporter<ResourceLogs> for ParquetExporter {
    type Data = LogRecordRow;

    fn convert(&self, resource_logs: &ResourceLogs) -> Result<Vec<Self::Data>> {
        LogRecordRow::from_resource_logs(resource_logs)
    }

    fn export(&self, data: &[Self::Data], path: &std::path::Path) -> Result<()> {
        self.export_log_record_rows(data, path)
    }

    fn file_extension(&self) -> &'static str {
        ".parquet"
    }
}

impl Default for ParquetExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_valid_json() {
        let exporter = ParquetExporter::new();
        let data = r#"[
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]"#
        .as_bytes();
        let result = exporter.validate_json(data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_invalid_json() {
        let exporter = ParquetExporter::new();
        let data = r#"invalid json"#.as_bytes();
        let result = exporter.validate_json(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_empty_array() {
        let exporter = ParquetExporter::new();
        let data = r#"[]"#.as_bytes();
        let result = exporter.validate_json(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_with_compression() {
        use parquet::basic::Compression;

        // Test different compression types
        let gzip_exporter =
            ParquetExporter::with_compression(Compression::GZIP(Default::default()));
        let lz4_exporter = ParquetExporter::with_compression(Compression::LZ4);
        let uncompressed_exporter = ParquetExporter::with_compression(Compression::UNCOMPRESSED);

        // Just verify they can be created without error
        // In a real test, we'd check that the compression is actually applied to the output files
        let _ = gzip_exporter;
        let _ = lz4_exporter;
        let _ = uncompressed_exporter;
    }
}
