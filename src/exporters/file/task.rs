use crate::bounded_channel::BoundedReceiver;
use crate::exporters::file::config::FileExporterConfig;
use crate::exporters::file::json::JsonExporter;
use crate::exporters::file::parquet::{LogRecordRow, MetricRow, ParquetExporter, SpanRow};
use crate::exporters::file::{FileExporterError, Result};
use crate::init::file_exporter::FileExporterFormat;
use chrono::Utc;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::path::Path;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Generic trait for file exporters that can handle different data formats
trait TypedFileExporter {
    /// The type used to represent span data in this exporter
    type SpanData: Clone;
    /// The type used to represent metric data in this exporter
    type MetricData: Clone;
    /// The type used to represent log data in this exporter
    type LogData: Clone;

    /// Convert ResourceSpans to the exporter's span data format
    fn convert_spans(&self, resource_spans: &ResourceSpans) -> Result<Vec<Self::SpanData>>;
    /// Convert ResourceMetrics to the exporter's metric data format
    fn convert_metrics(&self, resource_metrics: &ResourceMetrics) -> Result<Vec<Self::MetricData>>;
    /// Convert ResourceLogs to the exporter's log data format
    fn convert_logs(&self, resource_logs: &ResourceLogs) -> Result<Vec<Self::LogData>>;

    /// Export span data to a file
    fn export_spans(&self, data: &[Self::SpanData], path: &Path) -> Result<()>;
    /// Export metric data to a file
    fn export_metrics(&self, data: &[Self::MetricData], path: &Path) -> Result<()>;
    /// Export log data to a file
    fn export_logs(&self, data: &[Self::LogData], path: &Path) -> Result<()>;

    /// Get the file extension for this exporter
    fn file_extension(&self) -> &'static str;
}

/// Implementation of TypedFileExporter for ParquetExporter
impl TypedFileExporter for ParquetExporter {
    type SpanData = SpanRow;
    type MetricData = MetricRow;
    type LogData = LogRecordRow;

    fn convert_spans(&self, resource_spans: &ResourceSpans) -> Result<Vec<Self::SpanData>> {
        SpanRow::from_resource_spans(resource_spans)
    }

    fn convert_metrics(&self, resource_metrics: &ResourceMetrics) -> Result<Vec<Self::MetricData>> {
        MetricRow::from_resource_metrics(resource_metrics)
    }

    fn convert_logs(&self, resource_logs: &ResourceLogs) -> Result<Vec<Self::LogData>> {
        LogRecordRow::from_resource_logs(resource_logs)
    }

    fn export_spans(&self, data: &[Self::SpanData], path: &Path) -> Result<()> {
        self.export_span_rows(data, path)
    }

    fn export_metrics(&self, data: &[Self::MetricData], path: &Path) -> Result<()> {
        self.export_metric_rows(data, path)
    }

    fn export_logs(&self, data: &[Self::LogData], path: &Path) -> Result<()> {
        self.export_log_record_rows(data, path)
    }

    fn file_extension(&self) -> &'static str {
        ".parquet"
    }
}

/// Implementation of TypedFileExporter for JsonExporter
impl TypedFileExporter for JsonExporter {
    type SpanData = ResourceSpans;
    type MetricData = ResourceMetrics;
    type LogData = ResourceLogs;

    fn convert_spans(&self, resource_spans: &ResourceSpans) -> Result<Vec<Self::SpanData>> {
        Ok(vec![resource_spans.clone()])
    }

    fn convert_metrics(&self, resource_metrics: &ResourceMetrics) -> Result<Vec<Self::MetricData>> {
        Ok(vec![resource_metrics.clone()])
    }

    fn convert_logs(&self, resource_logs: &ResourceLogs) -> Result<Vec<Self::LogData>> {
        Ok(vec![resource_logs.clone()])
    }

    fn export_spans(&self, data: &[Self::SpanData], path: &Path) -> Result<()> {
        self.export_traces(data, path)
    }

    fn export_metrics(&self, data: &[Self::MetricData], path: &Path) -> Result<()> {
        self.export_metrics(data, path)
    }

    fn export_logs(&self, data: &[Self::LogData], path: &Path) -> Result<()> {
        self.export_logs(data, path)
    }

    fn file_extension(&self) -> &'static str {
        ".json"
    }
}

pub async fn run_file_exporter(
    config: FileExporterConfig,
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
    token: CancellationToken,
) -> Result<()> {
    let format = config.format;
    let path = config.path;
    let flush_interval = config.flush_interval;

    // Create output directories if they don't exist
    let traces_dir = path.join("spans");
    let metrics_dir = path.join("metrics");
    let logs_dir = path.join("logs");

    std::fs::create_dir_all(&traces_dir).map_err(FileExporterError::Io)?;
    std::fs::create_dir_all(&metrics_dir).map_err(FileExporterError::Io)?;
    std::fs::create_dir_all(&logs_dir).map_err(FileExporterError::Io)?;

    info!(
        "File exporter started, writing {} data to {}",
        format,
        path.display()
    );

    match format {
        FileExporterFormat::Parquet => {
            let exporter = ParquetExporter::new();
            run_export_loop(
                exporter,
                traces_dir,
                metrics_dir,
                logs_dir,
                traces_rx,
                metrics_rx,
                logs_rx,
                flush_interval,
                token,
            )
            .await
        }
        FileExporterFormat::Json => {
            let exporter = JsonExporter::new();
            run_export_loop(
                exporter,
                traces_dir,
                metrics_dir,
                logs_dir,
                traces_rx,
                metrics_rx,
                logs_rx,
                flush_interval,
                token,
            )
            .await
        }
    }
}

/// Generic export loop that works with any TypedFileExporter implementation
#[allow(clippy::too_many_arguments)]
async fn run_export_loop<E>(
    exporter: E,
    traces_dir: std::path::PathBuf,
    metrics_dir: std::path::PathBuf,
    logs_dir: std::path::PathBuf,
    mut traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    mut metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    mut logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter,
{
    let file_ext = exporter.file_extension();

    // In-memory buffers for accumulating data
    let mut span_buffer: Vec<E::SpanData> = Vec::new();
    let mut metric_buffer: Vec<E::MetricData> = Vec::new();
    let mut log_buffer: Vec<E::LogData> = Vec::new();

    // Helper that writes out any non-empty buffer and clears it afterwards
    let flush = |span_buf: &mut Vec<E::SpanData>,
                 metric_buf: &mut Vec<E::MetricData>,
                 log_buf: &mut Vec<E::LogData>|
     -> Result<()> {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        if !span_buf.is_empty() {
            let rows = span_buf.len();
            let file_path = traces_dir.join(format!("spans_{}{}", timestamp, file_ext));
            exporter.export_spans(span_buf, &file_path)?;
            debug!(rows, path=%file_path.display(), "Flushed spans file");
            span_buf.clear();
        }
        if !metric_buf.is_empty() {
            let rows = metric_buf.len();
            let file_path = metrics_dir.join(format!("metrics_{}{}", timestamp, file_ext));
            exporter.export_metrics(metric_buf, &file_path)?;
            debug!(rows, path=%file_path.display(), "Flushed metrics file");
            metric_buf.clear();
        }
        if !log_buf.is_empty() {
            let rows = log_buf.len();
            let file_path = logs_dir.join(format!("logs_{}{}", timestamp, file_ext));
            exporter.export_logs(log_buf, &file_path)?;
            debug!(rows, path=%file_path.display(), "Flushed logs file");
            log_buf.clear();
        }
        Ok(())
    };

    let mut flush_timer = tokio::time::interval(flush_interval);
    loop {
        tokio::select! {
            Some(traces) = traces_rx.next() => {
                for resource_spans in traces {
                    let mut converted_spans = exporter.convert_spans(&resource_spans)?;
                    span_buffer.append(&mut converted_spans);
                }
            }
            Some(metrics) = metrics_rx.next() => {
                for resource_metrics in metrics {
                    let mut converted_metrics = exporter.convert_metrics(&resource_metrics)?;
                    metric_buffer.append(&mut converted_metrics);
                }
            }
            Some(logs) = logs_rx.next() => {
                for resource_logs in logs {
                    let mut converted_logs = exporter.convert_logs(&resource_logs)?;
                    log_buffer.append(&mut converted_logs);
                }
            }
            _ = flush_timer.tick() => {
                flush(&mut span_buffer, &mut metric_buffer, &mut log_buffer)?;
            }
            _ = token.cancelled() => {
                info!("File exporter received shutdown signal");
                // Final flush before exit
                flush(&mut span_buffer, &mut metric_buffer, &mut log_buffer)?;
                break;
            }
        }
    }

    info!("File exporter shutting down, flush complete");
    Ok(())
}
