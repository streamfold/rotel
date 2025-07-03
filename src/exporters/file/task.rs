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
pub trait TypedFileExporter {
    /// The type used to represent span data in this exporter
    type SpanData: Clone + Send;
    /// The type used to represent metric data in this exporter
    type MetricData: Clone + Send;
    /// The type used to represent log data in this exporter
    type LogData: Clone + Send;

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
    let path = config.output_dir;
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
            let compression = config.parquet_compression.to_parquet_compression();
            let exporter = std::sync::Arc::new(ParquetExporter::with_compression(compression));
            run_exporter_tasks(
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
            let exporter = std::sync::Arc::new(JsonExporter::new());
            run_exporter_tasks(
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

/// Helper function to spawn and join exporter tasks for traces, metrics, and logs
async fn run_exporter_tasks<E>(
    exporter: std::sync::Arc<E>,
    traces_dir: std::path::PathBuf,
    metrics_dir: std::path::PathBuf,
    logs_dir: std::path::PathBuf,
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter + Send + Sync + 'static,
{
    let traces_task = {
        let exporter = exporter.clone();
        let traces_dir = traces_dir.clone();
        let token = token.clone();
        tokio::spawn(async move {
            run_traces_loop(exporter, traces_dir, traces_rx, flush_interval, token).await
        })
    };

    let metrics_task = {
        let exporter = exporter.clone();
        let metrics_dir = metrics_dir.clone();
        let token = token.clone();
        tokio::spawn(async move {
            run_metrics_loop(exporter, metrics_dir, metrics_rx, flush_interval, token).await
        })
    };

    let logs_task = {
        let exporter = exporter.clone();
        let logs_dir = logs_dir.clone();
        let token = token.clone();
        tokio::spawn(async move {
            run_logs_loop(exporter, logs_dir, logs_rx, flush_interval, token).await
        })
    };

    let (traces_result, metrics_result, logs_result) =
        tokio::join!(traces_task, metrics_task, logs_task);

    if let Err(e) = traces_result {
        return Err(FileExporterError::Export(format!(
            "Traces task failed: {}",
            e
        )));
    }
    if let Err(e) = metrics_result {
        return Err(FileExporterError::Export(format!(
            "Metrics task failed: {}",
            e
        )));
    }
    if let Err(e) = logs_result {
        return Err(FileExporterError::Export(format!(
            "Logs task failed: {}",
            e
        )));
    }

    Ok(())
}

/// Event loop for processing trace data
pub async fn run_traces_loop<E>(
    exporter: std::sync::Arc<E>,
    traces_dir: std::path::PathBuf,
    mut traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter + Send + Sync,
{
    let file_ext = exporter.file_extension();
    let mut span_buffer: Vec<E::SpanData> = Vec::new();
    let mut flush_timer = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            traces_result = traces_rx.next() => {
                match traces_result {
                    Some(traces) => {
                        // Process incoming traces
                        for resource_spans in traces {
                            let mut converted_spans = exporter.convert_spans(&resource_spans)?;
                            span_buffer.append(&mut converted_spans);
                        }
                    }
                    None => {
                        // Channel closed, flush and shutdown gracefully
                        debug!("Traces channel closed, flushing final data");
                        flush_spans(&*exporter, &mut span_buffer, &traces_dir, file_ext)?;
                        info!("Traces exporter shutting down gracefully");
                        return Ok(());
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush_spans(&*exporter, &mut span_buffer, &traces_dir, file_ext)?;
            }
            _ = token.cancelled() => {
                info!("Traces exporter received shutdown signal");
                flush_spans(&*exporter, &mut span_buffer, &traces_dir, file_ext)?;
                return Ok(());
            }
        }
    }
}

/// Event loop for processing metrics data
pub async fn run_metrics_loop<E>(
    exporter: std::sync::Arc<E>,
    metrics_dir: std::path::PathBuf,
    mut metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter + Send + Sync,
{
    let file_ext = exporter.file_extension();
    let mut metric_buffer: Vec<E::MetricData> = Vec::new();
    let mut flush_timer = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            metrics_result = metrics_rx.next() => {
                match metrics_result {
                    Some(metrics) => {
                        // Process incoming metrics
                        for resource_metrics in metrics {
                            let mut converted_metrics = exporter.convert_metrics(&resource_metrics)?;
                            metric_buffer.append(&mut converted_metrics);
                        }
                    }
                    None => {
                        // Channel closed, flush and shutdown gracefully
                        debug!("Metrics channel closed, flushing final data");
                        flush_metrics(&*exporter, &mut metric_buffer, &metrics_dir, file_ext)?;
                        info!("Metrics exporter shutting down gracefully");
                        return Ok(());
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush_metrics(&*exporter, &mut metric_buffer, &metrics_dir, file_ext)?;
            }
            _ = token.cancelled() => {
                info!("Metrics exporter received shutdown signal");
                flush_metrics(&*exporter, &mut metric_buffer, &metrics_dir, file_ext)?;
                return Ok(());
            }
        }
    }
}

/// Event loop for processing logs data
pub async fn run_logs_loop<E>(
    exporter: std::sync::Arc<E>,
    logs_dir: std::path::PathBuf,
    mut logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter + Send + Sync,
{
    let file_ext = exporter.file_extension();
    let mut log_buffer: Vec<E::LogData> = Vec::new();
    let mut flush_timer = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            logs_result = logs_rx.next() => {
                match logs_result {
                    Some(logs) => {
                        // Process incoming logs
                        for resource_logs in logs {
                            let mut converted_logs = exporter.convert_logs(&resource_logs)?;
                            log_buffer.append(&mut converted_logs);
                        }
                    }
                    None => {
                        // Channel closed, flush and shutdown gracefully
                        debug!("Logs channel closed, flushing final data");
                        flush_logs(&*exporter, &mut log_buffer, &logs_dir, file_ext)?;
                        info!("Logs exporter shutting down gracefully");
                        return Ok(());
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush_logs(&*exporter, &mut log_buffer, &logs_dir, file_ext)?;
            }
            _ = token.cancelled() => {
                info!("Logs exporter received shutdown signal");
                flush_logs(&*exporter, &mut log_buffer, &logs_dir, file_ext)?;
                return Ok(());
            }
        }
    }
}

/// Helper function to flush span data to disk
fn flush_spans<E>(
    exporter: &E,
    span_buffer: &mut Vec<E::SpanData>,
    traces_dir: &std::path::Path,
    file_ext: &str,
) -> Result<()>
where
    E: TypedFileExporter,
{
    if !span_buffer.is_empty() {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let rows = span_buffer.len();
        let file_path = traces_dir.join(format!("spans_{}{}", timestamp, file_ext));
        exporter.export_spans(span_buffer, &file_path)?;
        debug!(rows, path=%file_path.display(), "Flushed spans file");
        span_buffer.clear();
    }
    Ok(())
}

/// Helper function to flush metrics data to disk
fn flush_metrics<E>(
    exporter: &E,
    metric_buffer: &mut Vec<E::MetricData>,
    metrics_dir: &std::path::Path,
    file_ext: &str,
) -> Result<()>
where
    E: TypedFileExporter,
{
    if !metric_buffer.is_empty() {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let rows = metric_buffer.len();
        let file_path = metrics_dir.join(format!("metrics_{}{}", timestamp, file_ext));
        exporter.export_metrics(metric_buffer, &file_path)?;
        debug!(rows, path=%file_path.display(), "Flushed metrics file");
        metric_buffer.clear();
    }
    Ok(())
}

/// Helper function to flush logs data to disk
fn flush_logs<E>(
    exporter: &E,
    log_buffer: &mut Vec<E::LogData>,
    logs_dir: &std::path::Path,
    file_ext: &str,
) -> Result<()>
where
    E: TypedFileExporter,
{
    if !log_buffer.is_empty() {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let rows = log_buffer.len();
        let file_path = logs_dir.join(format!("logs_{}{}", timestamp, file_ext));
        exporter.export_logs(log_buffer, &file_path)?;
        debug!(rows, path=%file_path.display(), "Flushed logs file");
        log_buffer.clear();
    }
    Ok(())
}
