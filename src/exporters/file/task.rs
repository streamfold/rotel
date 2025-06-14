use crate::bounded_channel::BoundedReceiver;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use crate::exporters::file::schema::{SpanRow, MetricRow, LogRecordRow};
use crate::exporters::file::{FileExporterError, config::FileExporterConfig, parquet::ParquetExporter};
use tokio::time::sleep;
use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use crate::exporters::file::json::JsonExporter;

pub async fn run_file_exporter(
    config: FileExporterConfig,
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
    token: CancellationToken,
) -> Result<(), FileExporterError> {
    let _format = config.format;
    let path = config.path;
    let flush_interval = config.flush_interval;
    
    let format = _format.to_lowercase();

    // Create output directories if they don't exist
    let traces_dir = path.join("spans");
    let metrics_dir = path.join("metrics");
    let logs_dir = path.join("logs");
    
    std::fs::create_dir_all(&traces_dir)
        .map_err(FileExporterError::Io)?;
    std::fs::create_dir_all(&metrics_dir)
        .map_err(FileExporterError::Io)?;
    std::fs::create_dir_all(&logs_dir)
        .map_err(FileExporterError::Io)?;

    info!("File exporter started, writing {} data to {}", format, path.display());

    match format.as_str() {
        "parquet" => {
            let exporter = ParquetExporter::new();
            run_export_loop_parquet(exporter, traces_dir, metrics_dir, logs_dir, traces_rx, metrics_rx, logs_rx, flush_interval, token).await
        }
        "json" => {
            let exporter = JsonExporter::new();
            run_export_loop_json(exporter, traces_dir, metrics_dir, logs_dir, traces_rx, metrics_rx, logs_rx, flush_interval, token).await
        }
        _ => Err(FileExporterError::Config(format!("Unsupported export format: {}", format)))
    }
}

/// Parquet export loop (typed row conversion).
#[allow(clippy::too_many_arguments)]
async fn run_export_loop_parquet(
    exporter: ParquetExporter,
    traces_dir: std::path::PathBuf,
    metrics_dir: std::path::PathBuf,
    logs_dir: std::path::PathBuf,
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,    
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,    
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,    
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<(), FileExporterError> {
    let file_ext = ".parquet";

    // Create mutable receivers for stream iteration
    let mut traces_rx = traces_rx;
    let mut metrics_rx = metrics_rx;
    let mut logs_rx = logs_rx;

    loop {
        tokio::select! {
            Some(traces) = traces_rx.next() => {
                for resource_spans in traces {
                    let span_rows = SpanRow::from_resource_spans(&resource_spans)?;
                    if !span_rows.is_empty() {
                        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
                        let file_path = traces_dir.join(format!("spans_{}{}", timestamp, file_ext));
                        exporter.export_span_rows(&span_rows, &file_path)?;
                    }
                }
            }
            Some(metrics) = metrics_rx.next() => {
                for resource_metrics in metrics {
                    let metric_rows = MetricRow::from_resource_metrics(&resource_metrics)?;
                    if !metric_rows.is_empty() {
                        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
                        let file_path = metrics_dir.join(format!("metrics_{}{}", timestamp, file_ext));
                        exporter.export_metric_rows(&metric_rows, &file_path)?;
                    }
                }
            }
            Some(logs) = logs_rx.next() => {
                for resource_logs in logs {
                    let log_rows = LogRecordRow::from_resource_logs(&resource_logs)?;
                    if !log_rows.is_empty() {
                        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
                        let file_path = logs_dir.join(format!("logs_{}{}", timestamp, file_ext));
                        exporter.export_log_record_rows(&log_rows, &file_path)?;
                    }
                }
            }
            _ = sleep(flush_interval) => {}
            _ = token.cancelled() => {
                info!("File exporter received shutdown signal");
                break;
            }
        }
    }

    info!("File exporter shutting down, performing final flush");
    Ok(())
}

/// JSON export loop (native OTLP JSON).
#[allow(clippy::too_many_arguments)]
async fn run_export_loop_json(
    exporter: JsonExporter,
    traces_dir: std::path::PathBuf,
    metrics_dir: std::path::PathBuf,
    logs_dir: std::path::PathBuf,
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,    
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,    
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,    
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<(), FileExporterError> {
    let file_ext = ".json";

    // Create mutable receivers for stream iteration
    let mut traces_rx = traces_rx;
    let mut metrics_rx = metrics_rx;
    let mut logs_rx = logs_rx;

    loop {
        tokio::select! {
            Some(traces) = traces_rx.next() => {
                if !traces.is_empty() {
                    let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
                    let file_path = traces_dir.join(format!("spans_{}{}", timestamp, file_ext));
                    exporter.export_traces(&traces, &file_path)?;
                }
            }
            Some(metrics) = metrics_rx.next() => {
                if !metrics.is_empty() {
                    let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
                    let file_path = metrics_dir.join(format!("metrics_{}{}", timestamp, file_ext));
                    exporter.export_metrics(&metrics, &file_path)?;
                }
            }
            Some(logs) = logs_rx.next() => {
                if !logs.is_empty() {
                    let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
                    let file_path = logs_dir.join(format!("logs_{}{}", timestamp, file_ext));
                    exporter.export_logs(&logs, &file_path)?;
                }
            }
            _ = sleep(flush_interval) => {}
            _ = token.cancelled() => {
                info!("File exporter received shutdown signal");
                break;
            }
        }
    }

    info!("File exporter shutting down, performing final flush");
    Ok(())
} 