use crate::bounded_channel::BoundedReceiver;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use crate::exporters::file::schema::{SpanRow, MetricRow, LogRecordRow};
use crate::exporters::file::{FileExporterError, config::FileExporterConfig, parquet::ParquetExporter};
use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{info, debug};
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
    mut traces_rx: BoundedReceiver<Vec<ResourceSpans>>,    
    mut metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,    
    mut logs_rx: BoundedReceiver<Vec<ResourceLogs>>,    
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<(), FileExporterError> {
    let file_ext = ".parquet";

    // ---------------------------------------------------------------------
    // In-memory buffers.  We accumulate until `flush_interval` elapses or a
    // shutdown signal arrives, then write ONE file per signal type.
    // ---------------------------------------------------------------------
    let mut span_buffer: Vec<SpanRow> = Vec::new();
    let mut metric_buffer: Vec<MetricRow> = Vec::new();
    let mut log_buffer: Vec<LogRecordRow> = Vec::new();

    // Helper that writes out any non-empty buffer and clears it afterwards
    let flush = |span_buf: &mut Vec<SpanRow>,
                 metric_buf: &mut Vec<MetricRow>,
                 log_buf: &mut Vec<LogRecordRow>| -> Result<(), FileExporterError> {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        if !span_buf.is_empty() {
            let rows = span_buf.len();
            let file_path = traces_dir.join(format!("spans_{}{}", timestamp, file_ext));
            exporter.export_span_rows(span_buf, &file_path)?;
            debug!(rows, path=%file_path.display(), "Flushed spans parquet file");
            span_buf.clear();
        }
        if !metric_buf.is_empty() {
            let rows = metric_buf.len();
            let file_path = metrics_dir.join(format!("metrics_{}{}", timestamp, file_ext));
            exporter.export_metric_rows(metric_buf, &file_path)?;
            debug!(rows, path=%file_path.display(), "Flushed metrics parquet file");
            metric_buf.clear();
        }
        if !log_buf.is_empty() {
            let rows = log_buf.len();
            let file_path = logs_dir.join(format!("logs_{}{}", timestamp, file_ext));
            exporter.export_log_record_rows(log_buf, &file_path)?;
            debug!(rows, path=%file_path.display(), "Flushed logs parquet file");
            log_buf.clear();
        }
        Ok(())
    };

    let mut flush_timer = tokio::time::interval(flush_interval);
    loop {
        tokio::select! {
            Some(traces) = traces_rx.next() => {
                for resource_spans in traces {
                    let mut span_rows = SpanRow::from_resource_spans(&resource_spans)?;
                    span_buffer.append(&mut span_rows);
                }
            }
            Some(metrics) = metrics_rx.next() => {
                for resource_metrics in metrics {
                    let mut metric_rows = MetricRow::from_resource_metrics(&resource_metrics)?;
                    metric_buffer.append(&mut metric_rows);
                }
            }
            Some(logs) = logs_rx.next() => {
                for resource_logs in logs {
                    let mut log_rows = LogRecordRow::from_resource_logs(&resource_logs)?;
                    log_buffer.append(&mut log_rows);
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

/// JSON export loop (native OTLP JSON).
#[allow(clippy::too_many_arguments)]
async fn run_export_loop_json(
    exporter: JsonExporter,
    traces_dir: std::path::PathBuf,
    metrics_dir: std::path::PathBuf,
    logs_dir: std::path::PathBuf,
    mut traces_rx: BoundedReceiver<Vec<ResourceSpans>>,    
    mut metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,    
    mut logs_rx: BoundedReceiver<Vec<ResourceLogs>>,    
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<(), FileExporterError> {
    let file_ext = ".json";

    // Buffers
    let mut trace_buffer: Vec<ResourceSpans> = Vec::new();
    let mut metric_buffer: Vec<ResourceMetrics> = Vec::new();
    let mut log_buffer: Vec<ResourceLogs> = Vec::new();

    // Flush helper
    let flush = |tr_buf: &mut Vec<ResourceSpans>,
                 met_buf: &mut Vec<ResourceMetrics>,
                 log_buf: &mut Vec<ResourceLogs>| -> Result<(), FileExporterError> {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        if !tr_buf.is_empty() {
            let items = tr_buf.len();
            let file_path = traces_dir.join(format!("spans_{}{}", timestamp, file_ext));
            exporter.export_traces(tr_buf, &file_path)?;
            debug!(items, path=%file_path.display(), "Flushed spans JSON file");
            tr_buf.clear();
        }
        if !met_buf.is_empty() {
            let items = met_buf.len();
            let file_path = metrics_dir.join(format!("metrics_{}{}", timestamp, file_ext));
            exporter.export_metrics(met_buf, &file_path)?;
            debug!(items, path=%file_path.display(), "Flushed metrics JSON file");
            met_buf.clear();
        }
        if !log_buf.is_empty() {
            let items = log_buf.len();
            let file_path = logs_dir.join(format!("logs_{}{}", timestamp, file_ext));
            exporter.export_logs(log_buf, &file_path)?;
            debug!(items, path=%file_path.display(), "Flushed logs JSON file");
            log_buf.clear();
        }
        Ok(())
    };

    let mut flush_timer = tokio::time::interval(flush_interval);
    loop {
        tokio::select! {
            Some(traces) = traces_rx.next() => {
                trace_buffer.extend(traces);
            }
            Some(metrics) = metrics_rx.next() => {
                metric_buffer.extend(metrics);
            }
            Some(logs) = logs_rx.next() => {
                log_buffer.extend(logs);
            }
            _ = flush_timer.tick() => {
                flush(&mut trace_buffer, &mut metric_buffer, &mut log_buffer)?;
            }
            _ = token.cancelled() => {
                info!("File exporter received shutdown signal");
                flush(&mut trace_buffer, &mut metric_buffer, &mut log_buffer)?;
                break;
            }
        }
    }

    info!("File exporter shutting down, flush complete");
    Ok(())
} 