// SPDX-License-Identifier: Apache-2.0

/// Provides functionality for exporting telemetry data via OTLP protocol
///
/// This module contains the core OTLP exporter implementation that handles:
/// - Concurrent request processing and encoding
/// - Retry and timeout policy enforcement
/// - Graceful shutdown and request draining
/// - Support for both metrics and traces telemetry
///
use crate::bounded_channel::BoundedReceiver;
use crate::exporters::otlp::client::OTLPClient;
use crate::exporters::otlp::config::{
    OTLPExporterLogsConfig, OTLPExporterMetricsConfig, OTLPExporterTracesConfig,
};
use crate::exporters::otlp::request::{EncodedRequest, RequestBuilder};
use crate::exporters::otlp::{errors, get_meter, request};
use crate::exporters::retry::RetryPolicy;
use crate::telemetry::RotelCounter;
use crate::topology::batch::BatchSizer;
use crate::topology::payload::OTLPFrom;
use futures::stream::FuturesUnordered;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::error::Error;
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinError;
use tokio::time::{timeout_at, Instant};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower::retry::{Retry, RetryLayer};
use tower::timeout::{Timeout, TimeoutLayer};
use tower::{BoxError, Service, ServiceBuilder};
use tracing::{debug, error};


const MAX_CONCURRENT_REQUESTS: usize = 10;
const MAX_CONCURRENT_ENCODERS: usize = 20;

#[rustfmt::skip]
type ExportFuture<T> = Pin<Box<dyn Future<Output = Result<T, BoxError>> + Send>>;
#[rustfmt::skip]
type EncodingFuture = Pin<Box<dyn Future<Output = Result<Result<EncodedRequest, errors::ExporterError>, JoinError>> + Send>>;

/// Creates a configured OTLP traces exporter
///
/// # Arguments
/// * `traces_config` - Configuration for the traces exporter
/// * `trace_rx` - Channel receiver for incoming trace data
///
/// # Returns
/// Configured Exporter instance for trace telemetry
pub fn build_traces_exporter(
    traces_config: OTLPExporterTracesConfig,
    trace_rx: BoundedReceiver<Vec<ResourceSpans>>,
) -> Result<
    Exporter<ResourceSpans, ExportTraceServiceRequest, ExportTraceServiceResponse>,
    Box<dyn Error + Send + Sync>,
> {
    let sent = get_meter().u64_counter("rotel_exporter_sent_spans").build();
    let send_failed = get_meter()
        .u64_counter("rotel_exporter_send_failed_spans")
        .build();
    let sent = RotelCounter::OTELCounter(sent);
    let send_failed = RotelCounter::OTELCounter(send_failed);
    let tls_config = traces_config.tls_cfg_builder.clone().build()?;
    let client = OTLPClient::new(
        tls_config,
        traces_config.protocol.clone(),
        sent,
        send_failed,
    )?;
    let retry_policy = RetryPolicy::new(
        traces_config.retry_config.clone(),
        errors::is_retryable_error,
    );

    let req_builder = request::build_traces(&traces_config)?;

    let svc = ServiceBuilder::new()
        .layer(RetryLayer::new(retry_policy))
        .layer(TimeoutLayer::new(traces_config.request_timeout))
        .service(client);
    Ok(Exporter::new(
        traces_config.type_name.clone(),
        svc,
        req_builder.clone(),
        trace_rx.clone(),
        traces_config.encode_drain_max_time,
        traces_config.export_drain_max_time,
    ))
}

/// Creates a configured OTLP metrics exporter
///
/// # Arguments
/// * `metrics_config` - Configuration for the metrics exporter  
/// * `metrics_rx` - Channel receiver for incoming metrics data
///
/// # Returns
/// Configured Exporter instance for metrics telemetry
pub fn build_metrics_exporter(
    metrics_config: OTLPExporterMetricsConfig,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
) -> Result<
    Exporter<ResourceMetrics, ExportMetricsServiceRequest, ExportMetricsServiceResponse>,
    Box<dyn Error + Send + Sync>,
> {
    let sent = get_meter()
        .u64_counter("rotel_exporter_sent_metric_points")
        .build();
    let send_failed = get_meter()
        .u64_counter("rotel_exporter_send_failed_metric_points")
        .build();
    let sent = RotelCounter::OTELCounter(sent);
    let send_failed = RotelCounter::OTELCounter(send_failed);
    _build_metrics_exporter(sent, send_failed, metrics_config, metrics_rx)
}

/// Creates a configured OTLP logs exporter
///
/// # Arguments
/// * `logs_config` - Configuration for the logs exporter  
/// * `logs_rx` - Channel receiver for incoming logs data
///
/// # Returns
/// Configured Exporter instance for logs telemetry
pub fn build_logs_exporter(
    logs_config: OTLPExporterLogsConfig,
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
) -> Result<
    Exporter<ResourceLogs, ExportLogsServiceRequest, ExportLogsServiceResponse>,
    Box<dyn Error + Send + Sync>,
> {
    let sent = get_meter()
        .u64_counter("rotel_exporter_sent_log_records")
        .build();
    let send_failed = get_meter()
        .u64_counter("rotel_exporter_send_failed_log_records")
        .build();
    let sent = RotelCounter::OTELCounter(sent);
    let send_failed = RotelCounter::OTELCounter(send_failed);
    let tls_config = logs_config.tls_cfg_builder.clone().build()?;
    let client = OTLPClient::new(tls_config, logs_config.protocol.clone(), sent, send_failed)?;
    let retry_policy =
        RetryPolicy::new(logs_config.retry_config.clone(), errors::is_retryable_error);

    let req_builder = request::build_logs(&logs_config)?;

    let svc = ServiceBuilder::new()
        .layer(RetryLayer::new(retry_policy))
        .layer(TimeoutLayer::new(logs_config.request_timeout))
        .service(client);
    Ok(Exporter::new(
        logs_config.type_name.clone(),
        svc,
        req_builder.clone(),
        logs_rx.clone(),
        logs_config.encode_drain_max_time,
        logs_config.export_drain_max_time,
    ))
}

/// Creates a configured OTLP metrics exporter
///
/// # Arguments
/// * `metrics_config` - Configuration for the metrics exporter
/// * `metrics_rx` - Channel receiver for incoming metrics data
///
/// # Returns
/// Configured Exporter instance for metrics telemetry
pub fn build_internal_metrics_exporter(
    metrics_config: OTLPExporterMetricsConfig,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
) -> Result<
    Exporter<ResourceMetrics, ExportMetricsServiceRequest, ExportMetricsServiceResponse>,
    Box<dyn Error + Send + Sync>,
> {
    let sent = RotelCounter::NoOpCounter;
    let send_failed = RotelCounter::NoOpCounter;
    _build_metrics_exporter(sent, send_failed, metrics_config, metrics_rx)
}

fn _build_metrics_exporter(
    sent: RotelCounter<u64>,
    send_failed: RotelCounter<u64>,
    metrics_config: OTLPExporterMetricsConfig,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
) -> Result<
    Exporter<ResourceMetrics, ExportMetricsServiceRequest, ExportMetricsServiceResponse>,
    Box<dyn Error + Send + Sync>,
> {
    let tls_config = metrics_config.tls_cfg_builder.clone().build()?;
    let client = OTLPClient::new(
        tls_config,
        metrics_config.protocol.clone(),
        sent,
        send_failed,
    )?;
    let retry_policy = RetryPolicy::new(
        metrics_config.retry_config.clone(),
        errors::is_retryable_error,
    );

    let req_builder = request::build_metrics(&metrics_config)?;

    let svc = ServiceBuilder::new()
        .layer(RetryLayer::new(retry_policy))
        .layer(TimeoutLayer::new(metrics_config.request_timeout))
        .service(client);
    Ok(Exporter::new(
        metrics_config.type_name.clone(),
        svc,
        req_builder.clone(),
        metrics_rx.clone(),
        metrics_config.encode_drain_max_time,
        metrics_config.export_drain_max_time,
    ))
}

/// Main exporter struct that handles processing and sending telemetry data
///
/// # Type Parameters
/// * `Resource` - The telemetry resource type (spans or metrics)
/// * `Request` - The OTLP request type
/// * `Response` - The OTLP response type
pub struct Exporter<Resource, Request, Response>
where
    Response: prost::Message + std::fmt::Debug + Default + Send + 'static,
    Resource: prost::Message + std::fmt::Debug + Send + 'static,
    Request: prost::Message + OTLPFrom<Vec<Resource>> + 'static,
{
    type_name: String,
    rx: BoundedReceiver<Vec<Resource>>,
    req_builder: RequestBuilder<Request>,
    encoding_futures: FuturesUnordered<EncodingFuture>,
    export_futures: FuturesUnordered<ExportFuture<Response>>,
    svc: Retry<RetryPolicy<Response>, Timeout<OTLPClient<Response>>>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
}

impl<Resource, Request, Response> Exporter<Resource, Request, Response>
where
    Response: prost::Message + std::fmt::Debug + Clone + Default + Send + 'static,
    Resource: prost::Message + std::fmt::Debug + Clone + Send + 'static,
    Request: prost::Message + OTLPFrom<Vec<Resource>> + Clone + 'static,
    [Resource]: BatchSizer,
{
    /// Creates a new Exporter instance
    ///
    /// # Arguments
    /// * `type_name` - Name identifying the exporter type
    /// * `svc` - The configured service stack with retry and timeout policies
    /// * `req_builder` - Builder for creating OTLP protocol requests
    /// * `rx` - Channel receiver for incoming telemetry data
    pub fn new(
        type_name: String,
        svc: Retry<RetryPolicy<Response>, Timeout<OTLPClient<Response>>>,
        req_builder: RequestBuilder<Request>,
        rx: BoundedReceiver<Vec<Resource>>,
        encode_drain_max_time: Duration,
        export_drain_max_time: Duration,
    ) -> Self {
        Self {
            type_name,
            rx,
            req_builder,
            svc,
            encoding_futures: FuturesUnordered::new(),
            export_futures: FuturesUnordered::new(),
            encode_drain_max_time,
            export_drain_max_time,
        }
    }

    /// Starts the exporter processing loop
    ///
    /// Handles concurrent processing of:
    /// - Receiving telemetry data
    /// - Encoding into OTLP format
    /// - Sending to endpoints with retries
    ///
    /// # Arguments
    /// * `token` - Cancellation token for graceful shutdown
    pub async fn start(
        &mut self,
        token: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let type_name = self.type_name.to_string();
        loop {
            select! {
                biased;

                Some(resp) = self.export_futures.next() => {
                    match resp {
                        Err(e) => {
                            error!(error = ?e, exporter_type = type_name, "Exporting failed, dropping data.")
                        },
                        Ok(rs) => {
                            debug!(exporter_type = type_name,
                                rs = ?rs,
                                futures_size = self.export_futures.len(),
                                "OTLPExporter sent response.");
                        }
                    }
                },

                Some(v) = self.encoding_futures.next(), if self.export_futures.len() < MAX_CONCURRENT_REQUESTS => {
                    match v {
                        Ok(encoded_request) => {
                            match encoded_request {
                                Ok(request) => {
                                    self.export_futures.push(Box::pin(self.svc.call(request)));
                                },
                                Err(e) => {
                                    // TODO
                                    // Add metric for encoding error
                                    error!(exporter_type = type_name,
                                        "Failed to encode OTLP request into Request<Full<Bytes>> {:?}", e);
                                }
                            }
                        },
                        Err(e) => {
                            error!(exporter_type = type_name,
                                "Error processing encoding futures {}", e.to_string());
                            break
                        },
                    }
                },

                req = self.rx.next(), if self.encoding_futures.len() < MAX_CONCURRENT_ENCODERS => {
                    match req {
                        Some(payload) => {
                            debug!(exporter_type =self.type_name.to_string(),
                                "Got a request {:?}", payload);
                                let req_builder = self.req_builder.clone();
                                let f = tokio::task::spawn_blocking(move || {
                                    let size = BatchSizer::size_of(payload.as_slice());
                                    req_builder.encode(Request::otlp_from(payload), size)
                                });
                                self.encoding_futures.push(Box::pin(f));
                        },
                        None => {
                            debug!(exporter_type = type_name,
                                "OTLPExporter receiver has closed, exiting main processing loop.");
                            break
                        }
                    }
                },

                _ = token.cancelled() => {
                    debug!(
                        exporter_type = type_name,
                        "OTLPExporter received shutdown signal, exiting main processing loop.");
                    break
                },
            }
        }

        self.drain_futures().await
    }

    /// Drains in-flight requests during shutdown
    ///
    /// Attempts to complete processing of:
    /// - Requests being encoded
    /// - Requests being sent to endpoints
    ///
    /// Times out after configured durations to prevent hanging
    async fn drain_futures(&mut self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let finish_encoding = Instant::now().add(self.encode_drain_max_time);
        let finish_sending = Instant::now().add(self.export_drain_max_time);
        let type_name = self.type_name.to_string();

        // First we must wait on currently encoding futures
        loop {
            if self.encoding_futures.is_empty() {
                break;
            }

            let poll_res = timeout_at(finish_encoding, self.encoding_futures.next()).await;
            match poll_res {
                Err(_) => {
                    return Err(format!(
                        "OTLPExporter, {} timed out waiting for requests to encode",
                        type_name,
                    )
                    .into())
                }
                Ok(res) => match res {
                    None => {
                        error!(type_name, "None returned while polling encoding futures.");
                        break;
                    }
                    Some(r) => match r {
                        Ok(Ok(r)) => {
                            // we could exceed the previous limit on export_futures here?
                            self.export_futures.push(Box::pin(self.svc.call(r)));
                        }
                        Err(e) => {
                            return Err(format!(
                                "OTLPExporter, {} JoinError on encoding future: {:?}",
                                type_name, e
                            )
                            .into())
                        }
                        Ok(Err(e)) => {
                            return Err(format!(
                                "OTLPExporter, {} encoding error: {:?}",
                                type_name, e
                            )
                            .into())
                        }
                    },
                },
            }
        }

        let mut drain_errors = 0;
        loop {
            if self.export_futures.is_empty() {
                break;
            }

            let poll_res = timeout_at(finish_sending, self.export_futures.next()).await;
            match poll_res {
                Err(_) => {
                    return Err(format!(
                        "OTLPExporter, {} timed out waiting for requests to finish",
                        type_name
                    )
                    .into())
                }
                Ok(res) => match res {
                    None => {
                        error!(type_name, "None returned while polling futures");
                        break;
                    }
                    Some(r) => {
                        if let Err(e) = r {
                            error!(type_name,
                                error = ?e,
                                "OTLPExporter error from endpoint."
                            );

                            drain_errors += 1;
                        }
                    }
                },
            }
        }

        if drain_errors > 0 {
            Err(format!(
                "Failed draining export requests, {} requests failed",
                drain_errors
            )
            .into())
        } else {
            Ok(())
        }
    }
}
