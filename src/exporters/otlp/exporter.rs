// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::config::AwsConfig;
/// Provides functionality for exporting telemetry data via OTLP protocol
///
/// This module contains the core OTLP exporter implementation that handles:
/// - Concurrent request processing and encoding
/// - Retry and timeout policy enforcement
/// - Graceful shutdown and request draining
/// - Support for both metrics and traces telemetry
///
use crate::bounded_channel::BoundedReceiver;
use crate::exporters::http::acknowledger::{Acknowledger, DefaultAcknowledger};
use crate::exporters::http::response::Response as HttpResponse;
use crate::exporters::http::retry::RetryPolicy;
use crate::exporters::otlp::client::OTLPClient;
use crate::exporters::otlp::config::{
    OTLPExporterLogsConfig, OTLPExporterMetricsConfig, OTLPExporterTracesConfig,
};
use crate::exporters::otlp::payload::OtlpPayload;
use crate::exporters::otlp::request::RequestBuilder;
use crate::exporters::otlp::signer::{
    AwsSigv4RequestSigner, AwsSigv4RequestSignerBuilder, RequestSigner,
};
use crate::exporters::otlp::{Authenticator, errors, get_meter, request};
use crate::telemetry::RotelCounter;
use crate::topology::batch::BatchSizer;
use crate::topology::flush_control::{FlushReceiver, conditional_flush};
use crate::topology::payload::{Ack, Message, MessageMetadata, OTLPFrom};
use futures::stream::FuturesUnordered;
use http::Request;
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
use tokio::sync::broadcast::Sender;
use tokio::task::JoinError;
use tokio::time::{Instant, timeout_at};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower::retry::{Retry, RetryLayer};
use tower::timeout::{Timeout, TimeoutLayer};
use tower::{BoxError, Service, ServiceBuilder};
use tracing::{debug, error, warn};

const MAX_CONCURRENT_REQUESTS: usize = 10;
const MAX_CONCURRENT_ENCODERS: usize = 20;

#[rustfmt::skip]
type ExportFuture<T> = Pin<Box<dyn Future<Output = (Result<T, BoxError>, Option<Vec<MessageMetadata>>)> + Send>>;
#[rustfmt::skip]
type EncodingFuture = Pin<Box<dyn Future<Output = Result<Result<Request<OtlpPayload>, errors::ExporterError>, JoinError>> + Send>>;

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
    trace_rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
    flush_receiver: Option<FlushReceiver>,
) -> Result<
    Exporter<
        ResourceSpans,
        ExportTraceServiceRequest,
        AwsSigv4RequestSigner,
        ExportTraceServiceResponse,
    >,
    Box<dyn Error + Send + Sync>,
> {
    let sent = get_meter()
        .u64_counter("rotel_exporter_sent_spans")
        .with_description("Number of spans successfully exported")
        .with_unit("spans")
        .build();
    let send_failed = get_meter()
        .u64_counter("rotel_exporter_send_failed_spans")
        .with_description("Number of spans that could not be exported")
        .with_unit("spans")
        .build();
    let sent = RotelCounter::OTELCounter(sent);
    let send_failed = RotelCounter::OTELCounter(send_failed);
    let tls_config = traces_config.tls_cfg_builder.clone().build()?;
    let client = OTLPClient::new(
        tls_config,
        traces_config.protocol.clone(),
        sent,
        send_failed.clone(),
    )?;
    let retry_policy = RetryPolicy::new(
        traces_config.retry_config.clone(),
        Some(errors::is_retryable_error),
    );
    let retry_broadcast = retry_policy.retry_broadcast();

    let signer = get_signer_builder(&traces_config);
    let req_builder = request::build_traces(&traces_config, send_failed, signer)?;

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
        flush_receiver,
        retry_broadcast,
    ))
}

fn get_signer_builder(cfg: &OTLPExporterTracesConfig) -> Option<AwsSigv4RequestSignerBuilder> {
    match cfg.authenticator {
        Some(Authenticator::Sigv4auth) => {
            let cfg = AwsConfig::from_env();
            Some(AwsSigv4RequestSignerBuilder::new(cfg))
        }
        _ => None,
    }
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
    metrics_rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
    flush_receiver: Option<FlushReceiver>,
) -> Result<
    Exporter<
        ResourceMetrics,
        ExportMetricsServiceRequest,
        AwsSigv4RequestSigner,
        ExportMetricsServiceResponse,
    >,
    Box<dyn Error + Send + Sync>,
> {
    let sent = get_meter()
        .u64_counter("rotel_exporter_sent_metric_points")
        .with_description("Number of metrics points successfully exported")
        .with_unit("metric_points")
        .build();
    let send_failed = get_meter()
        .u64_counter("rotel_exporter_send_failed_metric_points")
        .with_description("Number of metric points that could not be exported")
        .with_unit("metric_points")
        .build();
    let sent = RotelCounter::OTELCounter(sent);
    let send_failed = RotelCounter::OTELCounter(send_failed);
    _build_metrics_exporter(
        sent,
        send_failed,
        metrics_config,
        metrics_rx,
        flush_receiver,
    )
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
    logs_rx: BoundedReceiver<Vec<Message<ResourceLogs>>>,
    flush_receiver: Option<FlushReceiver>,
) -> Result<
    Exporter<
        ResourceLogs,
        ExportLogsServiceRequest,
        AwsSigv4RequestSigner,
        ExportLogsServiceResponse,
    >,
    Box<dyn Error + Send + Sync>,
> {
    let sent = get_meter()
        .u64_counter("rotel_exporter_sent_log_records")
        .with_description("Number of log records successfully exported")
        .with_unit("log_records")
        .build();
    let send_failed = get_meter()
        .u64_counter("rotel_exporter_send_failed_log_records")
        .with_description("Number of log records that could not be exported")
        .with_unit("log_records")
        .build();
    let sent = RotelCounter::OTELCounter(sent);
    let send_failed = RotelCounter::OTELCounter(send_failed);
    let tls_config = logs_config.tls_cfg_builder.clone().build()?;
    let client = OTLPClient::new(
        tls_config,
        logs_config.protocol.clone(),
        sent,
        send_failed.clone(),
    )?;

    let retry_policy = RetryPolicy::new(
        logs_config.retry_config.clone(),
        Some(errors::is_retryable_error),
    );
    let retry_broadcast = retry_policy.retry_broadcast();

    let signer_builder = get_signer_builder(&logs_config);
    let req_builder = request::build_logs(&logs_config, send_failed, signer_builder)?;

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
        flush_receiver,
        retry_broadcast,
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
    metrics_rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
    flush_receiver: Option<FlushReceiver>,
) -> Result<
    Exporter<
        ResourceMetrics,
        ExportMetricsServiceRequest,
        AwsSigv4RequestSigner,
        ExportMetricsServiceResponse,
    >,
    Box<dyn Error + Send + Sync>,
> {
    let sent = RotelCounter::NoOpCounter;
    let send_failed = RotelCounter::NoOpCounter;
    _build_metrics_exporter(
        sent,
        send_failed,
        metrics_config,
        metrics_rx,
        flush_receiver,
    )
}

fn _build_metrics_exporter(
    sent: RotelCounter<u64>,
    send_failed: RotelCounter<u64>,
    metrics_config: OTLPExporterMetricsConfig,
    metrics_rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
    flush_receiver: Option<FlushReceiver>,
) -> Result<
    Exporter<
        ResourceMetrics,
        ExportMetricsServiceRequest,
        AwsSigv4RequestSigner,
        ExportMetricsServiceResponse,
    >,
    Box<dyn Error + Send + Sync>,
> {
    let tls_config = metrics_config.tls_cfg_builder.clone().build()?;
    let client = OTLPClient::new(
        tls_config,
        metrics_config.protocol.clone(),
        sent,
        send_failed.clone(),
    )?;
    let retry_policy = RetryPolicy::new(
        metrics_config.retry_config.clone(),
        Some(errors::is_retryable_error),
    );
    let retry_broadcast = retry_policy.retry_broadcast();

    let signer_builder = get_signer_builder(&metrics_config);
    let req_builder = request::build_metrics(&metrics_config, send_failed, signer_builder)?;

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
        flush_receiver,
        retry_broadcast,
    ))
}

/// Main exporter struct that handles processing and sending telemetry data
///
/// # Type Parameters
/// * `Resource` - The telemetry resource type (spans or metrics)
/// * `Request` - The OTLP request type
/// * `Response` - The OTLP response type
pub struct Exporter<Resource, Request, Signer, Response>
where
    Response: prost::Message + std::fmt::Debug + Default + Send + 'static,
    Resource: prost::Message + std::fmt::Debug + Send + 'static,
    Request: prost::Message + OTLPFrom<Vec<Resource>> + 'static,
    Signer: Clone,
{
    type_name: String,
    rx: BoundedReceiver<Vec<Message<Resource>>>,
    req_builder: RequestBuilder<Request, Signer>,
    encoding_futures: FuturesUnordered<EncodingFuture>,
    export_futures: FuturesUnordered<ExportFuture<HttpResponse<Response>>>,
    svc: Retry<RetryPolicy<Response>, Timeout<OTLPClient<Response>>>,
    acknowledger: DefaultAcknowledger,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
    flush_receiver: Option<FlushReceiver>,
    retry_broadcast: Sender<bool>,
}

impl<Resource, Request, Signer, Response> Exporter<Resource, Request, Signer, Response>
where
    Response: prost::Message + std::fmt::Debug + Clone + Default + Send + 'static,
    Resource: prost::Message + std::fmt::Debug + Clone + Send + 'static,
    Request: prost::Message + OTLPFrom<Vec<Resource>> + Clone + 'static,
    [Resource]: BatchSizer,
    Signer: RequestSigner + Clone + Send + 'static,
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
        req_builder: RequestBuilder<Request, Signer>,
        rx: BoundedReceiver<Vec<Message<Resource>>>,
        encode_drain_max_time: Duration,
        export_drain_max_time: Duration,
        flush_receiver: Option<FlushReceiver>,
        retry_broadcast: Sender<bool>,
    ) -> Self {
        Self {
            type_name,
            rx,
            req_builder,
            svc,
            acknowledger: DefaultAcknowledger::default(),
            encoding_futures: FuturesUnordered::new(),
            export_futures: FuturesUnordered::new(),
            encode_drain_max_time,
            export_drain_max_time,
            flush_receiver,
            retry_broadcast,
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
        let mut flush_receiver = self.flush_receiver.take();
        loop {
            select! {
                biased;

                Some(resp) = self.export_futures.next() => {
                    let (result, metadata) = resp;

                    // First acknowledge with a reference if successful
                    if let Ok(ref response) = result {
                        self.acknowledger.acknowledge(response).await;
                    }

                    // Then pass ownership to logging/finalization
                    self.log_if_failed((result, metadata));
                },

                Some(v) = self.encoding_futures.next(), if self.export_futures.len() < MAX_CONCURRENT_REQUESTS => {
                    match v {
                        Ok(encoded_request) => {
                            match encoded_request {
                                Ok(encoded_req) => {
                                    // The client will handle metadata extraction and attachment to response
                                    let fut = self.svc.call(encoded_req);
                                    let wrapped_fut = async move {
                                        let result = fut.await;
                                        (result, None) // Client handles metadata, don't pass it separately
                                    };
                                    self.export_futures.push(Box::pin(wrapped_fut));
                                },
                                Err(e) => {
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
                        Some(messages) => {
                            debug!(exporter_type =self.type_name.to_string(),
                                "Got a request with {} messages", messages.len());

                                // Extract payloads and metadata
                                let mut payloads = Vec::new();
                                let mut all_metadata = Vec::new();

                                for message in messages {
                                    if let Some(metadata) = message.metadata {
                                        all_metadata.push(metadata);
                                    }
                                    payloads.extend(message.payload);
                                }

                                let metadata = if all_metadata.is_empty() {
                                    None
                                } else {
                                    Some(all_metadata)
                                };

                                let req_builder = self.req_builder.clone();
                                let f = tokio::task::spawn_blocking(move || {
                                    let size = BatchSizer::size_of(payloads.as_slice());
                                    req_builder.encode(Request::otlp_from(payloads), size, metadata)
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

                Some(resp) = conditional_flush(&mut flush_receiver) => {
                    match resp {
                        (Some(req), listener) => {
                            debug!(exporter_type = type_name, "received force flush in OTLP exporter: {:?}", req);

                            if let Err(res) = self.drain_futures().await {
                                warn!(exporter_type = type_name, "unable to drain exporter: {}", res);
                            }

                            if let Err(e) = listener.ack(req).await {
                                warn!(exporter_type = type_name, "unable to ack flush request: {}", e);
                            }
                        },
                        (None, _) => warn!("flush channel was closed")
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

    /// Drains in-flight requests during shutdown or on a forced flush
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
                    .into());
                }
                Ok(res) => match res {
                    None => {
                        error!(type_name, "None returned while polling encoding futures.");
                        break;
                    }
                    Some(r) => match r {
                        Ok(Ok(encoded_req)) => {
                            // we could exceed the previous limit on export_futures here?
                            // The client will handle metadata extraction and attachment to response
                            let fut = self.svc.call(encoded_req);
                            let wrapped_fut = async move {
                                let result = fut.await;
                                (result, None) // Client handles metadata, don't pass it separately
                            };
                            self.export_futures.push(Box::pin(wrapped_fut));
                        }
                        Err(e) => {
                            return Err(format!(
                                "OTLPExporter, {} JoinError on encoding future: {:?}",
                                type_name, e
                            )
                            .into());
                        }
                        Ok(Err(e)) => {
                            return Err(format!(
                                "OTLPExporter, {} encoding error: {:?}",
                                type_name, e
                            )
                            .into());
                        }
                    },
                },
            }
        }

        // We ignore the error, since there may be no listeners
        let _ = self.retry_broadcast.send(true);

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
                    .into());
                }
                Ok(res) => match res {
                    None => {
                        error!(type_name, "None returned while polling futures");
                        break;
                    }
                    Some(r) => {
                        let (result, metadata) = r;

                        // First acknowledge with a reference if successful
                        if let Ok(ref response) = result {
                            // For drain, spawn acknowledgment to avoid blocking
                            let response_metadata = response.metadata().clone();
                            tokio::spawn(async move {
                                if let Some(mut metadata_vec) = response_metadata {
                                    // Acknowledge metadata directly without creating fake response
                                    for metadata in metadata_vec.iter_mut() {
                                        if let Err(e) = metadata.ack().await {
                                            tracing::warn!(
                                                "Failed to acknowledge OTLP message: {:?}",
                                                e
                                            );
                                        }
                                    }
                                }
                            });
                        }

                        // Then pass ownership to logging/finalization
                        if self.log_if_failed((result, metadata)) {
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

    // Log if the request failed and return true, otherwise return false
    fn log_if_failed(
        &self,
        resp: (
            Result<HttpResponse<Response>, BoxError>,
            Option<Vec<MessageMetadata>>,
        ),
    ) -> bool {
        let (res, _metadata) = resp; // metadata is now in the response, not the tuple
        let type_name = self.type_name.to_string();

        match res {
            Ok(r) => match r {
                HttpResponse::Http(parts, _, _) => match parts.status.as_u16() {
                    200..=202 => return false,
                    _ => {
                        error!(
                            type_name,
                            status = parts.status.as_u16(),
                            "OTLPExporter failed HTTP status code from endpoint."
                        );
                        return true;
                    }
                },
                HttpResponse::Grpc(status, _, _) => {
                    if status.code() == tonic::Code::Ok {
                        return false;
                    }
                    error!(type_name,
                        status = ?status.code(),
                        "OTLPExporter failed gRPC status code from endpoint.");
                    return true;
                }
            },
            Err(e) => {
                error!(type_name,
                    error = ?e,
                    "OTLPExporter error from endpoint."
                );
                return true;
            }
        }
    }
}
