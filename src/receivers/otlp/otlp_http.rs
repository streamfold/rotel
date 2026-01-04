// SPDX-License-Identifier: Apache-2.0

use crate::receivers::otlp_output::OTLPOutput;
use flate2::read::GzDecoder;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderValue, Method};
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::EncodeError;
use read_restrict::ReadExt;
use std::error::Error as StdError;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io::{ErrorKind, Read};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::listener::Listener;
use crate::receivers::get_meter;
use crate::topology::batch::BatchSizer;
use crate::topology::payload::{HttpMetadata, Message, OTLPInto};
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tower::{Service, ServiceBuilder};
use tower_http::compression::{Compression, CompressionLayer};
use tower_http::limit::{RequestBodyLimit, RequestBodyLimitLayer};
use tower_http::trace::{HttpMakeClassifier, Trace, TraceLayer};
use tower_http::validate_request::{
    ValidateRequest, ValidateRequestHeader, ValidateRequestHeaderLayer,
};

// 20MiB matches collector limit:
// https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/confighttp/README.md
const MAX_BODY_SIZE: usize = 20 * 1024 * 1024;

const DEFAULT_HEADER_TIMEOUT: Duration = Duration::from_secs(30);

const PROTOBUF_CT: &str = "application/x-protobuf";
const JSON_CT: &str = "application/json";

#[derive(Default)]
pub struct OTLPHttpServerBuilder {
    trace_output: Option<OTLPOutput<Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Message<ResourceLogs>>>,
    traces_path: String,
    metrics_path: String,
    logs_path: String,
    header_timeout: Option<Duration>,
    include_metadata: bool,
    headers_to_include: Vec<String>,
}

impl OTLPHttpServerBuilder {
    pub fn with_traces_output(
        mut self,
        output: Option<OTLPOutput<Message<ResourceSpans>>>,
    ) -> OTLPHttpServerBuilder {
        self.trace_output = output;
        self
    }
    pub fn with_metrics_output(
        mut self,
        output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    ) -> OTLPHttpServerBuilder {
        self.metrics_output = output;
        self
    }
    pub fn with_logs_output(
        mut self,
        output: Option<OTLPOutput<Message<ResourceLogs>>>,
    ) -> OTLPHttpServerBuilder {
        self.logs_output = output;
        self
    }
    pub fn with_traces_path(mut self, value: String) -> OTLPHttpServerBuilder {
        self.traces_path = value;
        self
    }
    pub fn with_metrics_path(mut self, value: String) -> OTLPHttpServerBuilder {
        self.metrics_path = value;
        self
    }
    pub fn with_logs_path(mut self, value: String) -> OTLPHttpServerBuilder {
        self.logs_path = value;
        self
    }
    pub fn with_header_timeout(self, header_timeout: Duration) -> Self {
        Self {
            header_timeout: Some(header_timeout),
            ..self
        }
    }

    pub fn with_include_metadata(mut self, include: bool) -> Self {
        self.include_metadata = include;
        self
    }

    pub fn with_headers_to_include(mut self, headers: Vec<String>) -> Self {
        self.headers_to_include = headers;
        self
    }

    pub fn build(self) -> OTLPHttpServer {
        OTLPHttpServer {
            trace_output: self.trace_output,
            metrics_output: self.metrics_output,
            logs_output: self.logs_output,
            header_timeout: self.header_timeout.unwrap_or(DEFAULT_HEADER_TIMEOUT),
            traces_path: self.traces_path,
            metrics_path: self.metrics_path,
            logs_path: self.logs_path,
            include_metadata: self.include_metadata,
            headers_to_include: self.headers_to_include,
        }
    }
}

pub struct OTLPHttpServer {
    pub trace_output: Option<OTLPOutput<Message<ResourceSpans>>>,
    pub metrics_output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    pub logs_output: Option<OTLPOutput<Message<ResourceLogs>>>,
    pub traces_path: String,
    pub metrics_path: String,
    pub logs_path: String,
    header_timeout: Duration,
    include_metadata: bool,
    headers_to_include: Vec<String>,
}

impl OTLPHttpServer {
    pub fn builder() -> OTLPHttpServerBuilder {
        Default::default()
    }

    pub async fn serve(
        &self,
        listener: Listener,
        cancellation: CancellationToken,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let svc = build_service(
            self.trace_output.clone(),
            self.metrics_output.clone(),
            self.logs_output.clone(),
            self.traces_path.clone(),
            self.metrics_path.clone(),
            self.logs_path.clone(),
            self.include_metadata,
            self.headers_to_include.clone(),
        );

        // To bridge Tower->Hyper we must wrap the tower service
        let svc = TowerToHyperService::new(svc);

        let timer = hyper_util::rt::TokioTimer::new();
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();

        let mut builder = Builder::new(TokioExecutor::new());
        builder
            .http1()
            .header_read_timeout(Some(self.header_timeout))
            .timer(timer.clone());
        builder.http2().timer(timer);

        let listener = listener.into_async()?;
        // We start a loop to continuously accept incoming connections
        loop {
            let stream = tokio::select! {
                r = listener.accept() => {
                    match r {
                        Ok((stream, _)) => stream,
                        Err(e) => return Err(e.into()),
                    }
                },
                _ = cancellation.cancelled() => break
            };

            let io = TokioIo::new(stream);

            let conn = builder.serve_connection(io, svc.clone());
            let fut = graceful.watch(conn.into_owned());

            tokio::spawn(async move {
                let _ = fut.await.map_err(|e| {
                    if let Some(hyper_err) = e.downcast_ref::<hyper::Error>() {
                        // xxx: is there any way to get the error kind?
                        let err_str = format!("{:?}", hyper_err);

                        // This may imply a client shutdown race: https://github.com/hyperium/hyper/issues/3775
                        let err_not_connected = err_str.contains("NotConnected");
                        // There is no idle timeout, so header timeout is hit first
                        let err_hdr_timeout = err_str.contains("HeaderTimeout");

                        if !err_not_connected && !err_hdr_timeout {
                            error!("error serving connection: {:?}", hyper_err);
                        }
                    } else {
                        error!("error serving connection: {:?}", e);
                    }
                });
            });
        }

        // gracefully shutdown existing connections
        graceful.shutdown().await;

        Ok(())
    }
}

#[derive(Clone, Default)]
struct ValidateOTLPContentType {
    traces_path: String,
    metrics_path: String,
}

impl<B> ValidateRequest<B> for ValidateOTLPContentType {
    type ResponseBody = Full<Bytes>;

    fn validate(&mut self, request: &mut Request<B>) -> Result<(), Response<Self::ResponseBody>> {
        // This is a bit odd, but only validate requests that are valid paths. We'd prefer
        // to return a 404 instead of a 400.
        if request.method() != Method::POST
            || (request.uri().path() != self.traces_path
                && request.uri().path() != self.metrics_path)
        {
            return Ok(());
        }

        let ct = request.headers().get(CONTENT_TYPE);
        if ct
            .is_none_or(|ct| ct.to_str().unwrap() != PROTOBUF_CT && ct.to_str().unwrap() != JSON_CT)
        {
            debug!(content_type = ?ct, "Unsupported content-type");
            Err(response_4xx(StatusCode::BAD_REQUEST).unwrap())
        } else {
            Ok(())
        }
    }
}

fn build_service(
    trace_output: Option<OTLPOutput<Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Message<ResourceLogs>>>,
    traces_path: String,
    metrics_path: String,
    logs_path: String,
    include_metadata: bool,
    headers_to_include: Vec<String>,
) -> Trace<
    RequestBodyLimit<Compression<ValidateRequestHeader<OTLPService, ValidateOTLPContentType>>>,
    HttpMakeClassifier,
> {
    ServiceBuilder::new()
        // Log requests at debug level
        .layer(TraceLayer::new_for_http())
        // Limit incoming body size
        .layer(RequestBodyLimitLayer::new(MAX_BODY_SIZE))
        // Compress responses with gzip, if accept-encoding set
        .layer(CompressionLayer::new())
        // Only supports protobuf content-type
        .layer(ValidateRequestHeaderLayer::custom(
            ValidateOTLPContentType {
                traces_path: traces_path.clone(),
                metrics_path: metrics_path.clone(),
            },
        ))
        .service(OTLPService::new(
            trace_output,
            metrics_output,
            logs_output,
            traces_path,
            metrics_path,
            logs_path,
            include_metadata,
            headers_to_include,
        ))
}

#[derive(Clone)]
struct OTLPService {
    trace_output: Option<OTLPOutput<Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Message<ResourceLogs>>>,
    traces_path: String,
    metrics_path: String,
    logs_path: String,
    include_metadata: bool,
    headers_to_include: Vec<String>,
    accepted_spans_records_counter: Counter<u64>,
    accepted_metric_points_counter: Counter<u64>,
    accepted_log_records_counter: Counter<u64>,
    refused_spans_records_counter: Counter<u64>,
    refused_metric_points_counter: Counter<u64>,
    refused_log_records_counter: Counter<u64>,
    tags: [KeyValue; 1],
}

impl OTLPService {
    fn new(
        trace_output: Option<OTLPOutput<Message<ResourceSpans>>>,
        metrics_output: Option<OTLPOutput<Message<ResourceMetrics>>>,
        logs_output: Option<OTLPOutput<Message<ResourceLogs>>>,
        traces_path: String,
        metrics_path: String,
        logs_path: String,
        include_metadata: bool,
        headers_to_include: Vec<String>,
    ) -> Self {
        // Compute this once

        Self {
            trace_output,
            metrics_output,
            logs_output,
            traces_path,
            metrics_path,
            logs_path,
            include_metadata,
            headers_to_include,
            accepted_spans_records_counter: get_meter()
                .u64_counter("rotel_receiver_accepted_spans")
                .with_description(
                    "Number of spans successfully ingested and pushed into the pipeline",
                )
                .with_unit("spans")
                .build(),
            accepted_metric_points_counter: get_meter()
                .u64_counter("rotel_receiver_accepted_metric_points")
                .with_description(
                    "Number of metric points successfully ingested and pushed into the pipeline.",
                )
                .with_unit("metric_points")
                .build(),
            accepted_log_records_counter: get_meter()
                .u64_counter("rotel_receiver_accepted_log_records")
                .with_description(
                    "Number of metric points successfully ingested and pushed into the pipeline.",
                )
                .with_unit("log_records")
                .build(),
            refused_spans_records_counter: get_meter()
                .u64_counter("rotel_receiver_refused_spans")
                .with_description("Number of spans that could not be pushed into the pipeline.")
                .with_unit("spans")
                .build(),
            refused_metric_points_counter: get_meter()
                .u64_counter("rotel_receiver_refused_metric_points")
                .with_description(
                    "Number of metric points that could not be pushed into the pipeline.",
                )
                .with_unit("metric_points")
                .build(),
            refused_log_records_counter: get_meter()
                .u64_counter("rotel_receiver_refused_log_records")
                .with_description("Number of logs that could not be pushed into the pipeline.")
                .with_unit("log_records")
                .build(),
            tags: [KeyValue::new("protocol", "http")],
        }
    }
}

impl<H> Service<Request<H>> for OTLPService
where
    H: Body + Send + Sync + 'static,
    <H as Body>::Data: Send + Sync + Clone,
    <H as Body>::Error: Display + Debug + Send + Sync + ToString,
{
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<H>) -> Self::Future {
        match req.method() {
            &Method::POST => {
                let path = req.uri().path();
                let include_metadata = self.include_metadata;
                let headers_to_include = self.headers_to_include.clone();
                let tags = self.tags.clone();

                if path == self.traces_path {
                    if self.trace_output.is_none() {
                        return Box::pin(futures::future::ok(
                            response_4xx(StatusCode::NOT_FOUND).unwrap(),
                        ));
                    }
                    let output = self.trace_output.clone().unwrap();
                    let accepted = self.accepted_spans_records_counter.clone();
                    let refused = self.refused_spans_records_counter.clone();
                    return Box::pin(handle::<
                        H,
                        ExportTraceServiceRequest,
                        ExportTraceServiceResponse,
                        ResourceSpans,
                    >(
                        req,
                        output,
                        accepted,
                        refused,
                        tags,
                        include_metadata,
                        headers_to_include,
                    ));
                }
                if path == self.metrics_path {
                    if self.metrics_output.is_none() {
                        return Box::pin(futures::future::ok(
                            response_4xx(StatusCode::NOT_FOUND).unwrap(),
                        ));
                    }
                    let output = self.metrics_output.clone().unwrap();
                    let accepted = self.accepted_metric_points_counter.clone();
                    let refused = self.refused_metric_points_counter.clone();
                    return Box::pin(handle::<
                        H,
                        ExportMetricsServiceRequest,
                        ExportMetricsServiceResponse,
                        ResourceMetrics,
                    >(
                        req,
                        output,
                        accepted,
                        refused,
                        tags,
                        include_metadata,
                        headers_to_include,
                    ));
                }
                if path == self.logs_path {
                    if self.logs_output.is_none() {
                        return Box::pin(futures::future::ok(
                            response_4xx(StatusCode::NOT_FOUND).unwrap(),
                        ));
                    }
                    let output = self.logs_output.clone().unwrap();
                    let accepted = self.accepted_log_records_counter.clone();
                    let refused = self.refused_log_records_counter.clone();
                    return Box::pin(handle::<
                        H,
                        ExportLogsServiceRequest,
                        ExportLogsServiceResponse,
                        ResourceLogs,
                    >(
                        req,
                        output,
                        accepted,
                        refused,
                        tags,
                        include_metadata,
                        headers_to_include,
                    ));
                }
                Box::pin(futures::future::ok(
                    response_4xx(StatusCode::NOT_FOUND).unwrap(),
                ))
            }
            // Return 404 Not Found for other routes.
            _ => Box::pin(futures::future::ok(
                response_4xx(StatusCode::NOT_FOUND).unwrap(),
            )),
        }
    }
}

async fn decode_body<H: Body>(req: Request<H>) -> Result<Bytes, StatusCode>
where
    <H as Body>::Error: Display + Debug + Send + Sync + ToString,
{
    let is_gzip = req
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_lowercase() == "gzip")
        .unwrap_or(false);

    let body_bytes = match req.collect().await {
        Ok(bytes) => bytes.to_bytes(),
        Err(e) => {
            // todo: I should be able to add traits to allow this to downcast_ref, but
            // haven't been able to get the right traits defined. Use the string match for
            // now.
            if e.to_string().contains("length limit exceeded") {
                return Err(StatusCode::PAYLOAD_TOO_LARGE);
            }
            error!("Failed to read request body: {:?}", e);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    let decoded_bytes = if is_gzip {
        match decompress_gzip(&body_bytes) {
            Ok(bytes) => bytes,
            Err(e) => {
                if ErrorKind::InvalidData == e.kind() {
                    return Err(StatusCode::PAYLOAD_TOO_LARGE);
                }
                error!("Failed to decompress gzip data: {}", e);
                return Err(StatusCode::BAD_REQUEST);
            }
        }
    } else {
        body_bytes
    };

    Ok(decoded_bytes)
}

/// Extract specified HTTP headers and store them in a HashMap for metadata.
/// Headers are normalized to lowercase for consistent lookup.
fn extract_headers_to_metadata<H: Body>(
    req: &Request<H>,
    headers_to_include: &[String],
) -> HashMap<String, String> {
    let mut headers_map = HashMap::new();
    let request_headers = req.headers();

    for header_name in headers_to_include {
        // Normalize header name: lowercase for consistency
        let normalized = header_name.to_lowercase();

        if let Ok(header_name_parsed) = normalized.parse::<http::HeaderName>() {
            if let Some(header_value) = request_headers.get(&header_name_parsed) {
                if let Ok(value_str) = header_value.to_str() {
                    let header_value_str = value_str.to_string();
                    headers_map.insert(normalized.clone(), header_value_str);
                }
            }
        }
    }

    headers_map
}

async fn handle<
    H: Body,
    ExpReq: prost::Message + DeserializeOwned + Default + OTLPInto<Vec<T>>,
    ExpResp: prost::Message + Serialize + Default,
    T: prost::Message + crate::topology::batch::BatchSizer,
>(
    req: Request<H>,
    output: OTLPOutput<Message<T>>,
    accepted_counter: Counter<u64>,
    refused_counter: Counter<u64>,
    tags: [KeyValue; 1],
    include_metadata: bool,
    headers_to_include: Vec<String>,
) -> Result<Response<Full<Bytes>>, hyper::Error>
where
    <H as Body>::Error: Display + Debug + Send + Sync + ToString,
    [T]: crate::topology::batch::BatchSizer,
{
    // Extract headers before consuming the request body
    let http_metadata = if include_metadata && !headers_to_include.is_empty() {
        let headers_map = extract_headers_to_metadata(&req, &headers_to_include);

        if !headers_map.is_empty() {
            Some(crate::topology::payload::MessageMetadata::http(
                HttpMetadata::new(headers_map),
            ))
        } else {
            None
        }
    } else {
        None
    };

    let ct = req.headers().get(CONTENT_TYPE).unwrap().clone();

    let decoded_bytes = decode_body(req).await;
    if decoded_bytes.is_err() {
        return response_4xx(decoded_bytes.err().unwrap());
    }

    let mut json_resp = false;
    let otlp_req = match ct.to_str().unwrap() {
        PROTOBUF_CT => {
            let decoded = ExpReq::decode(decoded_bytes.unwrap());
            if let Err(e) = decoded {
                error!(
                    error = e.to_string(),
                    "Failed to decode OTLP HTTP/Protobuf request."
                );
                return response_4xx(StatusCode::BAD_REQUEST);
            }
            decoded.unwrap()
        }
        JSON_CT => {
            let decoded = serde_json::from_slice::<ExpReq>(decoded_bytes.unwrap().as_ref());
            if let Err(e) = decoded {
                error!(
                    error = e.to_string(),
                    "Failed to decode OTLP HTTP/JSON request."
                );
                return response_4xx(StatusCode::BAD_REQUEST);
            }
            json_resp = true;
            decoded.unwrap()
        }
        _ => {
            return response_4xx(StatusCode::BAD_REQUEST);
        }
    };

    let mut rb = Response::builder();
    let resp_headers = rb.headers_mut().unwrap();

    if json_resp {
        resp_headers.insert(CONTENT_TYPE, HeaderValue::from_str(JSON_CT).unwrap());
    } else {
        resp_headers.insert(CONTENT_TYPE, HeaderValue::from_str(PROTOBUF_CT).unwrap());
    }

    let otlp_payload = otlp_req.otlp_into();
    let count = BatchSizer::size_of(otlp_payload.as_slice());

    match output
        .send(Message {
            metadata: http_metadata,
            payload: otlp_payload,
        })
        .await
    {
        Ok(_) => {
            // No partial success at the moment
            let body = compute_ok_resp::<ExpResp>(json_resp).unwrap();

            let body = Full::new(body);
            accepted_counter.add(count as u64, &tags);
            Ok(rb.body(body).unwrap())
        }
        // todo: these should encode a GRPC Status as a body response
        Err(_) => {
            refused_counter.add(count as u64, &[KeyValue::new("protocol", "http")]);
            response_4xx(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

// We can't use the DecompressionLayer because it doesn't provide a limit
// on the inflated size
fn decompress_gzip(compressed: &[u8]) -> std::io::Result<Bytes> {
    let decoder = GzDecoder::new(compressed);
    let mut decoder = decoder.restrict(MAX_BODY_SIZE as u64);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(Bytes::from(decompressed))
}

fn response_4xx(code: StatusCode) -> Result<Response<Full<Bytes>>, hyper::Error> {
    response_4xx_with_body(code, Bytes::default())
}

fn response_4xx_with_body(
    code: StatusCode,
    body: Bytes,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    Ok(Response::builder()
        .status(code)
        .body(Full::new(body))
        .unwrap())
}

fn compute_ok_resp<T: prost::Message + Serialize + Default>(
    as_json: bool,
) -> Result<Bytes, EncodeError> {
    // The default response is actually empty, so this results in an empty response
    let resp = T::default();

    let ret_buf = match as_json {
        true => serde_json::to_vec(&resp).unwrap(),
        false => {
            let mut buf = Vec::with_capacity(resp.encoded_len());
            resp.encode(&mut buf)?;
            buf
        }
    };

    Ok(ret_buf.into())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use flate2::Compression as GZCompression;
    use flate2::read::GzEncoder;
    use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
    use http::{Method, Request, StatusCode};
    use http_body_util::Full;
    use hyper::service::Service;
    use hyper_util::client::legacy::Client;
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::rt::{TokioExecutor, TokioTimer};
    use std::io::Read;
    use std::time::Duration;

    extern crate utilities;
    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::listener::Listener;
    use crate::receivers::otlp::otlp_http::{
        MAX_BODY_SIZE, OTLPHttpServer, OTLPService, ValidateOTLPContentType, build_service,
    };
    use crate::receivers::otlp_output::OTLPOutput;
    use hyper_util::service::TowerToHyperService;
    use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use prost::Message;
    use tokio::join;
    use tokio::time::sleep;
    use tokio_test::assert_ok;
    use tokio_util::sync::CancellationToken;
    use tower_http::compression::Compression;
    use tower_http::limit::RequestBodyLimit;
    use tower_http::trace::{HttpMakeClassifier, Trace};
    use tower_http::validate_request::ValidateRequestHeader;
    use tracing_test::traced_test;
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn invalid_requests() {
        let (svc, _, _, _) = new_svc();

        // Bad path
        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/")
            .method(Method::POST)
            .body(Full::<Bytes>::default())
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::NOT_FOUND, resp.status());

        // Wrong method
        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::GET)
            .body(Full::<Bytes>::default())
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::NOT_FOUND, resp.status());

        // Invalid content type
        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::<Bytes>::default())
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::BAD_REQUEST, resp.status());
    }

    #[tokio::test]
    async fn size_limits() {
        let (svc, _, _, _) = new_svc();

        let mut large_vec = vec![0; MAX_BODY_SIZE + 1];
        large_vec.resize(MAX_BODY_SIZE + 1, 0);

        let buf = Bytes::from(large_vec);

        // Content too long
        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::PAYLOAD_TOO_LARGE, resp.status());

        let mut large_vec = vec![0; MAX_BODY_SIZE + 1];
        large_vec.resize(MAX_BODY_SIZE + 1, 0);

        let mut gz_vec = Vec::new();
        let mut gz = GzEncoder::new(&large_vec[..], GZCompression::fast());
        gz.read_to_end(&mut gz_vec).unwrap();

        let buf = Bytes::from(gz_vec);

        assert!(buf.len() < MAX_BODY_SIZE);
        // Inflated content too long

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "gzip")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::PAYLOAD_TOO_LARGE, resp.status());
    }

    #[tokio::test]
    async fn valid_trace_posts() {
        let (svc, mut trace_rx, _, _) = new_svc();

        let trace_req = FakeOTLP::trace_service_request();

        let mut buf = Vec::with_capacity(trace_req.encoded_len());
        assert_ok!(trace_req.encode(&mut buf));

        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        assert_eq!(
            "application/x-protobuf",
            resp.headers().get(CONTENT_TYPE).unwrap()
        );

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());
    }

    #[tokio::test]
    async fn valid_trace_posts_json() {
        let (svc, mut trace_rx, _, _) = new_svc();

        let trace_req = FakeOTLP::trace_service_request();

        let buf = serde_json::to_vec(&trace_req).unwrap();
        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        assert_eq!(
            "application/json",
            resp.headers().get(CONTENT_TYPE).unwrap()
        );

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());
    }

    #[traced_test]
    #[tokio::test]
    async fn does_not_log_header_timeout() {
        let (trtx, _trrx) = bounded(10);
        let trout = OTLPOutput::new(trtx);
        let (mtx, _mrx) = bounded(10);
        let mout = OTLPOutput::new(mtx);

        let srv = OTLPHttpServer::builder()
            .with_traces_output(Some(trout))
            .with_metrics_output(Some(mout))
            .with_traces_path("/v1/traces".to_string())
            .with_header_timeout(Duration::from_millis(50))
            .build();

        let cancel_token = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.bound_address().unwrap();
        let srv_fut = {
            let cancel_token = cancel_token.clone();
            async move { srv.serve(listener, cancel_token).await }
        };

        let trace_req = FakeOTLP::trace_service_request();
        let mut buf = Vec::with_capacity(trace_req.encoded_len());
        assert_ok!(trace_req.encode(&mut buf));
        let buf = Bytes::from(buf);

        let srv_hnd = tokio::spawn(srv_fut);

        let uri = format!("http://{addr}/v1/traces");
        let client = new_client();
        let req = hyper::Request::builder()
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .uri(uri)
            .body(Full::new(buf))
            .unwrap();

        let resp = client.request(req.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            "application/x-protobuf",
            resp.headers().get(CONTENT_TYPE).unwrap()
        );

        // wait, ideally keeping the connection open
        sleep(Duration::from_millis(100)).await;

        // should reuse open connection
        let resp = client.request(req).await;
        assert_ok!(resp);

        cancel_token.cancel();
        let r = join!(srv_hnd);
        assert_ok!(r.0.unwrap());

        // we should not see an error logged
        assert!(!logs_contain("error serving connection"));
    }

    #[tokio::test]
    async fn valid_metrics_posts() {
        let (svc, _, mut metrics_rx, _) = new_svc();

        let metrics_req = FakeOTLP::metrics_service_request();

        let mut buf = Vec::with_capacity(metrics_req.encoded_len());
        assert_ok!(metrics_req.encode(&mut buf));

        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/metrics")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        assert_eq!(
            "application/x-protobuf",
            resp.headers().get(CONTENT_TYPE).unwrap()
        );

        let msg = metrics_rx.next().await.unwrap();
        assert_eq!(1, msg.len());
    }

    #[tokio::test]
    async fn valid_metrics_posts_json() {
        let (svc, _, mut metrics_rx, _) = new_svc();

        let metrics_req = FakeOTLP::metrics_service_request();

        let buf = serde_json::to_vec(&metrics_req).unwrap();
        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/metrics")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        assert_eq!(
            "application/json",
            resp.headers().get(CONTENT_TYPE).unwrap()
        );

        let msg = metrics_rx.next().await.unwrap();
        assert_eq!(1, msg.len());
    }

    #[tokio::test]
    async fn valid_logs_posts() {
        let (svc, _, _, mut logs_rx) = new_svc();

        let logs_req = FakeOTLP::logs_service_request();

        let mut buf = Vec::with_capacity(logs_req.encoded_len());
        assert_ok!(logs_req.encode(&mut buf));

        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/logs")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        assert_eq!(
            "application/x-protobuf",
            resp.headers().get(CONTENT_TYPE).unwrap()
        );

        let msg = logs_rx.next().await.unwrap();
        assert_eq!(1, msg.len());
    }

    #[tokio::test]
    async fn valid_logs_posts_json() {
        let (svc, _, _, mut logs_rx) = new_svc();

        let logs_req = FakeOTLP::logs_service_request();

        let buf = serde_json::to_vec(&logs_req).unwrap();
        let buf = Bytes::from(buf);

        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/logs")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(buf))
            .unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());
        assert_eq!(
            "application/json",
            resp.headers().get(CONTENT_TYPE).unwrap()
        );

        let msg = logs_rx.next().await.unwrap();
        assert_eq!(1, msg.len());
    }

    fn new_svc() -> (
        TowerToHyperService<
            Trace<
                RequestBodyLimit<
                    Compression<ValidateRequestHeader<OTLPService, ValidateOTLPContentType>>,
                >,
                HttpMakeClassifier,
            >,
        >,
        BoundedReceiver<crate::topology::payload::Message<ResourceSpans>>,
        BoundedReceiver<crate::topology::payload::Message<ResourceMetrics>>,
        BoundedReceiver<crate::topology::payload::Message<ResourceLogs>>,
    ) {
        let (trace_tx, trace_rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(10);
        let (metrics_tx, metrics_rx) =
            bounded::<crate::topology::payload::Message<ResourceMetrics>>(10);
        let (logs_tx, logs_rx) = bounded::<crate::topology::payload::Message<ResourceLogs>>(10);
        let trace_output = OTLPOutput::new(trace_tx);
        let metrics_output = OTLPOutput::new(metrics_tx);
        let logs_output = OTLPOutput::new(logs_tx);

        let svc = build_service(
            Some(trace_output),
            Some(metrics_output),
            Some(logs_output),
            "/v1/traces".to_string(),
            "/v1/metrics".to_string(),
            "/v1/logs".to_string(),
            false,
            vec![],
        );
        let svc = TowerToHyperService::new(svc);

        (svc, trace_rx, metrics_rx, logs_rx)
    }

    fn new_client() -> Client<HttpConnector, Full<Bytes>> {
        hyper_util::client::legacy::Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(2))
            .pool_max_idle_per_host(2)
            .timer(TokioTimer::new())
            .build::<_, Full<Bytes>>(HttpConnector::new())
    }

    fn new_svc_with_metadata(
        include_metadata: bool,
        headers_to_include: Vec<String>,
    ) -> (
        TowerToHyperService<
            Trace<
                RequestBodyLimit<
                    Compression<ValidateRequestHeader<OTLPService, ValidateOTLPContentType>>,
                >,
                HttpMakeClassifier,
            >,
        >,
        BoundedReceiver<crate::topology::payload::Message<ResourceSpans>>,
        BoundedReceiver<crate::topology::payload::Message<ResourceMetrics>>,
        BoundedReceiver<crate::topology::payload::Message<ResourceLogs>>,
    ) {
        let (trace_tx, trace_rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(10);
        let (metrics_tx, metrics_rx) =
            bounded::<crate::topology::payload::Message<ResourceMetrics>>(10);
        let (logs_tx, logs_rx) = bounded::<crate::topology::payload::Message<ResourceLogs>>(10);
        let trace_output = OTLPOutput::new(trace_tx);
        let metrics_output = OTLPOutput::new(metrics_tx);
        let logs_output = OTLPOutput::new(logs_tx);

        let svc = build_service(
            Some(trace_output),
            Some(metrics_output),
            Some(logs_output),
            "/v1/traces".to_string(),
            "/v1/metrics".to_string(),
            "/v1/logs".to_string(),
            include_metadata,
            headers_to_include,
        );
        let svc = TowerToHyperService::new(svc);

        (svc, trace_rx, metrics_rx, logs_rx)
    }

    #[tokio::test]
    async fn http_metadata_extracted_for_traces() {
        let example_headers = FakeOTLP::example_headers();
        let header_names: Vec<String> = example_headers.keys().cloned().collect();

        let (svc, mut trace_rx, _, _) = new_svc_with_metadata(true, header_names.clone());

        let trace_req = FakeOTLP::trace_service_request();
        let mut buf = Vec::with_capacity(trace_req.encoded_len());
        assert_ok!(trace_req.encode(&mut buf));
        let buf = Bytes::from(buf);

        let mut req_builder = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf");

        // Add example headers from FakeOTLP
        for (key, value) in &example_headers {
            req_builder = req_builder.header(key, value.as_str());
        }

        let req: Request<Full<Bytes>> = req_builder.body(Full::new(buf)).unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present
        assert!(msg.metadata.is_some());
        let metadata = msg.metadata.as_ref().unwrap();
        let http_metadata = metadata.as_http().unwrap();

        // Verify all example headers are present
        for (key, expected_value) in &example_headers {
            assert_eq!(
                http_metadata.get_header(key),
                Some(expected_value),
                "Header {} should be present with value {}",
                key,
                expected_value
            );
        }
    }

    #[tokio::test]
    async fn http_metadata_not_extracted_when_disabled() {
        let example_headers = FakeOTLP::example_headers();
        let header_names: Vec<String> = example_headers.keys().cloned().collect();
        let first_header = header_names.first().unwrap();

        let (svc, mut trace_rx, _, _) = new_svc_with_metadata(false, vec![first_header.clone()]);

        let trace_req = FakeOTLP::trace_service_request();
        let mut buf = Vec::with_capacity(trace_req.encoded_len());
        assert_ok!(trace_req.encode(&mut buf));
        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(first_header, example_headers.get(first_header).unwrap())
            .body(Full::new(buf))
            .unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is NOT present when disabled
        assert!(msg.metadata.is_none());
    }

    #[tokio::test]
    async fn http_metadata_not_extracted_when_no_headers_specified() {
        let example_headers = FakeOTLP::example_headers();
        let first_header = example_headers.keys().next().unwrap();

        let (svc, mut trace_rx, _, _) = new_svc_with_metadata(true, vec![]);

        let trace_req = FakeOTLP::trace_service_request();
        let mut buf = Vec::with_capacity(trace_req.encoded_len());
        assert_ok!(trace_req.encode(&mut buf));
        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(first_header, example_headers.get(first_header).unwrap())
            .body(Full::new(buf))
            .unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is NOT present when no headers specified
        assert!(msg.metadata.is_none());
    }

    #[tokio::test]
    async fn http_metadata_extracted_for_metrics() {
        let (svc, _, mut metrics_rx, _) =
            new_svc_with_metadata(true, vec!["my-custom-header".to_string()]);

        let metrics_req = FakeOTLP::metrics_service_request();
        let mut buf = Vec::with_capacity(metrics_req.encoded_len());
        assert_ok!(metrics_req.encode(&mut buf));
        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/metrics")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header("my-custom-header", "metrics-value")
            .body(Full::new(buf))
            .unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());

        let msg = metrics_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present
        assert!(msg.metadata.is_some());
        let metadata = msg.metadata.as_ref().unwrap();
        let http_metadata = metadata.as_http().unwrap();
        assert_eq!(
            http_metadata.get_header("my-custom-header"),
            Some(&"metrics-value".to_string())
        );
    }

    #[tokio::test]
    async fn http_metadata_extracted_for_logs() {
        let (svc, _, _, mut logs_rx) =
            new_svc_with_metadata(true, vec!["my-custom-header".to_string()]);

        let logs_req = FakeOTLP::logs_service_request();
        let mut buf = Vec::with_capacity(logs_req.encoded_len());
        assert_ok!(logs_req.encode(&mut buf));
        let buf = Bytes::from(buf);

        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/logs")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header("my-custom-header", "logs-value")
            .body(Full::new(buf))
            .unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());

        let msg = logs_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present
        assert!(msg.metadata.is_some());
        let metadata = msg.metadata.as_ref().unwrap();
        let http_metadata = metadata.as_http().unwrap();
        assert_eq!(
            http_metadata.get_header("my-custom-header"),
            Some(&"logs-value".to_string())
        );
    }

    #[tokio::test]
    async fn http_metadata_header_case_insensitive() {
        let (svc, mut trace_rx, _, _) =
            new_svc_with_metadata(true, vec!["My-Custom-Header".to_string()]);

        let trace_req = FakeOTLP::trace_service_request();
        let mut buf = Vec::with_capacity(trace_req.encoded_len());
        assert_ok!(trace_req.encode(&mut buf));
        let buf = Bytes::from(buf);

        // Send header with different case
        let req: Request<Full<Bytes>> = Request::builder()
            .uri("/v1/traces")
            .method(Method::POST)
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header("my-custom-header", "test-value")
            .body(Full::new(buf))
            .unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(StatusCode::OK, resp.status());

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present and header is normalized to lowercase
        assert!(msg.metadata.is_some());
        let metadata = msg.metadata.as_ref().unwrap();
        let http_metadata = metadata.as_http().unwrap();
        // Should be able to retrieve with lowercase
        assert_eq!(
            http_metadata.get_header("my-custom-header"),
            Some(&"test-value".to_string())
        );
    }
}
