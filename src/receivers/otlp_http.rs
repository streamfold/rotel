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
use tracing::error;

use crate::listener::Listener;
use crate::receivers::get_meter;
use crate::topology::batch::BatchSizer;
use crate::topology::payload::OTLPInto;
use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
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

#[derive(Default)]
pub struct OTLPHttpServerBuilder {
    trace_output: Option<OTLPOutput<Vec<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Vec<ResourceLogs>>>,
    traces_path: String,
    metrics_path: String,
    logs_path: String,
    header_timeout: Option<Duration>,
}

impl OTLPHttpServerBuilder {
    pub fn with_traces_output(
        mut self,
        output: Option<OTLPOutput<Vec<ResourceSpans>>>,
    ) -> OTLPHttpServerBuilder {
        self.trace_output = output;
        self
    }
    pub fn with_metrics_output(
        mut self,
        output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    ) -> OTLPHttpServerBuilder {
        self.metrics_output = output;
        self
    }
    pub fn with_logs_output(
        mut self,
        output: Option<OTLPOutput<Vec<ResourceLogs>>>,
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

    pub fn build(self) -> OTLPHttpServer {
        OTLPHttpServer {
            trace_output: self.trace_output,
            metrics_output: self.metrics_output,
            logs_output: self.logs_output,
            header_timeout: self.header_timeout.unwrap_or(DEFAULT_HEADER_TIMEOUT),
            traces_path: self.traces_path,
            metrics_path: self.metrics_path,
            logs_path: self.logs_path,
        }
    }
}

pub struct OTLPHttpServer {
    pub trace_output: Option<OTLPOutput<Vec<ResourceSpans>>>,
    pub metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    pub logs_output: Option<OTLPOutput<Vec<ResourceLogs>>>,
    pub traces_path: String,
    pub metrics_path: String,
    pub logs_path: String,
    header_timeout: Duration,
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

        if request
            .headers()
            .get(CONTENT_TYPE)
            .is_none_or(|ct| ct != PROTOBUF_CT)
        {
            Err(response_4xx(StatusCode::BAD_REQUEST).unwrap())
        } else {
            Ok(())
        }
    }
}

fn build_service(
    trace_output: Option<OTLPOutput<Vec<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Vec<ResourceLogs>>>,
    traces_path: String,
    metrics_path: String,
    logs_path: String,
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
        ))
}

#[derive(Clone)]
struct OTLPService {
    trace_output: Option<OTLPOutput<Vec<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Vec<ResourceLogs>>>,
    trace_ok_resp: Bytes,
    metrics_ok_resp: Bytes,
    traces_path: String,
    metrics_path: String,
    logs_path: String,
    logs_ok_resp: Bytes,
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
        trace_output: Option<OTLPOutput<Vec<ResourceSpans>>>,
        metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
        logs_output: Option<OTLPOutput<Vec<ResourceLogs>>>,
        traces_path: String,
        metrics_path: String,
        logs_path: String,
    ) -> Self {
        // Compute this once

        Self {
            trace_output,
            metrics_output,
            logs_output,
            traces_path,
            metrics_path,
            logs_path,
            trace_ok_resp: compute_ok_resp::<ExportTraceServiceResponse>().unwrap(),
            metrics_ok_resp: compute_ok_resp::<ExportMetricsServiceResponse>().unwrap(),
            logs_ok_resp: compute_ok_resp::<ExportLogsServiceResponse>().unwrap(),
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
                if path == self.traces_path {
                    if self.trace_output.is_none() {
                        return Box::pin(futures::future::ok(
                            response_4xx(StatusCode::NOT_FOUND).unwrap(),
                        ));
                    }
                    let trace_ok_resp = self.trace_ok_resp.clone();
                    let output = self.trace_output.clone().unwrap();
                    let accepted = self.accepted_spans_records_counter.clone();
                    let refused = self.refused_spans_records_counter.clone();
                    let tags = self.tags.clone();
                    return Box::pin(handle::<H, ExportTraceServiceRequest, ResourceSpans>(
                        req,
                        output,
                        trace_ok_resp,
                        accepted,
                        refused,
                        tags,
                    ));
                }
                if path == self.metrics_path {
                    if self.metrics_output.is_none() {
                        return Box::pin(futures::future::ok(
                            response_4xx(StatusCode::NOT_FOUND).unwrap(),
                        ));
                    }
                    let metrics_ok_resp = self.metrics_ok_resp.clone();
                    let output = self.metrics_output.clone().unwrap();
                    let accepted = self.accepted_metric_points_counter.clone();
                    let refused = self.refused_metric_points_counter.clone();
                    let tags= self.tags.clone();
                    return Box::pin(handle::<H, ExportMetricsServiceRequest, ResourceMetrics>(
                        req,
                        output,
                        metrics_ok_resp,
                        accepted,
                        refused,
                        tags,
                    ));
                }
                if path == self.logs_path {
                    if self.logs_output.is_none() {
                        return Box::pin(futures::future::ok(
                            response_4xx(StatusCode::NOT_FOUND).unwrap(),
                        ));
                    }
                    let logs_ok_resp = self.logs_ok_resp.clone();
                    let output = self.logs_output.clone().unwrap();
                    let accepted = self.accepted_log_records_counter.clone();
                    let refused = self.refused_log_records_counter.clone();
                    let tags = self.tags.clone();
                    return Box::pin(handle::<H, ExportLogsServiceRequest, ResourceLogs>(
                        req,
                        output,
                        logs_ok_resp,
                        accepted,
                        refused,
                        tags,
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

async fn handle<H: Body, ExpReq: prost::Message + Default + OTLPInto<Vec<T>>, T: prost::Message>(
    req: Request<H>,
    output: OTLPOutput<Vec<T>>,
    ok_resp: Bytes,
    accepted_counter: Counter<u64>,
    refused_counter: Counter<u64>,
    tags: [KeyValue; 1],
) -> Result<Response<Full<Bytes>>, hyper::Error>
where
    <H as Body>::Error: Display + Debug + Send + Sync + ToString,
    [T]: BatchSizer,
{
    let decoded_bytes = decode_body(req).await;
    if decoded_bytes.is_err() {
        return response_4xx(decoded_bytes.err().unwrap());
    }
    let decoded = ExpReq::decode(decoded_bytes.unwrap());
    if let Err(e) = decoded {
        error!(error = e.to_string(), "Failed to decode OTLP/HTTP request.");
        return response_4xx(StatusCode::BAD_REQUEST);
    }

    let req = decoded.unwrap();

    let mut rb = Response::builder();
    rb.headers_mut()
        .unwrap()
        .insert(CONTENT_TYPE, HeaderValue::from_str(PROTOBUF_CT).unwrap());

    let otlp_payload = ExpReq::otlp_into(req);
    let count = BatchSizer::size_of(otlp_payload.as_slice());

    match output.send(otlp_payload).await {
        Ok(_) => {
            // No partial success at the moment
            let body = Full::new(ok_resp.clone());
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

fn compute_ok_resp<T: prost::Message + Default>() -> Result<Bytes, EncodeError> {
    // The default response is actually empty, so this results in an empty response
    let resp = T::default();
    let mut buf = Vec::with_capacity(resp.encoded_len());
    resp.encode(&mut buf)?;

    Ok(buf.into())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use flate2::read::GzEncoder;
    use flate2::Compression as GZCompression;
    use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
    use http::{Method, Request, StatusCode};
    use http_body_util::Full;
    use hyper::service::Service;
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::{TokioExecutor, TokioTimer};
    use std::io::Read;
    use std::time::Duration;

    extern crate utilities;
    use crate::bounded_channel::{bounded, BoundedReceiver};
    use crate::listener::Listener;
    use crate::receivers::otlp_http::{
        build_service, OTLPHttpServer, OTLPService, ValidateOTLPContentType, MAX_BODY_SIZE,
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
        BoundedReceiver<Vec<ResourceSpans>>,
        BoundedReceiver<Vec<ResourceMetrics>>,
        BoundedReceiver<Vec<ResourceLogs>>,
    ) {
        let (trace_tx, trace_rx) = bounded::<Vec<ResourceSpans>>(10);
        let (metrics_tx, metrics_rx) = bounded::<Vec<ResourceMetrics>>(10);
        let (logs_tx, logs_rx) = bounded::<Vec<ResourceLogs>>(10);
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
}
