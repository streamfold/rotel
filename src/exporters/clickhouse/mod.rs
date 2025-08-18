mod api_request;
mod ch_error;
mod compression;
mod exception;
mod payload;
mod request_builder;
mod request_mapper;
mod rowbinary;
mod schema;
mod transform_logs;
mod transform_metrics;
mod transform_traces;
mod transformer;

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::clickhouse::api_request::ConnectionConfig;
use crate::exporters::clickhouse::exception::extract_exception;
use crate::exporters::clickhouse::request_builder::{RequestBuilder, TransformPayload};
use crate::exporters::clickhouse::request_mapper::RequestMapper;
use crate::exporters::clickhouse::transformer::Transformer;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::exporter::Exporter;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::request_iter::RequestIterator;
use crate::exporters::http::response::Response;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use crate::exporters::http::tls;
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::FlushReceiver;
use bytes::Bytes;
use flume::r#async::RecvStream;
use http::Request;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use payload::ClickhousePayload;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, str};
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, ServiceBuilder};

use super::http::finalizer::ResultFinalizer;

// Buffer sizes from Clickhouse driver
pub(crate) const BUFFER_SIZE: usize = 256 * 1024;
// Threshold to send a chunk. Should be slightly less than `BUFFER_SIZE`
// to avoid extra reallocations in case of a big last row.
pub(crate) const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 2048;

#[derive(Default, Clone, PartialEq)]
pub enum Compression {
    None,
    #[default]
    Lz4,
}

#[derive(Clone, Default)]
pub struct ClickhouseExporterConfigBuilder {
    retry_config: RetryConfig,
    compression: Compression,
    endpoint: String,
    database: String,
    table_prefix: String,
    auth_user: Option<String>,
    auth_password: Option<String>,
    async_insert: bool,
    use_json: bool,
    use_json_underscore: bool,
}

type SvcType<RespBody> = TowerRetry<
    RetryPolicy<RespBody>,
    Timeout<HttpClient<ClickhousePayload, RespBody, ClickhouseRespDecoder>>,
>;

pub type ExporterType<'a, Resource> = Exporter<
    RequestIterator<
        RequestBuilderMapper<
            RecvStream<'a, Vec<Resource>>,
            Resource,
            ClickhousePayload,
            RequestBuilder<Resource, Transformer>,
        >,
        Vec<Request<ClickhousePayload>>,
        ClickhousePayload,
    >,
    SvcType<ClickhouseResponse>,
    ClickhousePayload,
    ClickhouseResultFinalizer,
>;

impl ClickhouseExporterConfigBuilder {
    pub fn new(
        endpoint: String,
        database: String,
        table_prefix: String,
    ) -> ClickhouseExporterConfigBuilder {
        ClickhouseExporterConfigBuilder {
            endpoint,
            database,
            table_prefix,
            ..Default::default()
        }
    }

    pub fn with_compression(mut self, compression: impl Into<Compression>) -> Self {
        self.compression = compression.into();
        self
    }

    pub fn with_json(mut self, json: bool) -> Self {
        self.use_json = json;
        self
    }

    pub fn with_json_underscore(mut self, json_underscore: bool) -> Self {
        self.use_json_underscore = json_underscore;
        self
    }

    pub fn with_async_insert(mut self, async_insert: bool) -> Self {
        self.async_insert = async_insert;
        self
    }

    pub fn with_user(mut self, user: String) -> Self {
        self.auth_user = Some(user);
        self
    }

    pub fn with_password(mut self, password: String) -> Self {
        self.auth_password = Some(password);
        self
    }

    pub fn build(self) -> Result<ClickhouseExporterBuilder, BoxError> {
        let config = ConnectionConfig {
            endpoint: self.endpoint,
            database: self.database,
            compression: self.compression,
            auth_user: self.auth_user,
            auth_password: self.auth_password,
            async_insert: self.async_insert,
            use_json: self.use_json,
            use_json_underscore: self.use_json_underscore,
        };

        let mapper = Arc::new(RequestMapper::new(&config, self.table_prefix)?);

        Ok(ClickhouseExporterBuilder {
            config,
            request_mapper: mapper,
            retry_config: self.retry_config,
        })
    }
}

pub struct ClickhouseExporterBuilder {
    config: ConnectionConfig,
    retry_config: RetryConfig,
    request_mapper: Arc<RequestMapper>,
}

impl ClickhouseExporterBuilder {
    pub fn build_traces_exporter<'a>(
        &self,
        rx: BoundedReceiver<Vec<ResourceSpans>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceSpans>, BoxError> {
        self.build_exporter("traces", rx, flush_receiver)
    }

    pub fn build_logs_exporter<'a>(
        &self,
        rx: BoundedReceiver<Vec<ResourceLogs>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceLogs>, BoxError> {
        self.build_exporter("logs", rx, flush_receiver)
    }

    pub fn build_metrics_exporter<'a>(
        &self,
        rx: BoundedReceiver<Vec<ResourceMetrics>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceMetrics>, BoxError> {
        self.build_exporter("metrics", rx, flush_receiver)
    }

    fn build_exporter<'a, Resource>(
        &self,
        telemetry_type: &'static str,
        rx: BoundedReceiver<Vec<Resource>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, Resource>, BoxError>
    where
        Resource: Clone + Send + Sync + 'static,
        Transformer: TransformPayload<Resource>,
    {
        let client = HttpClient::build(tls::Config::default(), Default::default())?;

        let transformer = Transformer::new(
            self.config.compression.clone(),
            self.config.use_json,
            self.config.use_json_underscore,
        );

        let req_builder = RequestBuilder::new(transformer, self.request_mapper.clone())?;

        let retry_layer = RetryPolicy::new(self.retry_config.clone(), None);
        let retry_broadcast = retry_layer.retry_broadcast();

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .timeout(Duration::from_secs(5))
            .service(client);

        let enc_stream =
            RequestIterator::new(RequestBuilderMapper::new(rx.into_stream(), req_builder));

        let exp = Exporter::new(
            "clickhouse",
            telemetry_type,
            enc_stream,
            svc,
            ClickhouseResultFinalizer {
                telemetry_type: telemetry_type.to_string(),
            },
            flush_receiver,
            retry_broadcast,
            Duration::from_secs(1),
            Duration::from_secs(2),
        );

        Ok(exp)
    }
}

#[derive(Debug, Clone)]
pub enum ClickhouseResponse {
    Empty,
    DbException(i32, String, String),
    Unknown(String),
}

impl Display for ClickhouseResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClickhouseResponse::Empty => write!(f, ""),
            ClickhouseResponse::DbException(code, exception, version) => {
                write!(
                    f,
                    "Database exception (code: {}, exception: {}, version: {})",
                    code, exception, version
                )
            }
            ClickhouseResponse::Unknown(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

#[derive(Default, Clone)]
pub struct ClickhouseRespDecoder;

impl ResponseDecode<ClickhouseResponse> for ClickhouseRespDecoder {
    fn decode(&self, body: Bytes, _: ContentEncoding) -> Result<ClickhouseResponse, BoxError> {
        // If there's an exception in the response, extract it
        if let Some((code, exception, version)) = extract_exception(body.as_ref()) {
            return Ok(ClickhouseResponse::DbException(code, exception, version));
        }

        let str_payload = str::from_utf8(&body)
            .map(|s| s.to_string())
            .map_err(|e| format!("error decoding response: {}", e))?;

        if str_payload.is_empty() {
            return Ok(ClickhouseResponse::Empty);
        }

        return Ok(ClickhouseResponse::Unknown(str_payload));
    }
}

pub struct ClickhouseResultFinalizer {
    telemetry_type: String,
}

impl<Err> ResultFinalizer<Result<Response<ClickhouseResponse>, Err>> for ClickhouseResultFinalizer
where
    Err: Into<BoxError>,
{
    fn finalize(&self, result: Result<Response<ClickhouseResponse>, Err>) -> Result<(), BoxError> {
        match result {
            Ok(r) => match r {
                Response::Http(parts, body) => {
                    match parts.status.as_u16() {
                        200..=202 => Ok(()),
                        404 => Err(format!("Received a 404 when exporting to Clickhouse, does the table exist? (type = {})", self.telemetry_type).into()),
                        _ => match body {
                            Some(body) => Err(format!("Failed to export to Clickhouse (status = {}): {}", parts.status, body).into()),
                            None => Err(format!("Failed to export to Clickhouse (status = {})", parts.status).into()),
                        }
                    }
                },
                Response::Grpc(_, _) => Err(format!("Clickhouse invalid response type").into()),
            },
            Err(e) => Err(e.into())
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use super::*;
    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::exporters::crypto_init_tests::init_crypto;
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use tokio::join;
    use tokio_test::{assert_err, assert_ok};
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn traces_success() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("ohi");
        });

        let (btx, brx) = bounded::<Vec<ResourceSpans>>(100);
        let exporter = new_traces_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let traces = FakeOTLP::trace_service_request();
        btx.send(traces.resource_spans).await.unwrap();
        drop(btx);
        let res = join!(jh);
        assert_ok!(res.0);

        hello_mock.assert();
    }

    #[tokio::test]
    async fn logs_success() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("ohi");
        });

        let (btx, brx) = bounded::<Vec<ResourceLogs>>(100);
        let exporter = new_logs_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let logs = FakeOTLP::logs_service_request();
        btx.send(logs.resource_logs).await.unwrap();
        drop(btx);
        let res = join!(jh);
        assert_ok!(res.0);

        hello_mock.assert();
    }

    #[tokio::test]
    async fn metrics_success() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("ohi");
        });

        let (btx, brx) = bounded::<Vec<ResourceMetrics>>(100);
        let exporter = new_metrics_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(metrics.resource_metrics).await.unwrap();
        drop(btx);
        let res = join!(jh);
        assert_ok!(res.0);

        hello_mock.assert();
    }

    #[tokio::test]
    async fn db_exception() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                // Must keep newline for matching
                .body("Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(equals(number, 2) :: 1) -> throwIf(equals(number, 2))
");
        });

        let (btx, brx) = bounded::<Vec<ResourceSpans>>(100);
        let exporter = new_traces_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let traces = FakeOTLP::trace_service_request();
        btx.send(traces.resource_spans).await.unwrap();
        drop(btx);
        let res = join!(jh).0.unwrap();
        assert_err!(res);

        hello_mock.assert();
    }

    fn new_traces_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<ResourceSpans>>,
    ) -> ExporterType<'a, ResourceSpans> {
        let builder =
            ClickhouseExporterConfigBuilder::new(addr, "otel".to_string(), "otel".to_string())
                .build()
                .unwrap();

        builder.build_traces_exporter(brx, None).unwrap()
    }

    fn new_logs_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<ResourceLogs>>,
    ) -> ExporterType<'a, ResourceLogs> {
        let builder =
            ClickhouseExporterConfigBuilder::new(addr, "otel".to_string(), "otel".to_string())
                .build()
                .unwrap();

        builder.build_logs_exporter(brx, None).unwrap()
    }

    fn new_metrics_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<ResourceMetrics>>,
    ) -> ExporterType<'a, ResourceMetrics> {
        let builder =
            ClickhouseExporterConfigBuilder::new(addr, "otel".to_string(), "otel".to_string())
                .build()
                .unwrap();

        builder.build_metrics_exporter(brx, None).unwrap()
    }
}
