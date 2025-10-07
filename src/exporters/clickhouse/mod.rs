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

use super::http::finalizer::ResultFinalizer;
use crate::bounded_channel::BoundedReceiver;
use crate::exporters::clickhouse::api_request::ConnectionConfig;
use crate::exporters::clickhouse::exception::extract_exception;
use crate::exporters::clickhouse::request_builder::{RequestBuilder, TransformPayload};
use crate::exporters::clickhouse::request_mapper::RequestMapper;
use crate::exporters::clickhouse::transformer::Transformer;
use crate::exporters::http::acknowledger::Acknowledger;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::client::{Client, Protocol};
use crate::exporters::http::exporter::Exporter;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::request_iter::RequestIterator;
use crate::exporters::http::response::Response;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use crate::exporters::http::tls;
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::FlushReceiver;
use crate::topology::payload::Message;
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
    request_timeout: Duration,
}

type SvcType<RespBody> = TowerRetry<
    RetryPolicy<RespBody>,
    Timeout<Client<ClickhousePayload, RespBody, ClickhouseRespDecoder>>,
>;

pub type ExporterType<'a, Resource, Ack> = Exporter<
    RequestIterator<
        RequestBuilderMapper<
            RecvStream<'a, Vec<Message<Resource>>>,
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
    Ack,
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
            request_timeout: Duration::from_secs(5),
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

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
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
            request_timeout: self.request_timeout,
        })
    }
}

pub struct ClickhouseExporterBuilder {
    config: ConnectionConfig,
    retry_config: RetryConfig,
    request_mapper: Arc<RequestMapper>,
    request_timeout: Duration,
}

impl ClickhouseExporterBuilder {
    pub fn build_traces_exporter<'a, Ack>(
        &self,
        rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
        flush_receiver: Option<FlushReceiver>,
        acknowledger: Ack,
    ) -> Result<ExporterType<'a, ResourceSpans, Ack>, BoxError>
    where
        Ack: Acknowledger<ClickhouseResponse> + Clone + Send + Sync + 'static,
    {
        self.build_exporter("traces", rx, flush_receiver, acknowledger)
    }

    pub fn build_logs_exporter<'a, Ack>(
        &self,
        rx: BoundedReceiver<Vec<Message<ResourceLogs>>>,
        flush_receiver: Option<FlushReceiver>,
        acknowledger: Ack,
    ) -> Result<ExporterType<'a, ResourceLogs, Ack>, BoxError>
    where
        Ack: Acknowledger<ClickhouseResponse> + Clone + Send + Sync + 'static,
    {
        self.build_exporter("logs", rx, flush_receiver, acknowledger)
    }

    pub fn build_metrics_exporter<'a, Ack>(
        &self,
        rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
        flush_receiver: Option<FlushReceiver>,
        acknowledger: Ack,
    ) -> Result<ExporterType<'a, ResourceMetrics, Ack>, BoxError>
    where
        Ack: Acknowledger<ClickhouseResponse> + Clone + Send + Sync + 'static,
    {
        self.build_exporter("metrics", rx, flush_receiver, acknowledger)
    }

    fn build_exporter<'a, Resource, Ack>(
        &self,
        telemetry_type: &'static str,
        rx: BoundedReceiver<Vec<Message<Resource>>>,
        flush_receiver: Option<FlushReceiver>,
        acknowledger: Ack,
    ) -> Result<ExporterType<'a, Resource, Ack>, BoxError>
    where
        Resource: Clone + Send + Sync + 'static,
        Transformer: TransformPayload<Resource>,
        Ack: Acknowledger<ClickhouseResponse> + Clone + Send + Sync + 'static,
    {
        let client = Client::build(tls::Config::default(), Protocol::Http, Default::default())?;

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
            .timeout(self.request_timeout)
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
            acknowledger,
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

        Ok(ClickhouseResponse::Unknown(str_payload))
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
                Response::Http(parts, body, _) => {
                    match parts.status.as_u16() {
                        200..=202 => Ok(()),
                        404 => Err(format!("Received a 404 when exporting to Clickhouse, does the table exist? (type = {})", self.telemetry_type).into()),
                        _ => match body {
                            Some(body) => Err(format!("Failed to export to Clickhouse (status = {}): {}", parts.status, body).into()),
                            None => Err(format!("Failed to export to Clickhouse (status = {})", parts.status).into()),
                        }
                    }
                },
                Response::Grpc(_, _, _) => Err("Clickhouse invalid response type".to_string().into()),
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
    use crate::exporters::http::acknowledger::{Acknowledger, NoOpAcknowledger};
    use crate::exporters::http::response::Response;
    use crate::topology::payload::MessageMetadata;
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::join;
    use tokio_test::assert_ok;
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    /// Mock acknowledger for testing that tracks whether acknowledgments were received
    #[derive(Clone)]
    pub struct MockAcknowledger {
        acknowledgments: Arc<Mutex<Vec<MessageMetadata>>>,
    }

    impl MockAcknowledger {
        pub fn new() -> Self {
            Self {
                acknowledgments: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn get_acknowledgments(&self) -> Vec<MessageMetadata> {
            self.acknowledgments.lock().unwrap().clone()
        }
    }

    impl<T> Acknowledger<T> for MockAcknowledger
    where
        T: Send + Sync,
    {
        fn acknowledge<'a>(
            &'a self,
            response: &'a Response<T>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                if let Some(metadata_vec) = response.metadata() {
                    let mut acks = self.acknowledgments.lock().unwrap();
                    acks.extend(metadata_vec.clone());
                }
            })
        }
    }

    #[tokio::test]
    async fn traces_success() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("ohi");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let exporter = new_traces_exporter(addr, brx, NoOpAcknowledger);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let traces = FakeOTLP::trace_service_request();
        btx.send(vec![Message {
            metadata: None,
            payload: traces.resource_spans,
        }])
        .await
        .unwrap();
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

        let (btx, brx) = bounded::<Vec<Message<ResourceLogs>>>(100);
        let exporter = new_logs_exporter(addr, brx, NoOpAcknowledger);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let logs = FakeOTLP::logs_service_request();
        btx.send(vec![Message {
            metadata: None,
            payload: logs.resource_logs,
        }])
        .await
        .unwrap();
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

        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);
        let exporter = new_metrics_exporter(addr, brx, NoOpAcknowledger);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            payload: metrics.resource_metrics,
            metadata: None,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_ok!(res.0);

        hello_mock.assert();
    }

    #[tokio::test]
    async fn test_message_acknowledgment_flow_fails() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // Mock Clickhouse endpoint
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("OK");
        });

        // Create a mock acknowledger to verify no metadata flows through
        let mock_acknowledger = MockAcknowledger::new();

        // Create metadata with acknowledgment channel
        let (ack_tx, _ack_rx) = crate::bounded_channel::bounded(1);
        let metadata = MessageMetadata::Kafka(crate::topology::payload::KafkaMetadata {
            offset: 123,
            partition: 0,
            topic_id: 1,
            ack_chan: Some(ack_tx),
        });

        // Create a channel for sending messages with metadata
        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);

        // Build exporter with mock acknowledger
        let exporter = new_traces_exporter(addr, brx, mock_acknowledger.clone());

        // Start exporter
        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();
        let exporter_handle =
            tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        // Send traces with metadata
        let traces = FakeOTLP::trace_service_request();
        btx.send(vec![Message {
            metadata: Some(metadata),
            payload: traces.resource_spans,
        }])
        .await
        .unwrap();

        // Give some time for processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Clean up
        drop(btx);
        cancellation_token.cancel();
        let _ = exporter_handle.await;

        // ASSERTION: This should fail because Clickhouse doesn't propagate metadata yet
        let acknowledgments = mock_acknowledger.get_acknowledgments();
        assert!(
            !acknowledgments.is_empty(),
            "Clickhouse exporter did not receive metadata - metadata flow is not implemented yet!"
        );
    }

    fn new_traces_exporter<'a, Ack>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
        acknowledger: Ack,
    ) -> ExporterType<'a, ResourceSpans, Ack>
    where
        Ack: Acknowledger<ClickhouseResponse> + Clone + Send + Sync + 'static,
    {
        let builder =
            ClickhouseExporterConfigBuilder::new(addr, "otel".to_string(), "otel".to_string())
                .build()
                .unwrap();

        builder
            .build_traces_exporter(brx, None, acknowledger)
            .unwrap()
    }

    fn new_logs_exporter<'a, Ack>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceLogs>>>,
        acknowledger: Ack,
    ) -> ExporterType<'a, ResourceLogs, Ack>
    where
        Ack: Acknowledger<ClickhouseResponse> + Clone + Send + Sync + 'static,
    {
        let builder =
            ClickhouseExporterConfigBuilder::new(addr, "otel".to_string(), "otel".to_string())
                .build()
                .unwrap();

        builder
            .build_logs_exporter(brx, None, acknowledger)
            .unwrap()
    }

    fn new_metrics_exporter<'a, Ack>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
        acknowledger: Ack,
    ) -> ExporterType<'a, ResourceMetrics, Ack>
    where
        Ack: Acknowledger<ClickhouseResponse> + Clone + Send + Sync + 'static,
    {
        let builder =
            ClickhouseExporterConfigBuilder::new(addr, "otel".to_string(), "otel".to_string())
                .build()
                .unwrap();

        builder
            .build_metrics_exporter(brx, None, acknowledger)
            .unwrap()
    }
}
