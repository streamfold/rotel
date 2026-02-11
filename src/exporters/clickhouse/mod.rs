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
use crate::exporters::http::acknowledger::DefaultHTTPAcknowledger;
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

#[derive(Clone)]
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
    request_timeout: Duration,
}

type SvcType<RespBody> = TowerRetry<
    RetryPolicy<RespBody>,
    Timeout<Client<ClickhousePayload, RespBody, ClickhouseRespDecoder>>,
>;

pub type ExporterType<'a, Resource> = Exporter<
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
    DefaultHTTPAcknowledger,
>;

impl ClickhouseExporterConfigBuilder {
    pub fn new(
        endpoint: String,
        database: String,
        table_prefix: String,
        retry_config: RetryConfig,
    ) -> ClickhouseExporterConfigBuilder {
        ClickhouseExporterConfigBuilder {
            retry_config,
            endpoint,
            database,
            table_prefix,
            auth_user: None,
            auth_password: None,
            request_timeout: Duration::from_secs(5),
            compression: Default::default(),
            use_json: false,
            async_insert: false,
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

    pub fn set_indefinite_retry(&mut self) {
        self.retry_config.indefinite_retry = true;
    }

    #[cfg(test)]
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
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
    pub fn build_traces_exporter<'a>(
        &self,
        rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceSpans>, BoxError> {
        self.build_exporter("traces", rx, flush_receiver)
    }

    pub fn build_logs_exporter<'a>(
        &self,
        rx: BoundedReceiver<Vec<Message<ResourceLogs>>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceLogs>, BoxError> {
        self.build_exporter("logs", rx, flush_receiver)
    }

    pub fn build_metrics_exporter<'a>(
        &self,
        rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceMetrics>, BoxError> {
        self.build_exporter("metrics", rx, flush_receiver)
    }

    fn build_exporter<'a, Resource>(
        &self,
        telemetry_type: &'static str,
        rx: BoundedReceiver<Vec<Message<Resource>>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, Resource>, BoxError>
    where
        Resource: Clone + Send + Sync + 'static,
        Transformer: TransformPayload<Resource>,
    {
        use crate::exporters::http::client::{
            DEFAULT_POOL_IDLE_TIMEOUT, DEFAULT_POOL_MAX_IDLE_PER_HOST,
        };
        let client = Client::build(
            tls::Config::default(),
            Protocol::Http,
            Default::default(),
            DEFAULT_POOL_IDLE_TIMEOUT,
            DEFAULT_POOL_MAX_IDLE_PER_HOST,
        )?;

        let transformer = Transformer::new(self.config.compression.clone(), self.config.use_json);

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
            DefaultHTTPAcknowledger::default(),
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
    use crate::topology::payload::{KafkaAcknowledgement, KafkaMetadata, Message, MessageMetadata};
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use std::time::Duration;
    use tokio::join;
    use tokio_test::assert_ok;
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

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let exporter = new_traces_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let traces = FakeOTLP::trace_service_request();
        btx.send(vec![Message {
            metadata: None,
            request_context: None,
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
        let exporter = new_logs_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let logs = FakeOTLP::logs_service_request();
        btx.send(vec![Message {
            metadata: None,
            request_context: None,
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
        let exporter = new_metrics_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            payload: metrics.resource_metrics,
            metadata: None,
            request_context: None,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_ok!(res.0);

        hello_mock.assert();
    }

    #[tokio::test]
    async fn test_message_acknowledgment_flow() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // Mock Clickhouse endpoint
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("OK");
        });

        // Create metadata with acknowledgment channel
        let (ack_tx, mut ack_rx) = crate::bounded_channel::bounded(1);
        let expected_offset = 123;
        let expected_partition = 0;
        let expected_topic_id = 1;
        let metadata = MessageMetadata::kafka(crate::topology::payload::KafkaMetadata {
            offset: expected_offset,
            partition: expected_partition,
            topic_id: expected_topic_id,
            ack_chan: Some(ack_tx),
        });

        // Create a channel for sending messages with metadata
        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);

        // Build exporter with DefaultHTTPAcknowledger
        let exporter = new_traces_exporter(addr, brx);

        // Start exporter
        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();
        let exporter_handle =
            tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        // Send traces with metadata
        let traces = FakeOTLP::trace_service_request();
        btx.send(vec![Message {
            metadata: Some(metadata),
            request_context: None,
            payload: traces.resource_spans,
        }])
        .await
        .unwrap();

        // Wait for acknowledgment
        let received_ack = tokio::time::timeout(Duration::from_secs(5), ack_rx.next())
            .await
            .expect("Timeout waiting for acknowledgment")
            .expect("Failed to receive acknowledgment");

        // Verify the acknowledgment contains the expected information
        match received_ack {
            crate::topology::payload::KafkaAcknowledgement::Ack(ack) => {
                assert_eq!(ack.offset, expected_offset, "Offset should match");
                assert_eq!(ack.partition, expected_partition, "Partition should match");
                assert_eq!(ack.topic_id, expected_topic_id, "Topic ID should match");
            }
            crate::topology::payload::KafkaAcknowledgement::Nack(_) => {
                panic!("Received Nack instead of Ack");
            }
        }

        // Clean up
        drop(btx);
        cancellation_token.cancel();
        let _ = exporter_handle.await;
    }

    #[tokio::test]
    async fn test_multi_batch_acknowledgment_flow() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // Mock Clickhouse endpoint that accepts multiple requests
        let clickhouse_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("OK");
        });

        // Create acknowledgment channel
        let (ack_tx, mut ack_rx) = bounded(10);
        let expected_offset = 456;
        let expected_partition = 2;
        let expected_topic_id = 3;
        let metadata = MessageMetadata::kafka(KafkaMetadata {
            offset: expected_offset,
            partition: expected_partition,
            topic_id: expected_topic_id,
            ack_chan: Some(ack_tx),
        });

        // Create a channel for sending messages with metadata
        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);

        // Build exporter with DefaultHTTPAcknowledger
        let exporter = new_traces_exporter(addr, brx);

        // Start exporter
        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();
        let exporter_handle =
            tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        // Create a large amount of trace data that could potentially trigger
        // multiple internal chunks or payloads in Clickhouse
        let mut all_resource_spans = Vec::new();

        // 100 iterations * 10 ResourceSpans = 1000 ResourceSpans (was 10,000)
        for _i in 0..100 {
            let traces = FakeOTLP::trace_service_request_with_spans(10, 50); // 10 ResourceSpans, 50 spans each
            all_resource_spans.extend(traces.resource_spans);
        }

        // Send traces with metadata
        btx.send(vec![Message {
            metadata: Some(metadata),
            request_context: None,
            payload: all_resource_spans,
        }])
        .await
        .unwrap();

        // Wait for the expected acknowledgment with timeout
        let (ack_count, received_acks) =
            match tokio::time::timeout(Duration::from_millis(10000), ack_rx.next()).await {
                Ok(Some(ack)) => (1, vec![ack]),
                Ok(None) => {
                    // Channel closed without receiving acknowledgment
                    (0, vec![])
                }
                Err(_) => {
                    // Timeout occurred
                    (0, vec![])
                }
            };

        // Verify acknowledgment behavior BEFORE cleanup
        for ack in received_acks.iter() {
            match ack {
                KafkaAcknowledgement::Ack(kafka_ack) => {
                    assert_eq!(kafka_ack.offset, expected_offset);
                    assert_eq!(kafka_ack.partition, expected_partition);
                    assert_eq!(kafka_ack.topic_id, expected_topic_id);
                }
                KafkaAcknowledgement::Nack(_) => {
                    panic!("Received Nack instead of Ack");
                }
            }
        }

        assert_eq!(
            ack_count, 1,
            "Expected exactly 1 acknowledgment for Clickhouse request, got {}",
            ack_count
        );

        // Clean up AFTER verification
        drop(btx);

        // Give exporter a moment to finish any in-flight processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        cancellation_token.cancel();
        let _ = exporter_handle.await;

        // Verify the server received requests
        let _actual_hits = clickhouse_mock.hits();
    }

    fn new_traces_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
    ) -> ExporterType<'a, ResourceSpans> {
        let builder = ClickhouseExporterConfigBuilder::new(
            addr,
            "otel".to_string(),
            "otel".to_string(),
            Default::default(),
        )
        .build()
        .unwrap();

        builder.build_traces_exporter(brx, None).unwrap()
    }

    fn new_logs_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceLogs>>>,
    ) -> ExporterType<'a, ResourceLogs> {
        let builder = ClickhouseExporterConfigBuilder::new(
            addr,
            "otel".to_string(),
            "otel".to_string(),
            Default::default(),
        )
        .build()
        .unwrap();

        builder.build_logs_exporter(brx, None).unwrap()
    }

    fn new_metrics_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
    ) -> ExporterType<'a, ResourceMetrics> {
        let builder = ClickhouseExporterConfigBuilder::new(
            addr,
            "otel".to_string(),
            "otel".to_string(),
            Default::default(),
        )
        .build()
        .unwrap();

        builder.build_metrics_exporter(brx, None).unwrap()
    }
}
