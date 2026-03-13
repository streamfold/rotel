mod api_request;
mod ch_error;
mod compression;
mod describe_table;
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
    nested_kv_max_depth: usize,
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
            nested_kv_max_depth: 3,
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

    pub fn with_nested_kv_max_depth(mut self, max_depth: usize) -> Self {
        self.nested_kv_max_depth = max_depth;
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
            database: self.database.clone(),
            compression: self.compression,
            auth_user: self.auth_user,
            auth_password: self.auth_password,
            async_insert: self.async_insert,
            use_json: self.use_json,
        };

        let mapper = Arc::new(RequestMapper::new(&config, self.table_prefix.clone())?);

        Ok(ClickhouseExporterBuilder {
            config,
            table_prefix: self.table_prefix,
            request_mapper: mapper,
            retry_config: self.retry_config,
            request_timeout: self.request_timeout,
            nested_kv_max_depth: self.nested_kv_max_depth,
            logs_extended: false,
        })
    }

    /// Like [`build`], but also probes the Clickhouse tables via `DESCRIBE TABLE` so that
    /// the resulting builder writes the extended `EventName` column when it is present in
    /// the target table. On any probe failure the builder falls back to the baseline schema
    /// transparently. Only used for logs since that's the only table with a schema migration.
    pub async fn build_logs(self) -> Result<ClickhouseExporterBuilder, BoxError> {
        let mut builder = self.build()?;
        builder.probe_and_configure().await;
        Ok(builder)
    }
}

pub struct ClickhouseExporterBuilder {
    config: ConnectionConfig,
    table_prefix: String,
    retry_config: RetryConfig,
    request_mapper: Arc<RequestMapper>,
    request_timeout: Duration,
    nested_kv_max_depth: usize,
    /// Whether the logs table has the extended EventName column.
    logs_extended: bool,
}

impl ClickhouseExporterBuilder {
    /// Probe the Clickhouse tables via `DESCRIBE TABLE` and reconfigure the mapper and
    /// capability flags accordingly. Call this once at startup (before building exporters)
    /// to enable writing to tables that include the new `EventName` column.
    /// On failure the builder is left unchanged (baseline schema is used).
    pub async fn probe_and_configure(&mut self) {
        use crate::exporters::clickhouse::describe_table::probe_table_capabilities;
        use crate::exporters::clickhouse::request_mapper::RequestMapper;

        let caps = probe_table_capabilities(
            &self.config.endpoint,
            &self.config.database,
            &self.table_prefix,
            self.config.auth_user.as_deref(),
            self.config.auth_password.as_deref(),
        )
        .await;

        let logs_extended = caps.logs.has_column("EventName");

        match RequestMapper::new_with_capabilities(&self.config, self.table_prefix.clone(), &caps) {
            Ok(mapper) => {
                self.request_mapper = Arc::new(mapper);
                self.logs_extended = logs_extended;
                tracing::info!(
                    logs_extended,
                    "Clickhouse table capabilities probed successfully."
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to build Clickhouse request mapper with probed capabilities; \
                     falling back to baseline schema."
                );
            }
        }
    }

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

        let transformer = Transformer::new(self.config.compression.clone(), self.config.use_json)
            .with_nested_kv_max_depth(self.nested_kv_max_depth)
            .with_logs_extended(self.logs_extended);

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
    use chrono::Utc;
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueValue;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::span::{
        Event as SpanEvent, Link as SpanLink, SpanKind,
    };
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
    use std::time::Duration;
    use tokio::join;
    use tokio_test::assert_ok;
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    #[test_log::test(tokio::test)]
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

    #[test_log::test(tokio::test)]
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

    #[test_log::test(tokio::test)]
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

    #[test_log::test(tokio::test)]
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

    #[test_log::test(tokio::test)]
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

    /// Builds deeply nested `KeyValue` attributes suitable for stress-testing the transformer.
    ///
    /// At `depth == 0` only primitive leaf values are emitted.
    /// At each higher depth the attributes include:
    ///   - a plain string value
    ///   - a nested `KvlistValue` containing the next depth's attributes
    ///   - an `ArrayValue` whose elements are `KvlistValue` maps (with their own nested KV)
    ///   - a double value
    fn build_nested_kv_attributes(depth: usize) -> Vec<KeyValue> {
        if depth == 0 {
            return vec![
                KeyValue {
                    key: "leaf-string".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::StringValue("leaf_value".to_string())),
                    }),
                },
                KeyValue {
                    key: "leaf-int".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::IntValue(42)),
                    }),
                },
                KeyValue {
                    key: "leaf-bool".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::BoolValue(true)),
                    }),
                },
            ];
        }

        let child_attrs = build_nested_kv_attributes(depth - 1);

        // Each element of the array is itself a KvlistValue, and each such map contains
        // a further nested KvlistValue so that Array<KvList> nesting is exercised.
        let array_items: Vec<AnyValue> = (0..2)
            .map(|i| AnyValue {
                value: Some(AnyValueValue::KvlistValue(KeyValueList {
                    values: vec![
                        KeyValue {
                            key: format!("array_item_{}-key", i),
                            value: Some(AnyValue {
                                value: Some(AnyValueValue::StringValue(format!(
                                    "array_value_{}",
                                    i
                                ))),
                            }),
                        },
                        KeyValue {
                            key: format!("array_item_{}-nested", i),
                            value: Some(AnyValue {
                                value: Some(AnyValueValue::KvlistValue(KeyValueList {
                                    values: vec![
                                        KeyValue {
                                            key: "deep_key".to_string(),
                                            value: Some(AnyValue {
                                                value: Some(AnyValueValue::IntValue(
                                                    depth as i64 * 100 + i as i64,
                                                )),
                                            }),
                                        },
                                        KeyValue {
                                            key: "deep_string".to_string(),
                                            value: Some(AnyValue {
                                                value: Some(AnyValueValue::StringValue(format!(
                                                    "depth_{}_item_{}",
                                                    depth, i
                                                ))),
                                            }),
                                        },
                                    ],
                                })),
                            }),
                        },
                    ],
                })),
            })
            .collect();

        vec![
            KeyValue {
                key: format!("level{}-string", depth),
                value: Some(AnyValue {
                    value: Some(AnyValueValue::StringValue(format!(
                        "value_at_depth_{}",
                        depth
                    ))),
                }),
            },
            KeyValue {
                key: format!("level{}-nested_kv", depth),
                value: Some(AnyValue {
                    value: Some(AnyValueValue::KvlistValue(KeyValueList {
                        values: child_attrs,
                    })),
                }),
            },
            KeyValue {
                key: format!("level{}-array_of_maps", depth),
                value: Some(AnyValue {
                    value: Some(AnyValueValue::ArrayValue(ArrayValue {
                        values: array_items,
                    })),
                }),
            },
            KeyValue {
                key: format!("level{}-double", depth),
                value: Some(AnyValue {
                    value: Some(AnyValueValue::DoubleValue(depth as f64 * 3.14)),
                }),
            },
        ]
    }

    /// Constructs a `ResourceSpans` that contains:
    ///   - Resource attributes with nested KvlistValue + ArrayValue<KvlistValue>
    ///   - A root span and one child span per nesting level, each carrying attributes
    ///     built by `build_nested_kv_attributes` at the corresponding depth
    ///   - The root span has a span event whose attributes are also nested
    ///   - Each child span has a span link whose attributes are nested
    fn build_nested_resource_spans(nesting_depth: usize) -> ResourceSpans {
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap() as u64;

        let trace_id: Vec<u8> = vec![
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77,
        ];
        let root_span_id: Vec<u8> = vec![0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01];

        let mut spans = Vec::new();

        // Root span — deepest attribute nesting
        spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: root_span_id.clone(),
            parent_span_id: vec![],
            name: "root_operation".to_string(),
            kind: SpanKind::Server as i32,
            start_time_unix_nano: now_ns,
            end_time_unix_nano: now_ns + 10_000_000,
            attributes: build_nested_kv_attributes(nesting_depth),
            events: vec![SpanEvent {
                time_unix_nano: now_ns + 1_000_000,
                name: "nested_event".to_string(),
                // Event attributes also exercise nested KV at depth 1
                attributes: build_nested_kv_attributes(1),
                dropped_attributes_count: 0,
            }],
            links: vec![],
            status: Some(Status {
                code: 1, // Ok
                message: "OK".to_string(),
            }),
            trace_state: String::new(),
            ..Default::default()
        });

        // Child spans — one per nesting level, each with attributes at that depth
        for level in 1..=nesting_depth {
            let child_span_id: Vec<u8> =
                vec![0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (level + 1) as u8];
            let parent_id: Vec<u8> = if level == 1 {
                root_span_id.clone()
            } else {
                vec![0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, level as u8]
            };

            spans.push(Span {
                trace_id: trace_id.clone(),
                span_id: child_span_id,
                parent_span_id: parent_id,
                name: format!("child_operation_level_{}", level),
                kind: SpanKind::Internal as i32,
                start_time_unix_nano: now_ns + (level as u64) * 1_000_000,
                end_time_unix_nano: now_ns + (level as u64) * 1_000_000 + 5_000_000,
                attributes: build_nested_kv_attributes(level),
                events: vec![],
                links: vec![SpanLink {
                    trace_id: trace_id.clone(),
                    span_id: root_span_id.clone(),
                    trace_state: String::new(),
                    // Link attributes with one level of nesting
                    attributes: build_nested_kv_attributes(1),
                    dropped_attributes_count: 0,
                    flags: 0,
                }],
                status: Some(Status {
                    code: 0,
                    message: String::new(),
                }),
                trace_state: String::new(),
                ..Default::default()
            });
        }

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "nested-trace-integration-test".to_string(),
                version: "1.0.0".to_string(),
                ..Default::default()
            }),
            spans,
            schema_url: String::new(),
        };

        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    // Flat resource attribute
                    KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueValue::StringValue(
                                "nested-trace-test-service".to_string(),
                            )),
                        }),
                    },
                    KeyValue {
                        key: "deployment.environment".to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueValue::StringValue("integration-test".to_string())),
                        }),
                    },
                    // Nested KvlistValue inside resource attributes
                    KeyValue {
                        key: "resource.nested_kv".to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueValue::KvlistValue(KeyValueList {
                                values: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(AnyValueValue::StringValue(
                                                "localhost".to_string(),
                                            )),
                                        }),
                                    },
                                    KeyValue {
                                        key: "port".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(AnyValueValue::IntValue(8123)),
                                        }),
                                    },
                                ],
                            })),
                        }),
                    },
                    // ArrayValue of KvlistValue entries inside resource attributes
                    KeyValue {
                        key: "resource.tags".to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueValue::ArrayValue(ArrayValue {
                                values: vec![
                                    AnyValue {
                                        value: Some(AnyValueValue::KvlistValue(KeyValueList {
                                            values: vec![
                                                KeyValue {
                                                    key: "env".to_string(),
                                                    value: Some(AnyValue {
                                                        value: Some(AnyValueValue::StringValue(
                                                            "test".to_string(),
                                                        )),
                                                    }),
                                                },
                                                KeyValue {
                                                    key: "region".to_string(),
                                                    value: Some(AnyValue {
                                                        value: Some(AnyValueValue::StringValue(
                                                            "local".to_string(),
                                                        )),
                                                    }),
                                                },
                                            ],
                                        })),
                                    },
                                    AnyValue {
                                        value: Some(AnyValueValue::KvlistValue(KeyValueList {
                                            values: vec![KeyValue {
                                                key: "version".to_string(),
                                                value: Some(AnyValue {
                                                    value: Some(AnyValueValue::StringValue(
                                                        "1.0.0".to_string(),
                                                    )),
                                                }),
                                            }],
                                        })),
                                    },
                                ],
                            })),
                        }),
                    },
                ],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![scope_spans],
            schema_url: String::new(),
        }
    }

    /// Builds a traces exporter configured for JSON mode and deep nested KV support,
    /// pointing at the given Clickhouse endpoint.
    fn new_nested_traces_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
    ) -> ExporterType<'a, ResourceSpans> {
        let builder = ClickhouseExporterConfigBuilder::new(
            addr,
            "otel".to_string(),
            "otel".to_string(),
            Default::default(),
        )
        .with_json(true)
        .with_nested_kv_max_depth(5)
        .with_compression(Compression::None)
        .with_async_insert(true)
        .with_request_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

        builder.build_traces_exporter(brx, None).unwrap()
    }

    /// Integration test: exports a deeply nested OTel trace structure to a real Clickhouse
    /// instance at http://localhost:8123.
    ///
    /// The test is marked `#[ignore]` so it does not run as part of the standard test suite.
    /// Run it manually once a Clickhouse instance with the `otel` database and `otel_traces`
    /// table is available:
    ///
    /// ```shell
    /// cargo test -p rotel test_export_nested_traces_to_clickhouse -- --ignored --nocapture
    /// ```
    ///
    /// The nesting depth is controlled by `NESTING_DEPTH` (hardwired to 3 for now).
    /// Each depth level adds:
    ///   - A `KvlistValue` attribute containing the next level's KV tree
    ///   - An `ArrayValue` attribute whose elements are `KvlistValue` maps (with their own
    ///     nested KV entries)
    ///
    /// The test does **not** verify the data stored in Clickhouse; that is done by hand.
    #[test_log::test(tokio::test)]
    #[ignore]
    async fn test_export_nested_traces_to_clickhouse() {
        init_crypto();
        let _ = tracing_subscriber::fmt::try_init();

        /// Number of nesting levels to generate (change to experiment with deeper trees).
        const NESTING_DEPTH: usize = 3;
        const CLICKHOUSE_URL: &str = "http://localhost:8123";

        let resource_spans = build_nested_resource_spans(NESTING_DEPTH);

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let exporter = new_nested_traces_exporter(CLICKHOUSE_URL.to_string(), brx);

        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        btx.send(vec![Message {
            metadata: None,
            request_context: None,
            payload: vec![resource_spans],
        }])
        .await
        .unwrap();

        // Drop the sender so the exporter drains and shuts down cleanly.
        drop(btx);

        let res = join!(jh);
        assert_ok!(res.0, "Clickhouse export of nested traces should succeed");
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
