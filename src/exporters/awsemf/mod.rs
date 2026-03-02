// SPDX-License-Identifier: Apache-2.0

use crate::aws_api::creds::AwsCredsProvider;
use crate::bounded_channel::BoundedReceiver;
use crate::exporters::awsemf::request_builder::RequestBuilder;
use crate::exporters::awsemf::response_interceptor::ResponseInterceptor;
use crate::exporters::awsemf::transformer::Transformer;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};

use crate::exporters::http::client::{Client, Protocol};
use crate::exporters::http::exporter::Exporter;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::request_iter::RequestIterator;
use crate::exporters::http::tls;
use crate::exporters::shared::aws_signing_service::{AwsSigningService, AwsSigningServiceBuilder};
use crate::topology::flush_control::FlushReceiver;

use bytes::Bytes;
use dim_filter::DimensionFilter;
use errors::{AwsEmfDecoder, AwsEmfResponse, is_retryable_error};
use flume::r#async::RecvStream;
use http::Request;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use std::sync::Arc;
use std::time::Duration;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, ServiceBuilder};

use super::http::acknowledger::DefaultHTTPAcknowledger;
use super::http::finalizer::SuccessStatusFinalizer;
use super::shared::aws::Region;

mod cloudwatch;
mod dim_filter;
mod emf_request;
mod errors;
mod event;
mod request_builder;
mod response_interceptor;
mod transformer;

use crate::topology::payload::Message;
use cloudwatch::Cloudwatch;

/// Type alias for AWS EMF payloads using the generic MessagePayload
use crate::exporters::http::metadata_extractor::MessagePayload;
use http_body_util::Full;

pub type AwsEmfPayload = MessagePayload<Full<Bytes>>;

type SvcType<RespBody> = TowerRetry<
    RetryPolicy<RespBody>,
    ResponseInterceptor<AwsSigningService<Timeout<Client<AwsEmfPayload, RespBody, AwsEmfDecoder>>>>,
>;

type ExporterType<'a, Resource, Ack> = Exporter<
    RequestIterator<
        RequestBuilderMapper<
            RecvStream<'a, Vec<Message<Resource>>>,
            Resource,
            AwsEmfPayload,
            RequestBuilder<Resource, Transformer>,
        >,
        Vec<Request<AwsEmfPayload>>,
        AwsEmfPayload,
    >,
    SvcType<AwsEmfResponse>,
    AwsEmfPayload,
    SuccessStatusFinalizer,
    Ack,
>;

#[derive(Clone)]
pub struct AwsEmfExporterConfig {
    pub region: Region,
    pub log_group_name: String,
    pub log_stream_name: String,
    pub log_retention: u16,
    pub namespace: Option<String>,
    pub custom_endpoint: Option<String>,
    pub retain_initial_value_of_delta_metric: bool,
    pub retry_config: RetryConfig,
    pub include_dimensions: Vec<String>,
    pub exclude_dimensions: Vec<String>,
}

pub struct AwsEmfExporterConfigBuilder {
    config: AwsEmfExporterConfig,
}

impl AwsEmfExporterConfigBuilder {
    pub fn new(retry_config: RetryConfig) -> Self {
        Self {
            config: AwsEmfExporterConfig {
                region: Region::UsEast1,
                log_group_name: "/metrics/default".to_string(),
                log_stream_name: "otel-stream".to_string(),
                log_retention: 0,
                namespace: None,
                custom_endpoint: None,
                retain_initial_value_of_delta_metric: false,
                retry_config,
                include_dimensions: Vec::new(),
                exclude_dimensions: Vec::new(),
            },
        }
    }

    pub fn with_region(mut self, region: Region) -> Self {
        self.config.region = region;
        self
    }

    pub fn with_log_group_name<S: Into<String>>(mut self, log_group_name: S) -> Self {
        self.config.log_group_name = log_group_name.into();
        self
    }

    pub fn with_log_stream_name<S: Into<String>>(mut self, log_stream_name: S) -> Self {
        self.config.log_stream_name = log_stream_name.into();
        self
    }

    pub fn with_log_retention(mut self, log_retention: u16) -> Self {
        self.config.log_retention = log_retention;
        self
    }

    pub fn with_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.config.namespace = Some(namespace.into());
        self
    }

    pub fn with_custom_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.config.custom_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_retain_initial_value_of_delta_metric(mut self, retain: bool) -> Self {
        self.config.retain_initial_value_of_delta_metric = retain;
        self
    }

    pub fn set_indefinite_retry(&mut self) {
        self.config.retry_config.indefinite_retry = true;
    }

    pub fn with_include_dimensions(mut self, include_dimensions: Vec<String>) -> Self {
        self.config.include_dimensions = include_dimensions;
        self
    }

    pub fn with_exclude_dimensions(mut self, exclude_dimensions: Vec<String>) -> Self {
        self.config.exclude_dimensions = exclude_dimensions;
        self
    }

    pub fn build(self) -> AwsEmfExporterBuilder {
        AwsEmfExporterBuilder {
            config: self.config,
        }
    }
}

pub struct AwsEmfExporterBuilder {
    config: AwsEmfExporterConfig,
}

impl AwsEmfExporterBuilder {
    pub(crate) fn build<'a>(
        self,
        rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
        flush_receiver: Option<FlushReceiver>,
        aws_creds_provider: AwsCredsProvider,
    ) -> Result<ExporterType<'a, ResourceMetrics, DefaultHTTPAcknowledger>, BoxError> {
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
        let dim_filter = Arc::new(DimensionFilter::new(
            self.config.include_dimensions.clone(),
            self.config.exclude_dimensions.clone(),
        )?);
        let transformer = Transformer::new(self.config.clone(), dim_filter);

        let req_builder = RequestBuilder::new(transformer, self.config.clone())?;

        let retry_layer = RetryPolicy::new(self.config.retry_config, Some(is_retryable_error));
        let retry_broadcast = retry_layer.retry_broadcast();

        let region = self.config.region.to_string();

        let signing_builder =
            AwsSigningServiceBuilder::new("logs", region.as_str(), aws_creds_provider);

        // Create CloudWatch API instance
        let cloudwatch_api = Arc::new(Cloudwatch::new(
            signing_builder.clone(),
            region.clone(),
            self.config.custom_endpoint.clone(),
            self.config.log_group_name.clone(),
            self.config.log_stream_name.clone(),
            self.config.log_retention,
        )?);

        // Build service stack: retry -> response_interceptor -> timeout -> client
        let timeout_client = ServiceBuilder::new()
            .layer_fn(|inner| signing_builder.clone().build(inner))
            .timeout(Duration::from_secs(5))
            .service(client);

        let interceptor_service = ResponseInterceptor::new(timeout_client, cloudwatch_api);

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .service(interceptor_service);

        let enc_stream =
            RequestIterator::new(RequestBuilderMapper::new(rx.into_stream(), req_builder));

        let exp = Exporter::new(
            "awsemf",
            "metrics",
            enc_stream,
            svc,
            SuccessStatusFinalizer::default(),
            DefaultHTTPAcknowledger::default(),
            flush_receiver,
            retry_broadcast,
            Duration::from_secs(1),
            Duration::from_secs(2),
        );

        Ok(exp)
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use crate::aws_api::creds::{AwsCreds, AwsCredsProvider};
    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::exporters::awsemf::{AwsEmfExporterConfigBuilder, ExporterType};
    use crate::exporters::crypto_init_tests::init_crypto;
    use crate::exporters::http::retry::RetryConfig;
    use crate::exporters::shared::aws::Region;
    use crate::topology::payload::{KafkaAcknowledgement, KafkaMetadata, Message, MessageMetadata};
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use std::time::Duration;
    use tokio::join;
    use tokio_test::{assert_err, assert_ok};
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn success_and_retry() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body("{}");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);
        let exporter = new_exporter(addr, brx, None);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            metadata: None,
            request_context: None,
            payload: metrics.resource_metrics,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_ok!(res.0.unwrap());

        hello_mock.assert();

        // Test retry scenario with 429 status
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(429)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"__type":"ServiceUnavailableException","message":"Rate exceeded"}"#);
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);
        let exporter = new_exporter(addr, brx, None);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            metadata: None,
            request_context: None,
            payload: metrics.resource_metrics,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_err!(res.0.unwrap()); // failed to drain

        assert!(hello_mock.hits() >= 3); // somewhat timing dependent
    }

    #[tokio::test]
    async fn resource_not_found_triggers_creation() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // First request returns ResourceNotFoundException
        let resource_not_found_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.PutLogEvents");
            then.status(400)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"__type":"ResourceNotFoundException","message":"The specified log group does not exist."}"#);
        });

        // Mock for creating log stream
        let create_log_stream_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.CreateLogStream");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body("{}");
        });

        // Because create log stream succeeds, the following will not be called

        // Mock for creating log group
        let create_log_group_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.CreateLogGroup");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body("{}");
        });

        // Retention mock
        let retention_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.PutRetentionPolicy");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"nextSequenceToken":"12345"}"#);
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);
        let exporter = new_exporter(addr, brx, None);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            metadata: None,
            request_context: None,
            payload: metrics.resource_metrics,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_err!(res.0.unwrap());

        // Verify the calls made
        assert!(resource_not_found_mock.hits() > 0);
        assert!(create_log_stream_mock.hits() > 0);
        create_log_group_mock.assert_hits(0);
        retention_mock.assert_hits(0);
    }

    #[tokio::test]
    async fn resource_already_exists_is_handled() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // First request returns ResourceNotFoundException
        let put_log_events_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.PutLogEvents");
            then.status(400)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"__type":"ResourceNotFoundException","message":"The specified log stream does not exist."}"#);
        });

        // Mock for creating log stream that returns resource already exists
        let create_log_stream_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.CreateLogStream");
            then.status(400)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"__type":"ResourceAlreadyExistsException","message":"The specified log stream already exists."}"#);
        });

        // should not be hit
        let create_log_group_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.CreateLogGroup");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body("{}");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);
        let exporter = new_exporter(addr, brx, None);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            metadata: None,
            request_context: None,
            payload: metrics.resource_metrics,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_err!(res.0.unwrap());

        // Verify the calls were made
        assert!(put_log_events_mock.hits() > 0);
        assert!(create_log_stream_mock.hits() > 0);
        create_log_group_mock.assert_hits(0);
    }

    #[tokio::test]
    async fn log_group_retention() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // First request returns ResourceNotFoundException
        let put_logs_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.PutLogEvents");
            then.status(400)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"__type":"ResourceNotFoundException","message":"The specified log stream does not exist."}"#);
        });

        // Mock for creating log stream
        let create_log_stream_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.CreateLogStream");
            then.status(400)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"__type":"ResourceNotFoundException","message":"The specified log group does not exist."}"#);
        });

        // Mock for creating log group
        let create_log_group_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.CreateLogGroup");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body("{}");
        });

        // Retention mock
        let retention_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/")
                .header("x-amz-target", "Logs_20140328.PutRetentionPolicy");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body(r#"{"nextSequenceToken":"12345"}"#);
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);
        let exporter = new_exporter(addr, brx, Some(3));

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            metadata: None,
            request_context: None,
            payload: metrics.resource_metrics,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_err!(res.0.unwrap());

        // Verify the calls were made
        assert!(put_logs_mock.hits() > 0);
        assert!(create_log_stream_mock.hits() > 0);
        assert!(create_log_group_mock.hits() > 0);
        assert!(retention_mock.hits() > 0);
    }

    #[tokio::test]
    async fn test_message_acknowledgment_flow() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // Mock AWS EMF endpoint
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body("{}");
        });

        // Create acknowledgment channel for real acknowledgment flow
        let (ack_tx, mut ack_rx) = crate::bounded_channel::bounded(1);

        // Create metadata with real acknowledgment channel
        let metadata = MessageMetadata::kafka(crate::topology::payload::KafkaMetadata {
            offset: 123,
            partition: 0,
            topic_id: 1,
            ack_chan: Some(ack_tx),
        });

        // Create a channel for sending messages with metadata
        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);

        // Use DefaultHTTPAcknowledger to test real acknowledgment flow
        let exporter = new_exporter(addr, brx, None);

        // Start exporter
        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();
        let exporter_handle =
            tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        // Send metrics with metadata
        let metrics = FakeOTLP::metrics_service_request();
        btx.send(vec![Message {
            metadata: Some(metadata),
            request_context: None,
            payload: metrics.resource_metrics,
        }])
        .await
        .unwrap();

        // Wait for acknowledgment or timeout
        let ack_received = tokio::select! {
            result = ack_rx.next() => {
                match result {
                    Some(_) => true,  // Acknowledgment received
                    None => false,    // Channel closed without acknowledgment
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(5000)) => false, // Timeout
        };

        // Clean up
        drop(btx);
        cancellation_token.cancel();
        let _ = exporter_handle.await;

        // ASSERTION: This should pass since AWS EMF now properly acknowledges messages
        assert!(
            ack_received,
            "Message was not acknowledged by AWS EMF exporter - real acknowledgment flow failed!"
        );
    }

    #[tokio::test]
    async fn test_multi_batch_acknowledgment_flow() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // Mock server that accepts all requests
        let put_logs_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                .header("content-type", "application/x-amz-json-1.1")
                .body("{}");
        });

        // Create acknowledgment channel
        let (ack_tx, mut ack_rx) = bounded(10);
        let expected_offset = 123;
        let expected_partition = 1;
        let expected_topic_id = 2;
        let metadata = MessageMetadata::kafka(KafkaMetadata {
            offset: expected_offset,
            partition: expected_partition,
            topic_id: expected_topic_id,
            ack_chan: Some(ack_tx),
        });

        // Create exporter
        let (btx, brx) = bounded::<Vec<Message<ResourceMetrics>>>(100);
        let exporter = new_exporter(addr, brx, None);

        // Start exporter
        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();
        let exporter_handle =
            tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        // Create many metrics with different dimensions to prevent grouping
        // This should result in many individual EMF events that will need to be split into multiple batches
        let mut all_resource_metrics = Vec::new();

        // Generate 15,000 metrics with unique dimension combinations to exceed MAX_BATCH_EVENTS (10,000)
        for i in 0..15000 {
            let mut fake_metrics = FakeOTLP::metrics_service_request_with_metrics(1, 1);

            // Add unique resource attributes to prevent grouping
            if let Some(ref mut rm) = fake_metrics.resource_metrics.get_mut(0) {
                if let Some(ref mut resource) = rm.resource {
                    resource.attributes.push(opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "unique_dimension".to_string(),
                        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                format!("value_{}", i)
                            ))
                        })
                    });
                }
            }

            all_resource_metrics.extend(fake_metrics.resource_metrics);
        }

        btx.send(vec![Message {
            metadata: Some(metadata),
            request_context: None,
            payload: all_resource_metrics,
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

        // Clean up
        drop(btx);
        cancellation_token.cancel();
        let _ = exporter_handle.await;

        // Verify the server received multiple requests due to batch splitting
        let actual_hits = put_logs_mock.hits();
        assert!(
            actual_hits > 1,
            "Expected multiple HTTP requests for 15,000 metrics, got {}",
            actual_hits
        );

        // Verify acknowledgment behavior
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
            "Expected exactly 1 acknowledgment for multi-batch EMF request, got {}",
            ack_count
        );
    }

    fn new_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
        log_retention: Option<u16>,
    ) -> ExporterType<
        'a,
        ResourceMetrics,
        crate::exporters::http::acknowledger::DefaultHTTPAcknowledger,
    > {
        let mut builder = AwsEmfExporterConfigBuilder::new(RetryConfig::new(
            Duration::from_millis(10),
            Duration::from_millis(50),
            Duration::from_millis(50),
            false,
        ))
        .with_region(Region::UsEast1)
        .with_custom_endpoint(addr)
        .with_log_group_name("test-log-group".to_string())
        .with_log_stream_name("test-log-stream".to_string());

        if let Some(log_retention) = &log_retention {
            builder = builder.with_log_retention(*log_retention);
        }

        let creds_provider =
            AwsCredsProvider::from_static(AwsCreds::new("".to_string(), "".to_string(), None));

        builder.build().build(brx, None, creds_provider).unwrap()
    }
}
