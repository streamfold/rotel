// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::http::acknowledger::DefaultHTTPAcknowledger;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use crate::exporters::shared::aws_signing_service::{AwsSigningService, AwsSigningServiceBuilder};
use crate::exporters::xray::request_builder::RequestBuilder;
use crate::exporters::xray::transformer::Transformer;

use crate::aws_api::config::AwsConfig;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::client::{Client, Protocol};
use crate::exporters::http::exporter::Exporter;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::request_iter::RequestIterator;
use crate::exporters::http::tls;
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::FlushReceiver;
use crate::topology::payload::Message;
use bytes::Bytes;
use flume::r#async::RecvStream;
use http::Request;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::time::Duration;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, ServiceBuilder};

use super::http::finalizer::SuccessStatusFinalizer;
use super::shared::aws::Region;

mod request_builder;
mod transformer;
mod xray_request;

/// Type alias for XRay payloads using the generic MessagePayload
use crate::exporters::http::metadata_extractor::MessagePayload;
use http_body_util::Full;
pub type XRayPayload = MessagePayload<Full<Bytes>>;

type SvcType<RespBody> = TowerRetry<
    RetryPolicy<RespBody>,
    AwsSigningService<Timeout<Client<XRayPayload, RespBody, XRayTraceDecoder>>>,
>;

type ExporterType<'a, Resource> = Exporter<
    RequestIterator<
        RequestBuilderMapper<
            RecvStream<'a, Vec<Message<Resource>>>,
            Resource,
            XRayPayload,
            RequestBuilder<Resource, Transformer>,
        >,
        Vec<Request<XRayPayload>>,
        XRayPayload,
    >,
    SvcType<String>,
    XRayPayload,
    SuccessStatusFinalizer,
    DefaultHTTPAcknowledger,
>;

pub struct XRayExporterConfigBuilder {
    region: Region,
    custom_endpoint: Option<String>,
    retry_config: RetryConfig,
}

impl Default for XRayExporterConfigBuilder {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            custom_endpoint: None,
            retry_config: Default::default(),
        }
    }
}

impl XRayExporterConfigBuilder {
    pub fn new(region: Region, custom_endpoint: Option<String>) -> Self {
        Self {
            region,
            custom_endpoint,
            ..Default::default()
        }
    }

    #[allow(dead_code)]
    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub fn build(self) -> XRayExporterBuilder {
        XRayExporterBuilder {
            region: self.region,
            custom_endpoint: self.custom_endpoint,
            retry_config: self.retry_config,
        }
    }
}

pub struct XRayExporterBuilder {
    region: Region,
    custom_endpoint: Option<String>,
    retry_config: RetryConfig,
}

impl XRayExporterBuilder {
    pub fn build<'a>(
        self,
        rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
        flush_receiver: Option<FlushReceiver>,
        environment: String,
        aws_config: AwsConfig,
    ) -> Result<ExporterType<'a, ResourceSpans>, BoxError> {
        let client = Client::build(tls::Config::default(), Protocol::Http, Default::default())?;
        let transformer = Transformer::new(environment);

        let req_builder =
            RequestBuilder::new(transformer, self.region, self.custom_endpoint.clone())?;

        let retry_layer = RetryPolicy::new(self.retry_config, None);
        let retry_broadcast = retry_layer.retry_broadcast();

        let region = self.region.to_string();

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .layer_fn(|inner| {
                AwsSigningServiceBuilder::new("xray", region.as_str(), aws_config.clone())
                    .build(inner)
            })
            .timeout(Duration::from_secs(5))
            .service(client);

        let enc_stream =
            RequestIterator::new(RequestBuilderMapper::new(rx.into_stream(), req_builder));

        let exp = Exporter::new(
            "x-ray",
            "traces",
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

#[derive(Default, Clone)]
pub struct XRayTraceDecoder;

impl ResponseDecode<String> for XRayTraceDecoder {
    fn decode(&self, _: Bytes, _: ContentEncoding) -> Result<String, BoxError> {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use crate::aws_api::config::AwsConfig;
    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::exporters::crypto_init_tests::init_crypto;
    use crate::exporters::http::retry::RetryConfig;
    use crate::exporters::xray::{ExporterType, Region, XRayExporterConfigBuilder};
    use crate::topology::payload::{Message, MessageMetadata};
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
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
            when.method(POST).path("/TraceSegments");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body("ohi");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let config = AwsConfig::from_env();
        let exporter = new_exporter(addr, brx, config);

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

        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/TraceSegments");
            then.status(429)
                .header("content-type", "application/x-protobuf")
                .body("hold up");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let config = AwsConfig::from_env();
        let exporter = new_exporter(addr, brx, config);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let traces = FakeOTLP::trace_service_request();
        btx.send(vec![Message {
            metadata: None,
            payload: traces.resource_spans,
        }])
        .await
        .unwrap();
        drop(btx);
        let res = join!(jh);
        assert_err!(res.0.unwrap()); // failed to drain

        assert!(hello_mock.hits() >= 3); // somewhat timing dependent
    }

    #[tokio::test]
    async fn test_message_acknowledgment_flow() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // Mock XRay endpoint
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/TraceSegments");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body("ok");
        });

        // Create acknowledgment channel for real acknowledgment flow
        let (ack_tx, mut ack_rx) = crate::bounded_channel::bounded(1);

        // Create metadata with real acknowledgment channel
        let metadata = MessageMetadata::Kafka(crate::topology::payload::KafkaMetadata {
            offset: 123,
            partition: 0,
            topic_id: 1,
            ack_chan: Some(ack_tx),
        });

        // Create a channel for sending messages with metadata
        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);

        // Use DefaultHTTPAcknowledger to test real acknowledgment flow
        let config = AwsConfig::from_env();
        let exporter = new_exporter(addr, brx, config);

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

        // ASSERTION: This should pass since XRay now properly acknowledges messages
        assert!(
            ack_received,
            "Message was not acknowledged by XRay exporter - real acknowledgment flow failed!"
        );
    }

    fn new_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
        config: AwsConfig,
    ) -> ExporterType<'a, ResourceSpans> {
        XRayExporterConfigBuilder::new(Region::UsEast1, Some(addr))
            .with_retry_config(RetryConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                max_elapsed_time: Duration::from_millis(50),
            })
            .build()
            .build(brx, None, "production".to_string(), config)
            .unwrap()
    }
}
