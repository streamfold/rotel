// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::datadog::request_builder::RequestBuilder;
use crate::exporters::datadog::transform::Transformer;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};

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

use super::http::acknowledger::DefaultHTTPAcknowledger;
use super::http::finalizer::SuccessStatusFinalizer;

mod api_request;
mod request_builder;
mod transform;
mod types;

/// Type alias for Datadog payloads using the generic MessagePayload
use crate::exporters::http::metadata_extractor::MessagePayload;
use http_body_util::Full;
pub type DatadogPayload = MessagePayload<Full<Bytes>>;

type SvcType<RespBody> = TowerRetry<
    RetryPolicy<RespBody>,
    Timeout<Client<DatadogPayload, RespBody, DatadogTraceDecoder>>,
>;

type ExporterType<'a, Resource> = Exporter<
    RequestIterator<
        RequestBuilderMapper<
            RecvStream<'a, Vec<Message<Resource>>>,
            Resource,
            DatadogPayload,
            RequestBuilder<Resource, Transformer>,
        >,
        Vec<Request<DatadogPayload>>,
        DatadogPayload,
    >,
    SvcType<String>,
    DatadogPayload,
    SuccessStatusFinalizer,
    DefaultHTTPAcknowledger,
>;

#[derive(Copy, Clone)]
pub enum Region {
    US1,
    US3,
    US5,
    EU,
    AP1,
}

impl Region {
    pub(crate) fn trace_endpoint(&self) -> String {
        let base = match self {
            Region::US1 => "datadoghq.com",
            Region::US3 => "us3.datadoghq.com",
            Region::US5 => "us5.datadoghq.com",
            Region::EU => "datadoghq.eu",
            Region::AP1 => "ap1.datadoghq.com",
        };
        format!("https://trace.agent.{}", base)
    }
}

pub struct DatadogExporterConfigBuilder {
    region: Region,
    custom_endpoint: Option<String>,
    api_token: String,
    environment: String,
    hostname: String,
    retry_config: RetryConfig,
}

pub struct DatadogExporterBuilder {
    region: Region,
    custom_endpoint: Option<String>,
    api_token: String,
    environment: String,
    hostname: String,
    retry_config: RetryConfig,
}

impl DatadogExporterConfigBuilder {
    pub fn new(
        region: Region,
        custom_endpoint: Option<String>,
        api_key: String,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            region,
            custom_endpoint,
            api_token: api_key,
            environment: "dev".to_string(),
            hostname: "hostname".to_string(),
            retry_config,
        }
    }

    pub fn with_environment(mut self, environment: String) -> Self {
        self.environment = environment;
        self
    }

    pub fn with_hostname(mut self, hostname: String) -> Self {
        self.hostname = hostname;
        self
    }

    pub fn set_indefinite_retry(&mut self) {
        self.retry_config.indefinite_retry = true;
    }

    pub fn build(self) -> DatadogExporterBuilder {
        DatadogExporterBuilder {
            region: self.region,
            custom_endpoint: self.custom_endpoint,
            api_token: self.api_token,
            environment: self.environment,
            hostname: self.hostname,
            retry_config: self.retry_config,
        }
    }
}

impl DatadogExporterBuilder {
    pub fn build<'a>(
        self,
        rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceSpans>, BoxError> {
        let client = Client::build(tls::Config::default(), Protocol::Http, Default::default())?;

        let transformer = Transformer::new(self.environment, self.hostname);

        let req_builder = RequestBuilder::new(
            transformer,
            self.region,
            self.custom_endpoint,
            self.api_token,
        )?;

        let retry_layer = RetryPolicy::new(self.retry_config, None);
        let retry_broadcast = retry_layer.retry_broadcast();

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .timeout(Duration::from_secs(5))
            .service(client);

        let enc_stream =
            RequestIterator::new(RequestBuilderMapper::new(rx.into_stream(), req_builder));

        let exp = Exporter::new(
            "datadog",
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
pub struct DatadogTraceDecoder;

impl ResponseDecode<String> for DatadogTraceDecoder {
    // todo: look at response
    fn decode(&self, _: Bytes, _: ContentEncoding) -> Result<String, BoxError> {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::exporters::crypto_init_tests::init_crypto;
    use crate::exporters::datadog::{DatadogExporterConfigBuilder, ExporterType, Region};
    use crate::exporters::http::retry::RetryConfig;
    use crate::topology::payload::Message;
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use std::time::Duration;
    use tokio::join;
    use tokio_test::{assert_err, assert_ok};
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn test_message_acknowledgment_flow() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        // Set up mock endpoint that returns success
        let _success_mock = server.mock(|when, then| {
            when.method(POST).path("/api/v0.2/traces");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body("ok");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let exporter = new_exporter(addr, brx);

        // Create acknowledgment channel
        let (ack_tx, mut ack_rx) = bounded::<crate::topology::payload::KafkaAcknowledgement>(10);

        // Create fake traces with Kafka metadata for acknowledgment
        let traces = FakeOTLP::trace_service_request();
        let kafka_metadata = crate::topology::payload::KafkaMetadata {
            offset: 12345,
            partition: 2,
            topic_id: 1,
            ack_chan: Some(ack_tx),
        };

        let message = Message {
            metadata: Some(crate::topology::payload::MessageMetadata::kafka(
                kafka_metadata.clone(),
            )),
            payload: traces.resource_spans,
        };

        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();

        // Start the exporter
        let exporter_handle =
            tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        // Send the message with metadata
        btx.send(vec![message]).await.unwrap();
        drop(btx); // Close the sender to allow exporter to finish

        // Wait for the acknowledgment
        let ack_result = tokio::time::timeout(Duration::from_secs(5), ack_rx.next()).await;

        assert_ok!(&ack_result, "Should receive acknowledgment within timeout");

        let ack = ack_result.unwrap().unwrap();
        match ack {
            crate::topology::payload::KafkaAcknowledgement::Ack(kafka_ack) => {
                assert_eq!(kafka_ack.offset, 12345);
                assert_eq!(kafka_ack.partition, 2);
                assert_eq!(kafka_ack.topic_id, 1);
            }
            crate::topology::payload::KafkaAcknowledgement::Nack(_) => {
                panic!("Expected Ack but got Nack");
            }
        }

        // Clean up - cancel the exporter
        cancellation_token.cancel();
        let exporter_result = exporter_handle.await;
        assert_ok!(exporter_result);
    }

    #[tokio::test]
    async fn success_and_retry() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/api/v0.2/traces");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body("ohi");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let exporter = new_exporter(addr, brx);

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
            when.method(POST).path("/api/v0.2/traces");
            then.status(429)
                .header("content-type", "application/x-protobuf")
                .body("hold up");
        });

        let (btx, brx) = bounded::<Vec<Message<ResourceSpans>>>(100);
        let exporter = new_exporter(addr, brx);

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

    fn new_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
    ) -> ExporterType<'a, ResourceSpans> {
        let retry = RetryConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(50),
            max_elapsed_time: Duration::from_millis(50),
            indefinite_retry: false,
        };

        DatadogExporterConfigBuilder::new(Region::US1, Some(addr), "1234".to_string(), retry)
            .build()
            .build(brx, None)
            .unwrap()
    }
}
