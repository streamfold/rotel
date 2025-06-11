// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::datadog::request_builder::RequestBuilder;
use crate::exporters::datadog::transform::Transformer;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};

use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::exporter::{Exporter, ResultLogger};
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::request_iter::RequestIterator;
use crate::exporters::http::response::Response;
use crate::exporters::http::tls;
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::FlushReceiver;
use bytes::Bytes;
use flume::r#async::RecvStream;
use http::Request;
use http_body_util::Full;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::time::Duration;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, ServiceBuilder};
use tracing::error;

mod api_request;
mod request_builder;
mod transform;
mod types;

type SvcType =
    TowerRetry<RetryPolicy<()>, Timeout<HttpClient<Full<Bytes>, (), DatadogTraceDecoder>>>;

type ExporterType<'a, Resource> = Exporter<
    RequestIterator<
        RequestBuilderMapper<
            RecvStream<'a, Vec<Resource>>,
            Resource,
            Full<Bytes>,
            RequestBuilder<Resource, Transformer>,
        >,
        Vec<Request<Full<Bytes>>>,
        Full<Bytes>,
    >,
    SvcType,
    Full<Bytes>,
    DatadogResultLogger,
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

pub struct DatadogTraceExporterBuilder {
    region: Region,
    custom_endpoint: Option<String>,
    api_token: String,
    environment: String,
    hostname: String,
    retry_config: RetryConfig,
}

impl Default for DatadogTraceExporterBuilder {
    fn default() -> Self {
        Self {
            region: Region::US1,
            custom_endpoint: None,
            api_token: "".to_string(),
            environment: "dev".to_string(),
            hostname: "hostname".to_string(),
            retry_config: Default::default(),
        }
    }
}

impl DatadogTraceExporterBuilder {
    pub fn new(region: Region, custom_endpoint: Option<String>, api_key: String) -> Self {
        Self {
            region,
            custom_endpoint,
            api_token: api_key,
            ..Default::default()
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

    #[allow(dead_code)]
    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub fn build<'a>(
        self,
        rx: BoundedReceiver<Vec<ResourceSpans>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<ExporterType<'a, ResourceSpans>, BoxError> {
        let client = HttpClient::build(tls::Config::default(), Default::default())?;

        let transformer = Transformer::new(self.environment.clone(), self.hostname.clone());

        let req_builder = RequestBuilder::new(
            transformer,
            self.region,
            self.custom_endpoint.clone(),
            self.api_token.clone(),
        )?;

        let retry_layer = RetryPolicy::new(self.retry_config, None);

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
            DatadogResultLogger {
                telemetry_type: "traces".to_string(),
            },
            flush_receiver,
            Duration::from_secs(1),
            Duration::from_secs(2),
        );

        Ok(exp)
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::exporters::crypto_init_tests::init_crypto;
    use crate::exporters::datadog::{DatadogTraceExporterBuilder, ExporterType, Region};
    use crate::exporters::http::retry::RetryConfig;
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use std::time::Duration;
    use tokio::join;
    use tokio_test::assert_ok;
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

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

        let (btx, brx) = bounded::<Vec<ResourceSpans>>(100);
        let exporter = new_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let traces = FakeOTLP::trace_service_request();
        btx.send(traces.resource_spans).await.unwrap();
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

        let (btx, brx) = bounded::<Vec<ResourceSpans>>(100);
        let exporter = new_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await.unwrap() });

        let traces = FakeOTLP::trace_service_request();
        btx.send(traces.resource_spans).await.unwrap();
        drop(btx);
        let res = join!(jh);
        assert_ok!(res.0);

        assert!(hello_mock.hits() >= 3); // somewhat timing dependent
    }

    fn new_exporter<'a>(
        addr: String,
        brx: BoundedReceiver<Vec<ResourceSpans>>,
    ) -> ExporterType<'a, ResourceSpans> {
        DatadogTraceExporterBuilder::new(Region::US1, Some(addr), "1234".to_string())
            .with_retry_config(RetryConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                max_elapsed_time: Duration::from_millis(50),
            })
            .build(brx, None)
            .unwrap()
    }
}

#[derive(Default, Clone)]
pub struct DatadogTraceDecoder;

impl ResponseDecode<()> for DatadogTraceDecoder {
    // todo: look at response
    fn decode(&self, _: Bytes, _: ContentEncoding) -> Result<(), BoxError> {
        Ok(())
    }
}

pub struct DatadogResultLogger {
    telemetry_type: String,
}

impl ResultLogger<Response<()>> for DatadogResultLogger {
    fn handle(&self, resp: Response<()>) {
        match resp.status_code().as_u16() {
            200..=202 => {}
            _ => error!(
                telemetry_type = self.telemetry_type,
                "Failed to export to Datadog: {:?}", resp
            ),
        };
    }
}
