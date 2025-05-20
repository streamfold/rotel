// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::http;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use crate::exporters::xray::request_builder::RequestBuilder;
use crate::exporters::xray::transformer::Transformer;
use std::fmt::{Display, Formatter};

use crate::aws_api::config::AwsConfig;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::exporter::{Exporter, ResultLogger};
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::response::Response;
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::FlushReceiver;
use bytes::Bytes;
use flume::r#async::RecvStream;
use http_body_util::Full;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::time::Duration;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, ServiceBuilder};
use tracing::error;

mod request_builder;
mod transformer;
mod xray_request;

type SvcType = TowerRetry<RetryPolicy<()>, Timeout<HttpClient<Full<Bytes>, (), XRayTraceDecoder>>>;

type ExporterType<'a, Resource> = Exporter<
    RequestBuilderMapper<
        RecvStream<'a, Vec<Resource>>,
        Resource,
        Full<Bytes>,
        RequestBuilder<'a, Resource, Transformer>,
    >,
    SvcType,
    Full<Bytes>,
    XRayResultLogger,
>;

#[derive(Copy, Clone, Debug)]
pub enum Region {
    UsEast1,
    UsEast2,
    UsWest1,
    UsWest2,
    AfSouth1,
    ApEast1,
    ApSouth2,
    ApSoutheast3,
    ApSoutheast5,
    ApSoutheast4,
    ApSouth1,
    ApNortheast3,
    ApNortheast2,
    ApSoutheast1,
    ApSoutheast2,
    ApSoutheast7,
    ApNortheast1,
    CaCentral1,
    CaWest1,
    EuCentral1,
    EuWest1,
    EuWest2,
    EuSouth1,
    EuWest3,
    EuSouth2,
    EuNorth1,
    EuCentral2,
    IlCentral1,
    MxCentral1,
    MeSouth1,
    MeCentral1,
    SaEast1,
}

impl Display for Region {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Region::UsEast1 => "us-east-1",
            Region::UsEast2 => "us-east-2",
            Region::UsWest1 => "us-west-1",
            Region::UsWest2 => "us-west-2",
            Region::AfSouth1 => "af-south-1",
            Region::ApEast1 => "ap-east-1",
            Region::ApSouth2 => "ap-south-2",
            Region::ApSoutheast3 => "ap-southeast-3",
            Region::ApSoutheast5 => "ap-southeast-5",
            Region::ApSoutheast4 => "ap-southeast-4",
            Region::ApSouth1 => "ap-south-1",
            Region::ApNortheast3 => "ap-northeast-3",
            Region::ApNortheast2 => "ap-northeast-2",
            Region::ApSoutheast1 => "ap-southeast-1",
            Region::ApSoutheast2 => "ap-southeast-2",
            Region::ApSoutheast7 => "ap-southeast-7",
            Region::ApNortheast1 => "ap-northeast-1",
            Region::CaCentral1 => "ca-central-1",
            Region::CaWest1 => "ca-west-1",
            Region::EuCentral1 => "eu-central-1",
            Region::EuWest1 => "eu-west-1",
            Region::EuWest2 => "eu-west-2",
            Region::EuSouth1 => "eu-south-1",
            Region::EuWest3 => "eu-west-3",
            Region::EuSouth2 => "eu-south-2",
            Region::EuNorth1 => "eu-north-1",
            Region::EuCentral2 => "eu-central-2",
            Region::IlCentral1 => "il-central-1",
            Region::MxCentral1 => "mx-central-1",
            Region::MeSouth1 => "me-south-1",
            Region::MeCentral1 => "me-central-1",
            Region::SaEast1 => "sa-east-1",
        };
        write!(f, "{}", s)
    }
}

impl From<String> for Region {
    fn from(s: String) -> Self {
        match s.as_str() {
            "us-east-1" => Region::UsEast1,
            "us-east-2" => Region::UsEast2,
            "us-west-1" => Region::UsWest1,
            "us-west-2" => Region::UsWest2,
            "af-south-1" => Region::AfSouth1,
            "ap-east-1" => Region::ApEast1,
            "ap-south-2" => Region::ApSouth2,
            "ap-southeast-3" => Region::ApSoutheast3,
            "ap-southeast-5" => Region::ApSoutheast5,
            "ap-southeast-4" => Region::ApSoutheast4,
            "ap-south-1" => Region::ApSouth1,
            "ap-northeast-3" => Region::ApNortheast3,
            "ap-northeast-2" => Region::ApNortheast2,
            "ap-southeast-1" => Region::ApSoutheast1,
            "ap-southeast-2" => Region::ApSoutheast2,
            "ap-southeast-7" => Region::ApSoutheast7,
            "ap-northeast-1" => Region::ApNortheast1,
            "ca-central-1" => Region::CaCentral1,
            "ca-west-1" => Region::CaWest1,
            "eu-central-1" => Region::EuCentral1,
            "eu-west-1" => Region::EuWest1,
            "eu-west-2" => Region::EuWest2,
            "eu-south-1" => Region::EuSouth1,
            "eu-west-3" => Region::EuWest3,
            "eu-south-2" => Region::EuSouth2,
            "eu-north-1" => Region::EuNorth1,
            "eu-central-2" => Region::EuCentral2,
            "il-central-1" => Region::IlCentral1,
            "mx-central-1" => Region::MxCentral1,
            "me-south-1" => Region::MeSouth1,
            "me-central-1" => Region::MeCentral1,
            "sa-east-1" => Region::SaEast1,
            _ => panic!("Unknown region: {}", s),
        }
    }
}

pub struct XRayTraceExporterBuilder {
    region: Region,
    custom_endpoint: Option<String>,
    retry_config: RetryConfig,
}

impl Default for XRayTraceExporterBuilder {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            custom_endpoint: None,
            retry_config: Default::default(),
        }
    }
}

impl XRayTraceExporterBuilder {
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

    pub fn build(
        self,
        rx: BoundedReceiver<Vec<ResourceSpans>>,
        flush_receiver: Option<FlushReceiver>,
        environment: String,
        config: &AwsConfig,
    ) -> Result<ExporterType<ResourceSpans>, BoxError> {
        let client = HttpClient::build(http::tls::Config::default(), Default::default())?;
        let transformer = Transformer::new(environment);

        let req_builder = RequestBuilder::new(
            transformer,
            config,
            self.region,
            self.custom_endpoint.clone(),
        )?;

        let retry_layer = RetryPolicy::new(self.retry_config, None);

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .timeout(Duration::from_secs(5))
            .service(client);

        let enc_stream = RequestBuilderMapper::new(rx.into_stream(), req_builder);

        let exp = Exporter::new(
            "x-ray",
            "traces",
            enc_stream,
            svc,
            XRayResultLogger {
                telemetry_type: "traces".to_string(),
            },
            flush_receiver,
            Duration::from_secs(1),
            Duration::from_secs(2),
        );

        Ok(exp)
    }
}

#[derive(Default, Clone)]
pub struct XRayTraceDecoder;

impl ResponseDecode<()> for XRayTraceDecoder {
    fn decode(&self, _: Bytes, _: ContentEncoding) -> Result<(), BoxError> {
        Ok(())
    }
}

pub struct XRayResultLogger {
    telemetry_type: String,
}

impl ResultLogger<Response<()>> for XRayResultLogger {
    fn handle(&self, resp: Response<()>) {
        match resp.status_code().as_u16() {
            200..=202 => {}
            _ => error!(
                telemetry_type = self.telemetry_type,
                "Failed to export to X-Ray: {:?}", resp
            ),
        };
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use crate::aws_api::config::AwsConfig;
    use crate::bounded_channel::{bounded, BoundedReceiver};
    use crate::exporters::crypto_init_tests::init_crypto;
    use crate::exporters::http::retry::RetryConfig;
    use crate::exporters::xray::{ExporterType, Region, XRayTraceExporterBuilder};
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
            when.method(POST).path("/TraceSegments");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body("ohi");
        });

        let (btx, brx) = bounded::<Vec<ResourceSpans>>(100);
        // Create a true 'static reference using Box::leak
        let config = Box::leak(Box::new(AwsConfig::from_env()));
        let exporter = new_exporter(addr, brx, config);

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
            when.method(POST).path("/TraceSegments");
            then.status(429)
                .header("content-type", "application/x-protobuf")
                .body("hold up");
        });

        let (btx, brx) = bounded::<Vec<ResourceSpans>>(100);
        let config = Box::leak(Box::new(AwsConfig::from_env()));
        let exporter = new_exporter(addr, brx, config);

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

    fn new_exporter(
        addr: String,
        brx: BoundedReceiver<Vec<ResourceSpans>>,
        config: &AwsConfig,
    ) -> ExporterType<ResourceSpans> {
        XRayTraceExporterBuilder::new(Region::UsEast1, Some(addr))
            .with_retry_config(RetryConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                max_elapsed_time: Duration::from_millis(50),
            })
            .build(brx, None, "production".to_string(), config)
            .unwrap()
    }
}
