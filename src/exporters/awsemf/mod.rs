// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::awsemf::request_builder::RequestBuilder;
use crate::exporters::awsemf::transformer::Transformer;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use crate::aws_api::config::AwsConfig;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::exporter::Exporter;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::request_iter::RequestIterator;
use crate::exporters::http::tls;
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::FlushReceiver;
use bytes::Bytes;
use flume::r#async::RecvStream;
use http::Request;
use http_body_util::Full;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use serde::Deserialize;
use std::time::Duration;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, ServiceBuilder};

use super::http::finalizer::SuccessStatusFinalizer;

mod emf_request;
mod request_builder;
mod transformer;

type SvcType = TowerRetry<RetryPolicy<()>, Timeout<HttpClient<Full<Bytes>, (), AwsEmfDecoder>>>;

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
    SuccessStatusFinalizer,
>;

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(from = "String")]
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

#[derive(Clone)]
pub struct AwsEmfExporterConfig {
    pub region: Region,
    pub log_group_name: String,
    pub log_stream_name: Option<String>,
    pub log_retention: i32,
    pub namespace: Option<String>,
    pub custom_endpoint: Option<String>,
    pub retain_initial_value_of_delta_metric: bool,
    pub retry_config: RetryConfig,
}

impl Default for AwsEmfExporterConfig {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            log_group_name: "/rotel/metrics".to_string(),
            log_stream_name: None,
            log_retention: 0,
            namespace: None,
            custom_endpoint: None,
            retain_initial_value_of_delta_metric: false,
            retry_config: Default::default(),
        }
    }
}

pub struct AwsEmfExporterConfigBuilder {
    config: AwsEmfExporterConfig,
}

impl Default for AwsEmfExporterConfigBuilder {
    fn default() -> Self {
        Self {
            config: Default::default(),
        }
    }
}

impl AwsEmfExporterConfigBuilder {
    pub fn new() -> Self {
        Default::default()
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
        self.config.log_stream_name = Some(log_stream_name.into());
        self
    }

    pub fn with_log_retention(mut self, log_retention: i32) -> Self {
        self.config.log_retention = log_retention;
        self
    }

    pub fn with_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.config.namespace = Some(namespace.into());
        self
    }

    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        //        self.config.tags = tags;
        todo!()
    }

    pub fn with_custom_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.config.custom_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_retain_initial_value_of_delta_metric(mut self, retain: bool) -> Self {
        self.config.retain_initial_value_of_delta_metric = retain;
        self
    }

    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.config.retry_config = retry_config;
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
    pub fn build<'a>(
        self,
        rx: BoundedReceiver<Vec<ResourceMetrics>>,
        flush_receiver: Option<FlushReceiver>,
        aws_config: AwsConfig,
    ) -> Result<ExporterType<'a, ResourceMetrics>, BoxError> {
        let client = HttpClient::build(tls::Config::default(), Default::default())?;
        let transformer = Transformer::new(self.config.clone());

        let req_builder = RequestBuilder::new(transformer, aws_config, self.config.clone())?;

        let retry_layer = RetryPolicy::new(self.config.retry_config, None);
        let retry_broadcast = retry_layer.retry_broadcast();

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .timeout(Duration::from_secs(5))
            .service(client);

        let enc_stream =
            RequestIterator::new(RequestBuilderMapper::new(rx.into_stream(), req_builder));

        let exp = Exporter::new(
            "aws-emf",
            "metrics",
            enc_stream,
            svc,
            SuccessStatusFinalizer::default(),
            flush_receiver,
            retry_broadcast,
            Duration::from_secs(1),
            Duration::from_secs(2),
        );

        Ok(exp)
    }
}

#[derive(Default, Clone)]
pub struct AwsEmfDecoder;

impl ResponseDecode<()> for AwsEmfDecoder {
    fn decode(&self, _: Bytes, _: ContentEncoding) -> Result<(), BoxError> {
        Ok(())
    }
}
