// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::awsemf::request_builder::RequestBuilder;
use crate::exporters::awsemf::transformer::Transformer;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};

use crate::aws_api::config::AwsConfig;
use crate::exporters::http::exporter::Exporter;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::http::request_iter::RequestIterator;
use crate::exporters::http::tls;
use crate::topology::flush_control::FlushReceiver;
use bytes::Bytes;
use errors::{AwsEmfDecoder, AwsEmfResponse};
use flume::r#async::RecvStream;
use http::Request;
use http_body_util::Full;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use std::time::Duration;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, ServiceBuilder};

use super::http::finalizer::SuccessStatusFinalizer;
use super::shared::aws::Region;

mod emf_request;
mod errors;
mod event;
mod request_builder;
mod transformer;

type SvcType<RespBody> =
    TowerRetry<RetryPolicy<RespBody>, Timeout<HttpClient<Full<Bytes>, RespBody, AwsEmfDecoder>>>;

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
    SvcType<AwsEmfResponse>,
    Full<Bytes>,
    SuccessStatusFinalizer,
>;

#[derive(Clone)]
pub struct AwsEmfExporterConfig {
    pub region: Region,
    pub log_group_name: String,
    pub log_stream_name: String,
    pub namespace: Option<String>,
    pub custom_endpoint: Option<String>,
    pub retain_initial_value_of_delta_metric: bool,
    pub retry_config: RetryConfig,
}

impl Default for AwsEmfExporterConfig {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            log_group_name: "/metrics/default".to_string(),
            log_stream_name: "otel-stream".to_string(),
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
        self.config.log_stream_name = log_stream_name.into();
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
    pub(crate) fn build<'a>(
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
            "awsemf",
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
