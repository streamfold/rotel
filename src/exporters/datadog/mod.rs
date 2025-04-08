// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::datadog::request_builder::RequestBuilder;
use crate::exporters::datadog::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::datadog::transform::Transformer;
use crate::exporters::http;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};

use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::response::Response;
use crate::exporters::http::types::ContentEncoding;
use bytes::Bytes;
use flume::r#async::RecvStream;
use futures::stream::FuturesUnordered;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::error::Error;
use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::time::{Instant, timeout_at};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, Service, ServiceBuilder};
use tracing::{Level, debug, error, event};

mod api_request;
mod request_builder;
mod request_builder_mapper;
mod transform;
mod types;

const MAX_CONCURRENT_REQUESTS: usize = 10;

type ExportFuture =
    Pin<Box<dyn Future<Output = Result<Response<()>, Box<dyn Error + Send + Sync>>> + Send>>;

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

pub struct DatadogTraceExporter {
    rx: BoundedReceiver<Vec<ResourceSpans>>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
    req_builder: RequestBuilder<ResourceSpans, Transformer>,
    svc: TowerRetry<RetryPolicy<()>, Timeout<HttpClient<(), DatadogTraceDecoder>>>,
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

    pub fn build(
        self,
        rx: BoundedReceiver<Vec<ResourceSpans>>,
    ) -> Result<DatadogTraceExporter, BoxError> {
        let client = HttpClient::build(http::tls::Config::default(), Default::default())?;

        // Main processing loop

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

        Ok(DatadogTraceExporter {
            rx,
            req_builder,
            svc,
            encode_drain_max_time: Duration::from_secs(1),
            export_drain_max_time: Duration::from_secs(2),
        })
    }
}

impl DatadogTraceExporter {
    pub fn builder(
        region: Region,
        custom_endpoint: Option<String>,
        api_key: String,
    ) -> DatadogTraceExporterBuilder {
        DatadogTraceExporterBuilder {
            region,
            custom_endpoint,
            api_token: api_key,
            ..Default::default()
        }
    }

    /// Starts the main processing loop.
    pub async fn start(
        &mut self,
        token: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let rx = self.rx.clone();
        let mut enc_stream = RequestBuilderMapper::new(rx.stream(), self.req_builder.clone());
        let mut export_futures: FuturesUnordered<ExportFuture> = FuturesUnordered::new();

        loop {
            select! {
                biased;

                _ = token.cancelled() => {
                    event!(Level::INFO, "DatadogExporter received shutdown signal, exiting main processing loop");
                    break;
                },

                Some(resp) = export_futures.next() => {
                  match resp {
                        Err(e) => {
                            error!(error = ?e, "Exporting failed, dropping data.")
                        },
                        Ok(rs) => {
                            debug!(rs = ?rs, futures_size = export_futures.len(), "Datadog exporter sent response");
                        }
                    }
                },

                input = enc_stream.next(), if export_futures.len() < MAX_CONCURRENT_REQUESTS => {
                    match input {
                        None => {
                            debug!("Datadog exporter received end of input, exiting.");
                            break
                        },
                        Some(req) => match req {
                            Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
                            Err(e) => {
                                error!(error = ?e, "Failed to encode Datadog request, dropping.");
                            }
                        }
                    }
                },
            }
        }

        self.drain_futures(enc_stream, export_futures).await
    }

    async fn drain_futures(
        &mut self,
        mut enc_stream: RequestBuilderMapper<
            RecvStream<'_, Vec<ResourceSpans>>,
            ResourceSpans,
            RequestBuilder<ResourceSpans, Transformer>,
        >,
        mut export_futures: FuturesUnordered<ExportFuture>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let finish_encoding = Instant::now().add(self.encode_drain_max_time);
        let finish_sending = Instant::now().add(self.export_drain_max_time);
        let type_name = "datadog_exporter";

        // First we must wait on currently encoding futures
        loop {
            let poll_res = timeout_at(finish_encoding, enc_stream.next()).await;
            match poll_res {
                Err(_) => {
                    return Err("DatadogExporter, timed out waiting for requests to encode".into());
                }
                Ok(res) => match res {
                    None => break,
                    Some(r) => match r {
                        Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
                        Err(e) => {
                            error!(error = ?e, "Failed to encode Datadog request, dropping.");
                        }
                    },
                },
            }
        }

        let mut drain_errors = 0;
        loop {
            if export_futures.is_empty() {
                break;
            }

            let poll_res = timeout_at(finish_sending, export_futures.next()).await;
            match poll_res {
                Err(_) => {
                    return Err("DatadogExporter, timed out waiting for requests to finish".into());
                }
                Ok(res) => match res {
                    None => {
                        error!(type_name, "None returned while polling futures");
                        break;
                    }
                    Some(r) => {
                        if let Err(e) = r {
                            error!(type_name,
                                error = ?e,
                                "DatadogExporter error from endpoint."
                            );

                            drain_errors += 1;
                        }
                    }
                },
            }
        }

        if drain_errors > 0 {
            Err(format!(
                "Failed draining export requests, {} requests failed",
                drain_errors
            )
            .into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::exporters::crypto_init_tests::init_crypto;
    use crate::exporters::datadog::{DatadogTraceExporter, Region};
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
        let mut exporter = new_exporter(addr, brx);

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
        let mut exporter = new_exporter(addr, brx);

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
    ) -> DatadogTraceExporter {
        DatadogTraceExporter::builder(Region::US1, Some(addr), "1234".to_string())
            .with_retry_config(RetryConfig {
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                max_elapsed_time: Duration::from_millis(50),
            })
            .build(brx)
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
