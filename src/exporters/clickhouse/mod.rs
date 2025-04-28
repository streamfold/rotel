mod api_request;
mod payload;
mod request_builder;
mod request_builder_mapper;
mod rowbinary;
mod transformer;
mod schema;

use std::error::Error;
use std::ops::Add;
use crate::bounded_channel::BoundedReceiver;
use crate::exporters::clickhouse::request_builder::RequestBuilder;
use crate::exporters::clickhouse::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::clickhouse::transformer::Transformer;
use crate::exporters::http;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::response::Response;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::{FlushReceiver, conditional_flush};
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::pin::Pin;
use std::time::Duration;
use flume::r#async::RecvStream;
use http_body_util::Full;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use tokio::select;
use tokio::time::{timeout_at, Instant};
use tokio_util::sync::CancellationToken;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{Service, BoxError, ServiceBuilder};
use tracing::{debug, error, info, warn};
use crate::exporters::clickhouse::api_request::ApiRequestBuilder;
use crate::exporters::clickhouse::schema::{get_span_row_col_keys};

type ExportFuture = Pin<Box<dyn Future<Output = Result<Response<()>, BoxError>> + Send>>;

const MAX_CONCURRENT_REQUESTS: usize = 10;

pub struct ClickhouseExporter {
    rx: BoundedReceiver<Vec<ResourceSpans>>,
    svc: TowerRetry<RetryPolicy<()>, Timeout<HttpClient<Full<Bytes>, (), ClickhouseRespDecoder>>>,
    req_builder: RequestBuilder<ResourceSpans, Transformer>,
    flush_receiver: Option<FlushReceiver>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
}

#[derive(Default)]
pub struct ClickhouseExporterBuilder {
    retry_config: RetryConfig,
    flush_receiver: Option<FlushReceiver>,
    endpoint: String,
}

impl ClickhouseExporterBuilder {
    pub fn with_flush_receiver(mut self, flush_receiver: Option<FlushReceiver>) -> Self {
        self.flush_receiver = flush_receiver;
        self
    }

    pub fn build(
        self,
        rx: BoundedReceiver<Vec<ResourceSpans>>,
    ) -> Result<ClickhouseExporter, BoxError> {
        let client = HttpClient::build(http::tls::Config::default(), Default::default())?;

        let transformer = Transformer::new();

        let logs_sql = get_logs_sql();
        let api_req_builder = ApiRequestBuilder::new(self.endpoint, logs_sql);

        let req_builder = RequestBuilder::new(transformer, api_req_builder)?;

        let retry_layer = RetryPolicy::new(self.retry_config, None);

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .timeout(Duration::from_secs(5))
            .service(client);

        Ok(ClickhouseExporter {
            req_builder,
            rx,
            svc,
            flush_receiver: self.flush_receiver,
            encode_drain_max_time: Duration::from_secs(1),
            export_drain_max_time: Duration::from_secs(2),
        })
    }
}

fn get_logs_sql() -> String {
    format!("INSERT INTO otel_traces ({}) FORMAT RowBinary", get_span_row_col_keys())
}

impl ClickhouseExporter {
    pub fn builder(endpoint: String) -> ClickhouseExporterBuilder {
        ClickhouseExporterBuilder {
            endpoint,
            ..Default::default()
        }
    }

    pub async fn start(mut self, token: CancellationToken) -> Result<(), BoxError> {
        let rx = self.rx.clone();
        let mut enc_stream = RequestBuilderMapper::new(rx.stream(), self.req_builder.clone());
        let mut export_futures: FuturesUnordered<ExportFuture> = FuturesUnordered::new();
        let mut flush_receiver = self.flush_receiver.take();
        loop {
            select! {
                biased;

                Some(resp) = export_futures.next() => {
                  match resp {
                        Err(e) => {
                            error!(error = ?e, "Exporting failed, dropping data.")
                        },
                        Ok(rs) => {
                            debug!(rs = ?rs, futures_size = export_futures.len(), "Clickhouse exporter sent response");
                        }
                    }
                },

                input = enc_stream.next(), if export_futures.len() < MAX_CONCURRENT_REQUESTS => {
                    match input {
                        None => {
                            debug!("Clickhouse exporter received end of input, exiting.");
                            break
                        },
                        Some(req) => match req {
                            Ok(req) => export_futures.push(Box::pin(self.svc.call(req))),
                            Err(e) => {
                                error!(error = ?e, "Failed to encode Clickhouse request, dropping.");
                            }
                        }
                    }
                },

                Some(resp) = conditional_flush(&mut flush_receiver) => {
                    match resp {
                        (Some(req), listener) => {
                            debug!("received force flush in Clickhouse exporter: {:?}", req);

                            if let Err(res) = self.drain_futures(&mut enc_stream, &mut export_futures).await {
                                warn!("unable to drain exporter: {}", res);
                            }

                            if let Err(e) = listener.ack(req).await {
                                warn!("unable to ack flush request: {}", e);
                            }
                        },
                        (None, _) => warn!("flush channel was closed")
                    }
                },

                _ = token.cancelled() => {
                    info!("Clickhouse exporter received shutdown signal, exiting main processing loop");
                    break;
                },
            }
        }

        self.drain_futures(&mut enc_stream, &mut export_futures)
            .await
    }

    async fn drain_futures(&mut self,
                           enc_stream: &mut RequestBuilderMapper<
                               RecvStream<'_, Vec<ResourceSpans>>,
                               ResourceSpans,
                               RequestBuilder<ResourceSpans, Transformer>,
                           >,
                           export_futures: &mut FuturesUnordered<ExportFuture>,
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

#[derive(Default, Clone)]
pub struct ClickhouseRespDecoder;

impl ResponseDecode<()> for ClickhouseRespDecoder {
    // todo: look at response
    fn decode(&self, _: Bytes, _: ContentEncoding) -> Result<(), BoxError> {
        Ok(())
    }
}
