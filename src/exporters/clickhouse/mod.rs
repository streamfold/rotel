mod api_request;
mod ch_error;
mod compression;
mod exception;
mod payload;
mod request_builder;
mod request_builder_mapper;
mod rowbinary;
mod schema;
mod transformer;

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::clickhouse::api_request::ApiRequestBuilder;
use crate::exporters::clickhouse::exception::extract_exception;
use crate::exporters::clickhouse::payload::ClickhousePayload;
use crate::exporters::clickhouse::request_builder::RequestBuilder;
use crate::exporters::clickhouse::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::clickhouse::schema::get_span_row_col_keys;
use crate::exporters::clickhouse::transformer::Transformer;
use crate::exporters::http;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::response::Response;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::{FlushReceiver, conditional_flush};
use bytes::Bytes;
use flume::r#async::RecvStream;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::error::Error;
use std::ops::Add;
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;
use tower::retry::Retry as TowerRetry;
use tower::timeout::Timeout;
use tower::{BoxError, Service, ServiceBuilder};
use tracing::{debug, error, info, warn};

type ExportFuture = Pin<Box<dyn Future<Output = Result<Response<()>, BoxError>> + Send>>;

// Buffer sizes from Clickhouse driver
pub(crate) const BUFFER_SIZE: usize = 256 * 1024;
// Threshold to send a chunk. Should be slightly less than `BUFFER_SIZE`
// to avoid extra reallocations in case of a big last row.
pub(crate) const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 2048;

pub(crate) const MAX_CONCURRENT_REQUESTS: usize = 10;

#[derive(Default, Clone, PartialEq)]
pub enum Compression {
    None,
    #[default]
    Lz4,
}

pub struct ClickhouseExporter {
    rx: BoundedReceiver<Vec<ResourceSpans>>,
    svc: TowerRetry<
        RetryPolicy<()>,
        Timeout<HttpClient<ClickhousePayload, (), ClickhouseRespDecoder>>,
    >,
    req_builder: RequestBuilder<ResourceSpans, Transformer>,
    flush_receiver: Option<FlushReceiver>,
    encode_drain_max_time: Duration,
    export_drain_max_time: Duration,
}

#[derive(Default)]
pub struct ClickhouseExporterBuilder {
    retry_config: RetryConfig,
    flush_receiver: Option<FlushReceiver>,
    compression: Compression,
    endpoint: String,
    database: String,
    table_prefix: String,
    auth_user: Option<String>,
    auth_password: Option<String>,
    async_insert: bool,
}

impl ClickhouseExporterBuilder {
    pub fn with_flush_receiver(mut self, flush_receiver: Option<FlushReceiver>) -> Self {
        self.flush_receiver = flush_receiver;
        self
    }

    pub fn with_compression(mut self, compression: impl Into<Compression>) -> Self {
        self.compression = compression.into();
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

    pub fn build(
        self,
        rx: BoundedReceiver<Vec<ResourceSpans>>,
    ) -> Result<ClickhouseExporter, BoxError> {
        let client = HttpClient::build(http::tls::Config::default(), Default::default())?;

        let transformer = Transformer::new(self.compression.clone());

        let traces_sql = get_traces_sql(self.table_prefix);
        let api_req_builder = ApiRequestBuilder::new(
            self.endpoint,
            self.database,
            traces_sql,
            self.compression.clone(),
            self.auth_user,
            self.auth_password,
            self.async_insert,
        )?;

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

impl ClickhouseExporter {
    pub fn builder(
        endpoint: String,
        database: String,
        table_prefix: String,
    ) -> ClickhouseExporterBuilder {
        ClickhouseExporterBuilder {
            endpoint,
            database,
            table_prefix,
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

                            match rs.status_code().as_u16() {
                                200..=202 => {},
                                404 => error!("Received 404 when exporting to Clickhouse, does the table exist?"),
                                _ => error!("Failed to export to Clickhouse: {:?}", rs),
                            };
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

    async fn drain_futures(
        &mut self,
        enc_stream: &mut RequestBuilderMapper<
            RecvStream<'_, Vec<ResourceSpans>>,
            ResourceSpans,
            RequestBuilder<ResourceSpans, Transformer>,
        >,
        export_futures: &mut FuturesUnordered<ExportFuture>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let finish_encoding = Instant::now().add(self.encode_drain_max_time);
        let finish_sending = Instant::now().add(self.export_drain_max_time);
        let type_name = "clickhouse_exporter";

        // First we must wait on currently encoding futures
        loop {
            let poll_res = timeout_at(finish_encoding, enc_stream.next()).await;
            match poll_res {
                Err(_) => {
                    return Err(
                        "ClickHouseExporter, timed out waiting for requests to encode".into(),
                    );
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
                    return Err(
                        "ClickHouseExporter, timed out waiting for requests to finish".into(),
                    );
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
                                "ClickHouseExporter error from endpoint."
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

fn get_traces_sql(table_prefix: String) -> String {
    format!(
        "INSERT INTO {} ({}) FORMAT RowBinary",
        get_table_name(table_prefix, "traces"),
        get_span_row_col_keys()
    )
}

fn get_table_name(table_prefix: String, table: &str) -> String {
    format!("{}_{}", table_prefix, table)
}

#[derive(Default, Clone)]
pub struct ClickhouseRespDecoder;

impl ResponseDecode<()> for ClickhouseRespDecoder {
    fn decode(&self, resp: Bytes, _: ContentEncoding) -> Result<(), BoxError> {
        match extract_exception(resp.as_ref()) {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate utilities;

    use super::*;
    use crate::bounded_channel::{BoundedReceiver, bounded};
    use crate::exporters::crypto_init_tests::init_crypto;
    use httpmock::prelude::*;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use tokio::join;
    use tokio_test::{assert_err, assert_ok};
    use tokio_util::sync::CancellationToken;
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn success() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200).body("ohi");
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
    }

    #[tokio::test]
    async fn db_exception() {
        init_crypto();
        let server = MockServer::start();
        let addr = format!("http://127.0.0.1:{}", server.port());

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/");
            then.status(200)
                // Must keep newline for matching
                .body("Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(equals(number, 2) :: 1) -> throwIf(equals(number, 2))
");
        });

        let (btx, brx) = bounded::<Vec<ResourceSpans>>(100);
        let exporter = new_exporter(addr, brx);

        let cancellation_token = CancellationToken::new();

        let cancel_clone = cancellation_token.clone();
        let jh = tokio::spawn(async move { exporter.start(cancel_clone).await });

        let traces = FakeOTLP::trace_service_request();
        btx.send(traces.resource_spans).await.unwrap();
        drop(btx);
        let res = join!(jh).0.unwrap();
        assert_err!(res);

        hello_mock.assert();
    }

    fn new_exporter(addr: String, brx: BoundedReceiver<Vec<ResourceSpans>>) -> ClickhouseExporter {
        ClickhouseExporter::builder(addr, "otel".to_string(), "otel".to_string())
            .build(brx)
            .unwrap()
    }
}
