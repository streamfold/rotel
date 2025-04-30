mod api_request;
mod ch_error;
mod compression;
mod exception;
mod exporter;
mod payload;
mod request_builder;
mod request_builder_mapper;
mod rowbinary;
mod schema;
mod transformer;

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::clickhouse::api_request::ApiRequestBuilder;
use crate::exporters::clickhouse::exception::extract_exception;
use crate::exporters::clickhouse::exporter::Exporter;
use crate::exporters::clickhouse::request_builder::RequestBuilder;
use crate::exporters::clickhouse::request_builder_mapper::RequestBuilderMapper;
use crate::exporters::clickhouse::schema::get_span_row_col_keys;
use crate::exporters::clickhouse::transformer::Transformer;
use crate::exporters::http;
use crate::exporters::http::client::ResponseDecode;
use crate::exporters::http::http_client::HttpClient;
use crate::exporters::http::retry::{RetryConfig, RetryPolicy};
use crate::exporters::http::types::ContentEncoding;
use crate::topology::flush_control::FlushReceiver;
use bytes::Bytes;
use flume::r#async::RecvStream;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::time::Duration;
use tower::{BoxError, ServiceBuilder};

// Buffer sizes from Clickhouse driver
pub(crate) const BUFFER_SIZE: usize = 256 * 1024;
// Threshold to send a chunk. Should be slightly less than `BUFFER_SIZE`
// to avoid extra reallocations in case of a big last row.
pub(crate) const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 2048;

#[derive(Default, Clone, PartialEq)]
pub enum Compression {
    None,
    #[default]
    Lz4,
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
    pub fn new(
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

    pub fn build_traces_exporter<'a>(
        &self,
        rx: BoundedReceiver<Vec<ResourceSpans>>,
        flush_receiver: Option<FlushReceiver>,
    ) -> Result<
        Exporter<
            RequestBuilderMapper<
                RecvStream<'a, Vec<ResourceSpans>>,
                ResourceSpans,
                RequestBuilder<ResourceSpans, Transformer>,
            >,
        >,
        BoxError,
    > {
        let client = HttpClient::build(http::tls::Config::default(), Default::default())?;

        let transformer = Transformer::new(self.compression.clone());

        let traces_sql = get_traces_sql(self.table_prefix.clone());
        let api_req_builder = ApiRequestBuilder::new(
            self.endpoint.clone(),
            self.database.clone(),
            traces_sql,
            self.compression.clone(),
            self.auth_user.clone(),
            self.auth_password.clone(),
            self.async_insert,
        )?;

        let req_builder = RequestBuilder::new(transformer, api_req_builder)?;

        let retry_layer = RetryPolicy::new(self.retry_config.clone(), None);

        let svc = ServiceBuilder::new()
            .retry(retry_layer)
            .timeout(Duration::from_secs(5))
            .service(client);

        let enc_stream = RequestBuilderMapper::new(rx.into_stream(), req_builder);

        let inner = Exporter::new(
            enc_stream,
            svc,
            flush_receiver,
            Duration::from_secs(1),
            Duration::from_secs(2),
        );
        //
        // let exp = ClickhouseExporter{
        //     //rx,
        //     inner,
        // };

        Ok(inner)
    }
}
//
// impl ClickhouseExporter {
//     pub fn builder(
//         endpoint: String,
//         database: String,
//         table_prefix: String,
//     ) -> ClickhouseExporterBuilder {
//         ClickhouseExporterBuilder {
//             endpoint,
//             database,
//             table_prefix,
//             ..Default::default()
//         }
//     }
// }

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
