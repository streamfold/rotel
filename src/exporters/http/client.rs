// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::tls::Config;
use crate::exporters::http::types::ContentEncoding;
use bytes::Bytes;
use http_body_util::Full;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tower::BoxError;

#[derive(Debug)]
pub struct ConnectError;
impl Display for ConnectError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "unable to connect")
    }
}
impl Error for ConnectError {}

pub trait ResponseDecode<T> {
    fn decode(&self, body: Bytes, encoding: ContentEncoding) -> Result<T, BoxError>;
}

pub(crate) fn build_hyper_client(
    tls_config: Config,
    http2_only: bool,
) -> Result<HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>, BoxError> {
    let client_config = tls_config.into_client_config()?;

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(client_config)
        .https_or_http()
        .enable_http2()
        .build();

    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        // todo: make configurable
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(100)
        .http2_only(http2_only)
        .timer(TokioTimer::new())
        .build::<_, Full<Bytes>>(https);

    Ok(client)
}
