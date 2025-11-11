// SPDX-License-Identifier: Apache-2.0

// HTTP client implementation that supports both gRPC and HTTP protocols.
//
// This module provides a single `Client` that can handle both gRPC and HTTP requests
// based on the `Protocol` enum.

use crate::exporters::http::metadata_extractor::MetadataExtractor;
use crate::exporters::http::response::Response;
use crate::exporters::http::tls::Config;
use crate::exporters::http::types::ContentEncoding;
use crate::topology::payload::MessageMetadata;
use bytes::Bytes;
use http::Request;
use http::header::CONTENT_ENCODING;
use http_body_util::BodyExt;
use hyper::body::{Body, Incoming};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::Status;
use tower::{BoxError, Service};
use tracing::warn;

#[derive(Clone, Debug, PartialEq)]
pub enum Protocol {
    Grpc,
    Http,
}

#[derive(Debug)]
pub struct ConnectError;
impl Display for ConnectError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "unable to connect")
    }
}
impl Error for ConnectError {}

#[derive(Debug)]
pub struct TransportErrorWithMetadata {
    pub original_error: BoxError,
    pub metadata: Option<Vec<MessageMetadata>>,
}

impl Display for TransportErrorWithMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transport error: {}", self.original_error)
    }
}

impl Error for TransportErrorWithMetadata {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.original_error.as_ref())
    }
}

#[derive(Clone)]
pub struct Client<ReqBody, Resp, Dec> {
    inner: HyperClient<HttpsConnector<HttpConnector>, ReqBody>,
    decoder: Dec,
    protocol: Protocol,
    _phantom: PhantomData<Resp>,
}

pub trait ResponseDecode<T> {
    fn decode(&self, body: Bytes, encoding: ContentEncoding) -> Result<T, BoxError>;
}

pub(crate) fn build_hyper_client<ReqBody>(
    tls_config: Config,
    http2_only: bool,
) -> Result<HyperClient<HttpsConnector<HttpConnector>, ReqBody>, BoxError>
where
    ReqBody: Body + Send,
    <ReqBody as Body>::Data: Send,
{
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
        .build::<_, ReqBody>(https);

    Ok(client)
}

impl<ReqBody, Resp, Dec> Client<ReqBody, Resp, Dec>
where
    ReqBody: Body + Send,
    <ReqBody as Body>::Data: Send,
{
    pub fn build(tls_config: Config, protocol: Protocol, decoder: Dec) -> Result<Self, BoxError> {
        let inner = build_hyper_client(tls_config, protocol == Protocol::Grpc)?;

        Ok(Self {
            inner,
            decoder,
            protocol,
            _phantom: Default::default(),
        })
    }
}

impl<ReqBody, Resp, Dec> Client<ReqBody, Resp, Dec>
where
    ReqBody: Body + Send + 'static + Unpin,
    <ReqBody as Body>::Data: Send,
    <ReqBody as Body>::Error: Into<BoxError>,
    Dec: ResponseDecode<Resp> + Clone,
{
    async fn perform_request(&self, req: Request<ReqBody>) -> Result<Response<Resp>, BoxError> {
        match self.inner.request(req).await {
            Err(e) => {
                if e.is_connect() {
                    Err(ConnectError {}.into())
                } else {
                    Err(e.into())
                }
            }
            Ok(resp) => {
                let (head, mut body) = resp.into_parts();

                let encoding = match head.headers.get(CONTENT_ENCODING) {
                    None => ContentEncoding::None,
                    Some(v) => match TryFrom::try_from(v) {
                        Ok(ce) => ce,
                        Err(e) => return Err(e),
                    },
                };

                // gRPC has stricter encoding validation
                if self.protocol == Protocol::Grpc
                    && encoding != ContentEncoding::None
                    && encoding != ContentEncoding::Gzip
                {
                    return Err(format!("unsupported content encoding: {:?}", encoding).into());
                }

                // Handle non-success status codes
                if !(200..=202).contains(&head.status.as_u16()) {
                    return match self.protocol {
                        Protocol::Grpc => Ok(Response::from_http(head, None)),
                        Protocol::Http => {
                            // For HTTP, try to decode the error response body
                            let body = match self
                                .decoder
                                .decode(response_bytes(body).await?, encoding.clone())
                            {
                                Ok(r) => Some(r),
                                Err(_e) => None, // todo: handle string types better
                            };
                            Ok(Response::from_http(head, body))
                        }
                    };
                }

                // For gRPC, check status from headers early
                if self.protocol == Protocol::Grpc {
                    let header_status = Status::from_header_map(&head.headers);
                    if let Some(status) = header_status.clone() {
                        if status.code() != tonic::Code::Ok {
                            return Ok(Response::from_grpc(status, None));
                        }
                    }
                }

                let mut resp = Response::from_http(head, None);
                while let Some(next) = body.frame().await {
                    match next {
                        Ok(frame) => {
                            if frame.is_data() {
                                let data = frame.into_data().unwrap();

                                match self.decoder.decode(data, encoding.clone()) {
                                    Ok(r) => resp = resp.with_body(r),
                                    Err(e) => return Err(e),
                                }
                            } else if frame.is_trailers() {
                                let trailers = frame.into_trailers().unwrap();

                                match self.protocol {
                                    Protocol::Grpc => {
                                        // gRPC expects trailers with status information
                                        match Status::from_header_map(&trailers) {
                                            None => {
                                                return Err(
                                                    "unable to parse trailer headers".into()
                                                );
                                            }
                                            Some(status) => {
                                                if status.code() != tonic::Code::Ok {
                                                    return Ok(Response::from_grpc(status, None));
                                                }
                                            }
                                        }
                                    }
                                    Protocol::Http => {
                                        // HTTP doesn't typically use trailers, so warn about them
                                        warn!(
                                            "Received unexpected trailers on HTTP client: {:?}",
                                            trailers
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => return Err(format!("failed reading response: {}", e).into()),
                    }
                }

                Ok(resp)
            }
        }
    }
}

// Service implementation for Request<ReqBody> where ReqBody implements MetadataExtractor
impl<ReqBody, Resp, Dec> Service<Request<ReqBody>> for Client<ReqBody, Resp, Dec>
where
    ReqBody: Body + Clone + Send + 'static + Unpin + MetadataExtractor,
    <ReqBody as Body>::Data: Send,
    <ReqBody as Body>::Error: Into<BoxError>,
    Dec: ResponseDecode<Resp> + Send + Clone + Sync + 'static,
    Resp: Send + Clone + Sync + 'static,
{
    type Response = Response<Resp>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let this = self.clone();

        Box::pin(async move {
            // Extract metadata by destructuring the request
            let (parts, mut body) = req.into_parts();
            let metadata = body.take_metadata();

            // Reconstruct the request with the (possibly modified) body
            let req = Request::from_parts(parts, body);

            match this.perform_request(req).await {
                Ok(mut resp) => {
                    // Add metadata to the response
                    resp = resp.with_metadata(metadata);
                    Ok(resp)
                }
                Err(e) => {
                    // For transport errors, wrap the error with metadata
                    let wrapped_error: BoxError = Box::new(TransportErrorWithMetadata {
                        original_error: e,
                        metadata,
                    });
                    Err(wrapped_error)
                }
            }
        })
    }
}

pub async fn response_bytes(body: Incoming) -> Result<Bytes, BoxError> {
    body.collect()
        .await
        .map(|col| col.to_bytes())
        .map_err(|e| format!("failed to read response body: {}", e).into())
}
