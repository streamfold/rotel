// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::tls::Config;
/// A client implementation for OTLP (OpenTelemetry Protocol) exports that supports both gRPC and HTTP protocols.
/// The client handles TLS configuration, request processing, and response decoding.
use crate::exporters::otlp::errors::ExporterError;
use crate::exporters::otlp::{grpc_codec, http_codec, Protocol};
use bytes::Bytes;
use http::header::CONTENT_ENCODING;
use http::{HeaderValue, Request, Response};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::codegen::Service;
use tonic::Status;
use tower_http::BoxError;
use crate::exporters::otlp::request::EncodedRequest;

/// Client struct for handling OTLP exports.
/// Generic over message type T which must implement prost::Message, i.e. ExportTraceServiceRequest.
#[derive(Clone)]
pub struct OTLPClient<T>
where
    T: prost::Message + Default,
{
    /// The underlying Hyper HTTP client with TLS support
    client: HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    /// The protocol (HTTP or gRPC) used for communication
    protocol: Protocol,
    /// PhantomData to handle generic type T
    _phantom: PhantomData<T>,
}

/// Implementation of Tower's Service trait for OTLPClient
impl<T> Service<EncodedRequest> for OTLPClient<T>
where
    T: prost::Message + Default + Clone + Send + 'static,
{
    type Response = T;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// Checks if the service is ready to process requests
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// Processes the request and returns a Future containing the response
    fn call(&mut self, req: EncodedRequest) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            let result = this.perform_request(req.request.clone()).await;
            match result {
                Ok(response) => Ok(response),
                Err(error) => Err(error.into()),
            }
        })
    }
}

impl<T> OTLPClient<T>
where
    T: prost::Message + Default,
{
    /// Creates a new OTLPClient instance with the specified TLS configuration and protocol
    ///
    /// # Arguments
    /// * `tls_config` - TLS configuration for secure communication
    /// * `protocol` - The protocol (HTTP or gRPC) to use for communication
    ///
    /// # Returns
    /// * `Result<Self, Box<dyn Error + Send + Sync>>` - The created client or an error
    pub fn new(
        tls_config: Config,
        protocol: Protocol,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = build_client(tls_config, protocol.clone())?;
        Ok(Self {
            client,
            protocol,
            _phantom: PhantomData,
        })
    }

    /// Performs the actual request to the OTLP endpoint and handles the response
    ///
    /// # Arguments
    /// * `request` - The HTTP request to send
    ///
    /// # Returns
    /// * `Result<T, ExporterError>` - The decoded response or an error
    async fn perform_request(&self, request: Request<Full<Bytes>>) -> Result<T, ExporterError> {
        match self.client.request(request).await {
            Ok(response) => {
                let (mut body, encoding) = process_head(response)?;
                let mut resp = T::default();
                while let Some(next) = body.frame().await {
                    match next {
                        Ok(frame) => {
                            if frame.is_data() {
                                let data = frame.into_data().unwrap();

                                match self.protocol {
                                    Protocol::Grpc => {
                                        match grpc_codec::grpc_decode_body::<T>(data) {
                                            Ok(r) => resp = r,
                                            Err(e) => return Err(e),
                                        }
                                    }
                                    Protocol::Http => {
                                        match http_codec::http_decode_body(data, encoding.is_some())
                                        {
                                            Ok(r) => resp = r,
                                            Err(e) => return Err(e),
                                        }
                                    }
                                }
                            } else if frame.is_trailers() {
                                let trailers = frame.into_trailers().unwrap();

                                match Status::from_header_map(&trailers) {
                                    None => {
                                        return Err(ExporterError::Generic(
                                            "unable to parse trailer headers".into(),
                                        ))
                                    }
                                    Some(status) => {
                                        if status.code() != tonic::Code::Ok {
                                            return Err(ExporterError::Grpc(status));
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(ExporterError::Generic(format!(
                                "failed reading grpc response: {}",
                                e
                            )));
                        }
                    }
                }

                Ok(resp)
            }
            Err(status) => {
                if status.is_connect() {
                    Err(ExporterError::Connect)
                } else {
                    Err(ExporterError::Generic(format!(
                        "failed request: {:?}",
                        status.source()
                    )))
                }
            }
        }
    }
}

/// Processes the response headers and body, handling status codes and content encoding
///
/// # Arguments
/// * `response` - The HTTP response to process
///
/// # Returns
/// * `Result<(Incoming, Option<HeaderValue>), ExporterError>` - The processed body and content encoding
fn process_head(
    response: Response<Incoming>,
) -> Result<(Incoming, Option<HeaderValue>), ExporterError> {
    let (head, body) = response.into_parts();

    if head.status != 200 {
        return Err(ExporterError::Http(head.status));
    }

    // grpc responses encode the compression within the payload, for HTTP responses
    // we must identify them from the Content-Encoding header
    let encoding = head.headers.get(CONTENT_ENCODING);
    if encoding.is_some_and(|ce| ce != "gzip") {
        return Err(ExporterError::Generic(format!(
            "unknown content encoding: {:?}",
            encoding.unwrap()
        )));
    }

    // If we get an invalid status from headers, return immediately
    let header_status = Status::from_header_map(&head.headers);
    if let Some(status) = header_status.clone() {
        if status.code() != tonic::Code::Ok {
            return Err(ExporterError::Grpc(status));
        }
    }
    // Extra clone here to dry up, we might not want this. We could instead of splitting into
    // parts above, do that before the call and pass in the Option<&HeaderValue>
    Ok((body, encoding.cloned()))
}

/// Builds an HTTP client with the specified TLS configuration and protocol settings
///
/// # Arguments
/// * `tls_config` - TLS configuration for secure communication
/// * `protocol` - The protocol (HTTP or gRPC) to use for communication
///
/// # Returns
/// * `Result<HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>, Box<dyn Error + Send + Sync>>` - The configured client or an error
fn build_client(
    tls_config: Config,
    protocol: Protocol,
) -> Result<HyperClient<HttpsConnector<HttpConnector>, Full<Bytes>>, Box<dyn Error + Send + Sync>> {
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
        .http2_only(protocol == Protocol::Grpc)
        .timer(TokioTimer::new())
        .build::<_, Full<Bytes>>(https);

    Ok(client)
}
