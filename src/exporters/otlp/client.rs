// SPDX-License-Identifier: Apache-2.0

use crate::exporters::http::client::{Client, Protocol as HttpProtocol, ResponseDecode};
use crate::exporters::http::response::Response as HttpResponse;
use crate::exporters::http::tls::Config;
use crate::exporters::http::types::ContentEncoding;
use crate::exporters::otlp::request::EncodedRequest;
use crate::exporters::otlp::{Protocol, grpc_codec, http_codec};
use crate::telemetry::{Counter, RotelCounter};
use bytes::Bytes;
use http::StatusCode;
use http_body_util::Full;
use opentelemetry::KeyValue;
use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::codegen::Service;
use tower::BoxError;

/// ResponseDecode implementation for gRPC codec
#[derive(Clone)]
pub struct GrpcDecoder<T> {
    send_failed: RotelCounter<u64>,
    _phantom: PhantomData<T>,
}

impl<T> GrpcDecoder<T>
where
    T: prost::Message + Default,
{
    pub fn new(send_failed: RotelCounter<u64>) -> Self {
        Self {
            send_failed,
            _phantom: PhantomData,
        }
    }
}

impl<T> ResponseDecode<T> for GrpcDecoder<T>
where
    T: prost::Message + Default,
{
    fn decode(&self, body: Bytes, _encoding: ContentEncoding) -> Result<T, BoxError> {
        let body_len = body.len();
        // For gRPC, we don't use the ContentEncoding parameter since compression is handled in the gRPC framing
        grpc_codec::grpc_decode_body::<T>(body, self.send_failed.clone(), body_len as u64)
            .map_err(|e| e.into())
    }
}

/// ResponseDecode implementation for HTTP codec
#[derive(Clone)]
pub struct HttpDecoder<T> {
    send_failed: RotelCounter<u64>,
    _phantom: PhantomData<T>,
}

impl<T> HttpDecoder<T>
where
    T: prost::Message + Default,
{
    pub fn new(send_failed: RotelCounter<u64>) -> Self {
        Self {
            send_failed,
            _phantom: PhantomData,
        }
    }
}

impl<T> ResponseDecode<T> for HttpDecoder<T>
where
    T: prost::Message + Default,
{
    fn decode(&self, body: Bytes, encoding: ContentEncoding) -> Result<T, BoxError> {
        let compressed = encoding == ContentEncoding::Gzip;
        let body_len = body.len();
        http_codec::http_decode_body::<T>(
            body,
            compressed,
            self.send_failed.clone(),
            body_len as u64,
        )
        .map_err(|e| e.into())
    }
}

/// Enum to hold either gRPC or HTTP client. This helps hide the
/// decoder types from the client type
#[derive(Clone)]
enum UnifiedClientType<T> {
    Grpc(Client<Full<Bytes>, T, GrpcDecoder<T>>),
    Http(Client<Full<Bytes>, T, HttpDecoder<T>>),
}

/// Client struct for handling OTLP exports.
/// Generic over message type T which must implement prost::Message, i.e. ExportTraceServiceRequest.
#[derive(Clone)]
pub struct OTLPClient<T>
where
    T: prost::Message + Default,
{
    /// The underlying unified HTTP client
    client: UnifiedClientType<T>,
    /// PhantomData to handle generic type T
    _phantom: PhantomData<T>,
    send_failed: RotelCounter<u64>,
    sent: RotelCounter<u64>,
}

/// Implementation of Tower's Service trait for OTLPClient
impl<T> Service<EncodedRequest> for OTLPClient<T>
where
    T: prost::Message + Default + Clone + Send + Sync + 'static,
{
    type Response = HttpResponse<T>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// Checks if the service is ready to process requests
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    /// Processes the request and returns a Future containing the response
    fn call(&mut self, req: EncodedRequest) -> Self::Future {
        let mut client = self.client.clone();
        let send_failed = self.send_failed.clone();
        let sent = self.sent.clone();
        let req_size = req.size;

        Box::pin(async move {
            let result = Self::perform_request_with_client(&mut client, req).await;

            match result {
                Ok(resp) => match &resp {
                    HttpResponse::Http(parts, _) => {
                        if parts.status == StatusCode::OK {
                            sent.add(req_size as u64, &[]);
                        } else {
                            send_failed.add(
                                req_size as u64,
                                &[
                                    KeyValue::new("error", "http_status"),
                                    KeyValue::new("value", parts.status.to_string()),
                                ],
                            );
                        }

                        Ok(resp)
                    }
                    HttpResponse::Grpc(status, _) => {
                        if status.code() != tonic::Code::Ok {
                            send_failed.add(
                                req_size as u64,
                                &[
                                    KeyValue::new("error", "grpc_status"),
                                    KeyValue::new("value", status.code().to_string()),
                                ],
                            );
                        } else {
                            sent.add(req_size as u64, &[]);
                        }
                        Ok(resp)
                    }
                },
                Err(e) => {
                    send_failed.add(req_size as u64, &[KeyValue::new("error", "request_failed")]);
                    Err(e)
                }
            }
        })
    }
}

impl<T> OTLPClient<T>
where
    T: prost::Message + Default + Send + Sync + Clone + 'static,
{
    pub fn new(
        tls_config: Config,
        protocol: Protocol,
        sent: RotelCounter<u64>,
        send_failed: RotelCounter<u64>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = match protocol {
            Protocol::Grpc => {
                let decoder: GrpcDecoder<T> = GrpcDecoder::new(send_failed.clone());
                let client = Client::build(tls_config, HttpProtocol::Grpc, decoder)?;
                UnifiedClientType::Grpc(client)
            }
            Protocol::Http => {
                let decoder: HttpDecoder<T> = HttpDecoder::new(send_failed.clone());
                let client = Client::build(tls_config, HttpProtocol::Http, decoder)?;
                UnifiedClientType::Http(client)
            }
        };

        Ok(Self {
            client,
            sent,
            send_failed,
            _phantom: PhantomData,
        })
    }

    async fn perform_request_with_client(
        client: &mut UnifiedClientType<T>,
        encoded_request: EncodedRequest,
    ) -> Result<HttpResponse<T>, BoxError> {
        match client {
            UnifiedClientType::Grpc(client) => client.call(encoded_request.request).await,
            UnifiedClientType::Http(client) => client.call(encoded_request.request).await,
        }
    }
}
