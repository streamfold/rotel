// SPDX-License-Identifier: Apache-2.0

/// Provides functionality for exporting telemetry data via OTLP protocol
///
/// This module contains the core OTLP exporter implementation that handles:
/// - Concurrent request processing and encoding
/// - Retry and timeout policy enforcement
/// - Graceful shutdown and request draining
/// - Support for both metrics and traces telemetry
use crate::exporters::otlp::config::{
    OTLPExporterConfig, OTLPExporterLogsConfig, OTLPExporterMetricsConfig, OTLPExporterTracesConfig,
};
use crate::exporters::otlp::errors::ExporterError;
use crate::exporters::otlp::grpc_codec::grpc_encode_body;
use crate::exporters::otlp::http_codec::http_encode_body;
use crate::exporters::otlp::signer::{RequestSigner, RequestSignerBuilder};
use crate::exporters::otlp::{CompressionEncoding, Endpoint, Protocol};
use crate::telemetry::{Counter, RotelCounter};
use bytes::Bytes;
use http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, USER_AGENT};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request};
use http_body_util::Full;
use opentelemetry::KeyValue;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use std::error::Error;
use std::marker::PhantomData;
use tower::BoxError;

/// Service path for gRPC trace exports
const TRACE_GRPC_SERVICE_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
/// Service path for gRPC metrics exports
const METRICS_GRPC_SERVICE_PATH: &str =
    "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
/// Service path for gRPC logs exports
const LOGS_GRPC_SERVICE_PATH: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

// These are relative so that they can be joined with existing URLs
/// Relative HTTP path for trace exports
const TRACES_RELATIVE_HTTP_PATH: &str = "v1/traces";
/// Relative HTTP path for metric exports
const METRICS_RELATIVE_HTTP_PATH: &str = "v1/metrics";
/// Relative HTTP path for metric exports
const LOGS_RELATIVE_HTTP_PATH: &str = "v1/logs";

/// Builder for constructing OTLP export requests.
///
/// Generic type `T` represents the type of request being built, e.g. ExportTraceServiceRequest.
#[derive(Clone)]
pub struct RequestBuilder<T, Signer>
where
    Signer: Clone,
{
    config: RequestBuilderConfig,
    _phantom: PhantomData<T>,
    pub send_failed: RotelCounter<u64>,
    signer: Option<Signer>,
}

/// Configuration for the request builder.
///
/// Contains all necessary information to construct OTLP export requests.
#[derive(Clone)]
pub struct RequestBuilderConfig {
    uri: String,
    pub protocol: Protocol,
    compression: Option<CompressionEncoding>,
    default_headers: HeaderMap,
}

use crate::topology::payload::MessageMetadata;

#[derive(Clone)]
pub struct EncodedRequest {
    pub request: Request<Full<Bytes>>,
    pub size: usize,
    pub metadata: Option<Vec<MessageMetadata>>,
}

/// Creates a new RequestBuilder for trace exports.
///
/// # Arguments
/// * `traces_config` - Configuration for the traces exporter
///
/// # Returns
/// * `Result<RequestBuilder<ExportTraceServiceRequest>, Box<dyn Error + Send + Sync>>`
pub fn build_traces<SignerBuilder>(
    traces_config: &OTLPExporterTracesConfig,
    send_failed: RotelCounter<u64>,
    signer_builder: Option<SignerBuilder>,
) -> Result<RequestBuilder<ExportTraceServiceRequest, SignerBuilder::Signer>, BoxError>
where
    SignerBuilder: RequestSignerBuilder,
    SignerBuilder::Signer: RequestSigner + Clone,
{
    let rbc = get_request_builder_config(
        traces_config,
        TRACE_GRPC_SERVICE_PATH,
        TRACES_RELATIVE_HTTP_PATH,
    )?;

    let signer = match signer_builder {
        None => None,
        Some(b) => Some(b.build(rbc.uri.as_str())?),
    };

    Ok(RequestBuilder::new(rbc, send_failed, signer))
}

/// Creates a new RequestBuilder for metric exports.
///
/// # Arguments
/// * `metrics_config` - Configuration for the metrics exporter
///
/// # Returns
/// * `Result<RequestBuilder<ExportMetricsServiceRequest>, Box<dyn Error + Send + Sync>>`
pub fn build_metrics<SignerBuilder>(
    metrics_config: &OTLPExporterMetricsConfig,
    send_failed: RotelCounter<u64>,
    signer_builder: Option<SignerBuilder>,
) -> Result<
    RequestBuilder<ExportMetricsServiceRequest, SignerBuilder::Signer>,
    Box<dyn Error + Send + Sync>,
>
where
    SignerBuilder: RequestSignerBuilder,
    SignerBuilder::Signer: RequestSigner + Clone,
{
    let rbc = get_request_builder_config(
        metrics_config,
        METRICS_GRPC_SERVICE_PATH,
        METRICS_RELATIVE_HTTP_PATH,
    )?;

    let signer = match signer_builder {
        None => None,
        Some(b) => Some(b.build(rbc.uri.as_str())?),
    };

    Ok(RequestBuilder::new(rbc, send_failed, signer))
}

/// Creates a new RequestBuilder for metric exports.
///
/// # Arguments
/// * `logs_config` - Configuration for the logs exporter
///
/// # Returns
/// * `Result<RequestBuilder<ExportLogsServiceRequest>, Box<dyn Error + Send + Sync>>`
pub fn build_logs<SignerBuilder>(
    logs_config: &OTLPExporterLogsConfig,
    send_failed: RotelCounter<u64>,
    signer_builder: Option<SignerBuilder>,
) -> Result<
    RequestBuilder<ExportLogsServiceRequest, SignerBuilder::Signer>,
    Box<dyn Error + Send + Sync>,
>
where
    SignerBuilder: RequestSignerBuilder,
    SignerBuilder::Signer: RequestSigner + Clone,
{
    let rbc =
        get_request_builder_config(logs_config, LOGS_GRPC_SERVICE_PATH, LOGS_RELATIVE_HTTP_PATH)?;

    let signer = match signer_builder {
        None => None,
        Some(b) => Some(b.build(rbc.uri.as_str())?),
    };

    Ok(RequestBuilder::new(rbc, send_failed, signer))
}

/// Creates the configuration for a RequestBuilder.
///
/// # Arguments
/// * `config` - The OTLP exporter configuration
/// * `grpc_path` - The service path for gRPC requests
/// * `http_relative_path` - The relative path for HTTP requests
///
/// # Returns
/// * `Result<RequestBuilderConfig, Box<dyn Error + Send + Sync>>`
fn get_request_builder_config(
    config: &OTLPExporterConfig,
    grpc_path: &str,
    http_relative_path: &str,
) -> Result<RequestBuilderConfig, Box<dyn Error + Send + Sync>> {
    let uri = endpoint_build(
        &config.endpoint,
        &config.protocol,
        grpc_path,
        http_relative_path,
    )?;

    let mut headermap = HeaderMap::new();

    // build common headers across requests, can these be more efficiently reused?
    match config.protocol {
        Protocol::Grpc => {
            headermap.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
            headermap.insert(
                "grpc-accept-encoding",
                HeaderValue::from_static("gzip,identity"),
            );
            headermap.insert("Te", HeaderValue::from_static("trailers"));
            if config.compression.is_some() {
                headermap.insert("grpc-encoding", HeaderValue::from_static("gzip"));
            }
        }
        Protocol::Http => {
            headermap.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            );
            if config.compression.is_some() {
                headermap.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
            }
        }
    }

    headermap.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
    headermap.insert(
        USER_AGENT,
        HeaderValue::from_static("Rotel Rust/1.84.1 hyper/1.52.0"),
    ); // todo

    for (k, v) in config.headers.clone() {
        let key = match k.parse::<HeaderName>() {
            Ok(k) => k,
            Err(e) => return Err(format!("invalid header key: {}: {}", k, e).into()),
        };
        let value = match v.parse::<HeaderValue>() {
            Ok(v) => v,
            Err(e) => return Err(format!("invalid header value: {}: {}", v, e).into()),
        };

        headermap.insert(key, value);
    }

    Ok(RequestBuilderConfig {
        uri: uri.to_string(),
        protocol: config.protocol.clone(),
        compression: config.compression.clone(),
        default_headers: headermap,
    })
}

impl<T: prost::Message, Signer: RequestSigner + Clone> RequestBuilder<T, Signer> {
    /// Creates a new RequestBuilder with the given configuration.
    fn new(
        config: RequestBuilderConfig,
        send_failed: RotelCounter<u64>,
        signer: Option<Signer>,
    ) -> Self {
        Self {
            config,
            send_failed,
            signer,
            _phantom: PhantomData,
        }
    }

    /// Encodes a message into a full HTTP request.
    ///
    /// # Arguments
    /// * `message` - The message to encode
    ///
    /// # Returns
    /// * `Result<Request<Full<Bytes>>, ExporterError>`
    pub fn encode(
        &self,
        message: T,
        size: usize,
        metadata: Option<Vec<MessageMetadata>>,
    ) -> Result<EncodedRequest, ExporterError> {
        let res = self.new_request(message);
        match res {
            Ok(request) => Ok(EncodedRequest {
                request,
                size,
                metadata,
            }),
            Err(e) => {
                self.send_failed
                    .add(size as u64, &[KeyValue::new("error", "request.encode")]);
                Err(ExporterError::Generic(format!(
                    "error encoding request: {}",
                    e
                )))
            }
        }
    }

    /// Creates a new request from a message.
    ///
    /// # Arguments
    /// * `message` - The message to include in the request
    ///
    /// # Returns
    /// * `Result<Request<Full<Bytes>>, Box<dyn Error>>`
    fn new_request(&self, message: T) -> Result<Request<Full<Bytes>>, Box<dyn Error>> {
        let body = match self.config.protocol {
            Protocol::Grpc => grpc_encode_body(message, self.config.compression.is_some()),
            Protocol::Http => http_encode_body(message, self.config.compression.is_some()),
        }?;

        self.request_from_bytes(body)
            .map_err(|e| format!("failed to build request: {}", e).into())
    }

    /// Creates a request from encoded bytes.
    ///
    /// # Arguments
    /// * `body` - The encoded request body
    /// * `uri` - The URI for the request
    /// * `header_map` - Headers to include in the request
    ///
    /// # Returns
    /// * `Result<Request<Full<Bytes>>, BoxError>`
    fn request_from_bytes(&self, body: Bytes) -> Result<Request<Full<Bytes>>, BoxError> {
        match self.signer.as_ref() {
            None => self.unsigned_request_from_bytes(
                body,
                self.config.uri.clone(),
                &self.config.default_headers,
            ),
            Some(signer) => {
                match signer.sign(Method::POST, self.config.default_headers.clone(), body) {
                    Ok(req) => Ok(req),
                    Err(e) => Err(format!("unable to sign OTLP request: {}", e).into()),
                }
            }
        }
    }

    fn unsigned_request_from_bytes(
        &self,
        body: Bytes,
        uri: String,
        header_map: &HeaderMap,
    ) -> Result<Request<Full<Bytes>>, BoxError> {
        let mut builder = hyper::Request::builder().method(Method::POST).uri(uri);

        let hdrs = builder.headers_mut().unwrap();
        for (k, v) in header_map {
            hdrs.insert(k, v.clone());
        }

        builder.body(Full::from(body)).map_err(|e| e.into())
    }
}

/// Builds a complete endpoint URL based on the configuration.
///
/// # Arguments
/// * `endpoint` - The base endpoint configuration
/// * `protocol` - The protocol to use (HTTP or gRPC)
/// * `grpc_path` - The service path for gRPC requests
/// * `http_relative_path` - The relative path for HTTP requests
///
/// # Returns
/// * `Result<url::Url, Box<dyn Error + Send + Sync>>`
fn endpoint_build(
    endpoint: &Endpoint,
    protocol: &Protocol,
    grpc_path: &str,
    http_relative_path: &str,
) -> Result<url::Url, Box<dyn Error + Send + Sync>> {
    let mut uri: url::Url = match endpoint {
        Endpoint::Base(s) => s,
        Endpoint::Full(s) => s,
    }
    .parse()
    .map_err(|e| format!("failed to parse endpoint {:?}: {}", endpoint, e))?;

    if uri.cannot_be_a_base() && uri.scheme() != "http" && uri.scheme() != "https" {
        // Attempt to manually add the scheme
        uri = url::Url::parse(format!("{}{}", "http://", uri).as_str())?
    }

    let uri = match protocol {
        // The gRPC path is always absolute, so it overwrites any existing paths
        Protocol::Grpc => uri.join(grpc_path).unwrap(),
        // For HTTP we assume that an override (type Full) is fully formed, including the path
        Protocol::Http => match endpoint {
            Endpoint::Base(_) => {
                // If the provided URL does not end in `/`, then add. Otherwise the join will drop
                // the last segment of the URL.
                if !uri.path().ends_with("/") {
                    uri = format!("{}/", uri)
                        .parse()
                        .map_err(|e| format!("failed to parse endpoint {:?}: {}", uri, e))?;
                }

                uri.join(http_relative_path).unwrap()
            }
            Endpoint::Full(_) => uri,
        },
    };

    Ok(uri)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::exporters::otlp::Endpoint::{Base, Full};
    use crate::exporters::otlp::Protocol::{Grpc, Http};

    #[test]
    fn test_endpoint_build() {
        // grpc permutations
        let r = build(&Full("localhost:4317".to_string()), &Grpc);
        assert_eq!(
            r,
            ["http://localhost:4317", TRACE_GRPC_SERVICE_PATH].join("")
        );
        let r = build(&Base("localhost:4317".to_string()), &Grpc);
        assert_eq!(
            r,
            ["http://localhost:4317", TRACE_GRPC_SERVICE_PATH].join("")
        );
        let r = build(&Base("http://localhost:4317".to_string()), &Grpc);
        assert_eq!(
            r,
            ["http://localhost:4317", TRACE_GRPC_SERVICE_PATH].join("")
        );
        let r = build(&Base("https://localhost:4317".to_string()), &Grpc);
        assert_eq!(
            r,
            ["https://localhost:4317", TRACE_GRPC_SERVICE_PATH].join("")
        );

        // http with base URL
        let r = build(&Base("localhost:4318".to_string()), &Http);
        assert_eq!(r, ["http://localhost:4318", "/v1/traces"].join(""));
        let r = build(&Base("http://localhost:4318".to_string()), &Http);
        assert_eq!(r, ["http://localhost:4318", "/v1/traces"].join(""));
        let r = build(&Base("http://localhost:4318/".to_string()), &Http);
        assert_eq!(r, ["http://localhost:4318", "/v1/traces"].join(""));
        let r = build(&Base("http://localhost:4318/otlp".to_string()), &Http);
        assert_eq!(r, ["http://localhost:4318", "/otlp/v1/traces"].join(""));
        let r = build(&Base("http://localhost:4318/otlp/".to_string()), &Http);
        assert_eq!(r, ["http://localhost:4318", "/otlp/v1/traces"].join(""));

        let r = build(
            &Full("http://localhost:4318/something/custom".to_string()),
            &Http,
        );
        assert_eq!(r, "http://localhost:4318/something/custom");
    }

    fn build(endpoint: &Endpoint, protocol: &Protocol) -> String {
        endpoint_build(
            endpoint,
            protocol,
            TRACE_GRPC_SERVICE_PATH,
            TRACES_RELATIVE_HTTP_PATH,
        )
        .unwrap()
        .to_string()
    }
}
