// SPDX-License-Identifier: Apache-2.0

use crate::exporters::otlp::Authenticator;
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
use crate::exporters::otlp::payload::OtlpPayload;
use crate::exporters::otlp::{CompressionEncoding, Endpoint, Protocol};
use crate::telemetry::{Counter, RotelCounter};
use crate::topology::payload::MessageMetadata;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use http::header::AUTHORIZATION;
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
pub struct RequestBuilder<T> {
    config: RequestBuilderConfig,
    _phantom: PhantomData<T>,
    pub send_failed: RotelCounter<u64>,
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

/// Creates a new RequestBuilder for trace exports.
///
/// # Arguments
/// * `traces_config` - Configuration for the traces exporter
///
/// # Returns
/// * `Result<RequestBuilder<ExportTraceServiceRequest>, Box<dyn Error + Send + Sync>>`
pub fn build_traces(
    traces_config: &OTLPExporterTracesConfig,
    send_failed: RotelCounter<u64>,
) -> Result<RequestBuilder<ExportTraceServiceRequest>, BoxError> {
    let rbc = get_request_builder_config(
        traces_config,
        TRACE_GRPC_SERVICE_PATH,
        TRACES_RELATIVE_HTTP_PATH,
    )?;

    Ok(RequestBuilder::new(rbc, send_failed))
}

/// Creates a new RequestBuilder for metric exports.
///
/// # Arguments
/// * `metrics_config` - Configuration for the metrics exporter
///
/// # Returns
/// * `Result<RequestBuilder<ExportMetricsServiceRequest>, Box<dyn Error + Send + Sync>>`
pub fn build_metrics(
    metrics_config: &OTLPExporterMetricsConfig,
    send_failed: RotelCounter<u64>,
) -> Result<RequestBuilder<ExportMetricsServiceRequest>, Box<dyn Error + Send + Sync>> {
    let rbc = get_request_builder_config(
        metrics_config,
        METRICS_GRPC_SERVICE_PATH,
        METRICS_RELATIVE_HTTP_PATH,
    )?;

    Ok(RequestBuilder::new(rbc, send_failed))
}

/// Creates a new RequestBuilder for metric exports.
///
/// # Arguments
/// * `logs_config` - Configuration for the logs exporter
///
/// # Returns
/// * `Result<RequestBuilder<ExportLogsServiceRequest>, Box<dyn Error + Send + Sync>>`
pub fn build_logs(
    logs_config: &OTLPExporterLogsConfig,
    send_failed: RotelCounter<u64>,
) -> Result<RequestBuilder<ExportLogsServiceRequest>, Box<dyn Error + Send + Sync>> {
    let rbc =
        get_request_builder_config(logs_config, LOGS_GRPC_SERVICE_PATH, LOGS_RELATIVE_HTTP_PATH)?;

    Ok(RequestBuilder::new(rbc, send_failed))
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

    // Add Basic Auth header if configured
    if let Some(Authenticator::Basic { username, password }) = &config.authenticator {
        let credentials = format!("{}:{}", username, password);
        let encoded = BASE64.encode(credentials.as_bytes());
        let auth_value = format!("Basic {}", encoded);
        headermap.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value)
                .map_err(|e| format!("invalid basic auth header value: {}", e))?,
        );
    }

    Ok(RequestBuilderConfig {
        uri: uri.to_string(),
        protocol: config.protocol.clone(),
        compression: config.compression.clone(),
        default_headers: headermap,
    })
}

impl<T: prost::Message> RequestBuilder<T> {
    /// Creates a new RequestBuilder with the given configuration.
    fn new(config: RequestBuilderConfig, send_failed: RotelCounter<u64>) -> Self {
        Self {
            config,
            send_failed,
            _phantom: PhantomData,
        }
    }

    pub fn uri(&self) -> String {
        self.config.uri.clone()
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
    ) -> Result<Request<OtlpPayload>, ExporterError> {
        let res = self.new_request(message);
        match res {
            Ok(request) => {
                // Decompose the request to get parts and body
                let (parts, body) = request.into_parts();

                // Create OtlpPayload with just the body and metadata
                let payload = OtlpPayload::new(body, metadata, size);

                // Reconstruct request with the payload as the body
                let wrapped_request = Request::from_parts(parts, payload);

                Ok(wrapped_request)
            }
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

    /// Creates an HTTP request from raw bytes.
    ///
    /// Constructs a POST request with the configured URI and default headers,
    /// using the provided bytes as the request body.
    ///
    /// # Arguments
    /// * `body` - The raw bytes to use as the request body
    ///
    /// # Returns
    /// * `Result<Request<Full<Bytes>>, BoxError>` - The constructed HTTP request or an error
    fn request_from_bytes(&self, body: Bytes) -> Result<Request<Full<Bytes>>, BoxError> {
        let mut builder = hyper::Request::builder()
            .method(Method::POST)
            .uri(&self.config.uri);

        let hdrs = builder.headers_mut().unwrap();
        for (k, v) in &self.config.default_headers {
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

    #[test]
    fn test_basic_auth_header() {
        use crate::exporters::http::retry::RetryConfig;
        use crate::exporters::otlp::config::OTLPExporterConfig;

        // Create a config with Basic auth
        let config = OTLPExporterConfig::new(
            "test".to_string(),
            Base("http://localhost:4317".to_string()),
            Grpc,
            RetryConfig::default(),
        )
        .with_authenticator(Some(Authenticator::Basic {
            username: "testuser".to_string(),
            password: "testpass".to_string(),
        }));

        let rbc =
            get_request_builder_config(&config, TRACE_GRPC_SERVICE_PATH, TRACES_RELATIVE_HTTP_PATH)
                .expect("failed to build request config");

        // Verify the Authorization header is present with correct value
        let auth_header = rbc
            .default_headers
            .get(AUTHORIZATION)
            .expect("Authorization header should be present");

        // Base64 encode "testuser:testpass" = "dGVzdHVzZXI6dGVzdHBhc3M="
        assert_eq!(
            auth_header.to_str().unwrap(),
            "Basic dGVzdHVzZXI6dGVzdHBhc3M="
        );
    }

    #[test]
    fn test_no_auth_header_when_none() {
        use crate::exporters::http::retry::RetryConfig;
        use crate::exporters::otlp::config::OTLPExporterConfig;
        use http::header::AUTHORIZATION;

        // Create a config without auth
        let config = OTLPExporterConfig::new(
            "test".to_string(),
            Base("http://localhost:4317".to_string()),
            Grpc,
            RetryConfig::default(),
        );

        let rbc =
            get_request_builder_config(&config, TRACE_GRPC_SERVICE_PATH, TRACES_RELATIVE_HTTP_PATH)
                .expect("failed to build request config");

        // Verify the Authorization header is NOT present
        assert!(rbc.default_headers.get(AUTHORIZATION).is_none());
    }
}
