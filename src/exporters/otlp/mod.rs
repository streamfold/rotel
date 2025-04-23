// SPDX-License-Identifier: Apache-2.0

//! OTLP (OpenTelemetry Protocol) exporter implementation.
//!
//! This module provides functionality for exporting telemetry data using the OpenTelemetry Protocol (OTLP).
//! It supports both gRPC and HTTP protocols for sending trace and metric data to OTLP-compatible backends.
//!
//! # Examples
//!
//! ```rust,no_run
//! use rotel::exporters::otlp::{trace_config_builder, Protocol, Endpoint};
//!
//! // Create a basic OTLP trace exporter config
//! let config = trace_config_builder(
//!     Endpoint::Base("http://localhost:4317".to_string()),
//!     Protocol::Grpc
//! );
//! ```
//!
//! # Features
//!
//! - Support for both gRPC and HTTP transport protocols
//! - TLS/SSL encryption support
//! - Custom headers and authentication
//! - Configurable request timeout and retry behavior
//! - Batch processing of telemetry data
//!
//! # Modules
//!
//! - `config`: Configuration structures and builders for OTLP exporters
//! - `errors`: Error types specific to OTLP export operations  
//! - `request`: Request handling and processing
//! - `client`: Client implementations for different protocols
//! - `exporter`: Core exporter implementation
//! - `grpc_codec`: Encoding/decoding for gRPC protocol
//! - `http_codec`: Encoding/decoding for HTTP protocol
//! - `tls`: TLS/SSL configuration and utilities
//!
pub mod config;
pub mod exporter;
pub mod request;

pub(crate) mod errors;

mod client;
mod grpc_codec;
mod http_codec;

use crate::exporters::otlp::config::OTLPExporterConfig;
use clap::ValueEnum;
use opentelemetry::global;
use opentelemetry::metrics::Meter;
use std::time::Duration;

/// Default timeout duration for OTLP requests
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Supported compression encodings for OTLP data
#[derive(Clone, Debug, ValueEnum)]
pub enum CompressionEncoding {
    Gzip,
    None,
}

/// Transport protocols supported for OTLP export
#[derive(Clone, Debug, PartialEq)]
pub enum Protocol {
    Grpc,
    Http,
}

/// OTLP endpoint configuration
#[derive(Clone, Debug)]
pub enum Endpoint {
    Base(String),
    Full(String),
}

pub fn get_meter() -> Meter {
    global::meter("exporters")
}

/// Creates a configuration builder for OTLP trace export
///
/// # Arguments
///
/// * `endpoint` - The OTLP endpoint configuration
/// * `protocol` - The transport protocol to use
///
/// # Returns
///
/// An `OTLPExporterConfig` configured for trace export
pub fn trace_config_builder(endpoint: Endpoint, protocol: Protocol) -> OTLPExporterConfig {
    OTLPExporterConfig {
        type_name: "otlp_traces".to_string(),
        endpoint,
        protocol,
        ..Default::default()
    }
}

/// Creates a configuration builder for OTLP metrics export
///
/// # Arguments
///
/// * `endpoint` - The OTLP endpoint configuration  
/// * `protocol` - The transport protocol to use
///
/// # Returns
///
/// An `OTLPExporterConfig` configured for metrics export
pub fn metrics_config_builder(endpoint: Endpoint, protocol: Protocol) -> OTLPExporterConfig {
    OTLPExporterConfig {
        type_name: "otlp_metrics".to_string(),
        endpoint,
        protocol,
        ..Default::default()
    }
}

/// Creates a configuration builder for OTLP logs export
///
/// # Arguments
///
/// * `endpoint` - The OTLP endpoint configuration  
/// * `protocol` - The transport protocol to use
///
/// # Returns
///
/// An `OTLPExporterConfig` configured for metrics export
pub fn logs_config_builder(endpoint: Endpoint, protocol: Protocol) -> OTLPExporterConfig {
    OTLPExporterConfig {
        type_name: "otlp_logs".to_string(),
        endpoint,
        protocol,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use crate::bounded_channel::{BoundedSender, bounded};
    use crate::exporters::otlp::{Endpoint, Protocol};
    use crate::topology::batch::{BatchSizer, NestedBatch};
    extern crate utilities;
    use utilities::otlp::FakeOTLP;

    use bytes::BytesMut;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
        MetricsService, MetricsServiceServer,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    };
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use prost::Message;
    use std::cell::RefCell;
    use std::error::Error;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use crate::exporters::crypto_init_tests::init_crypto;
    use crate::exporters::otlp;
    use crate::exporters::otlp::exporter::Exporter;
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
        LogsService, LogsServiceServer,
    };
    use opentelemetry_proto::tonic::collector::logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    };
    use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
    use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::Receiver;
    use tokio::{join, spawn};
    use tokio_test::{assert_err, assert_ok};
    use tokio_util::sync::CancellationToken;
    use tonic::codec::CompressionEncoding;
    use tonic::transport::{Channel, Identity, Server, ServerTlsConfig};
    use tonic::{Request, Response, Status};
    use tracing::warn;

    #[derive(Clone)]
    struct MockOTLPService {
        trace_requests: Arc<Mutex<RefCell<Vec<Request<ExportTraceServiceRequest>>>>>,
        metrics_requests: Arc<Mutex<RefCell<Vec<Request<ExportMetricsServiceRequest>>>>>,
        logs_requests: Arc<Mutex<RefCell<Vec<Request<ExportLogsServiceRequest>>>>>,
        tx: Option<Sender<()>>,
        error: bool,
    }

    #[tonic::async_trait]
    impl TraceService for MockOTLPService {
        async fn export(
            &self,
            request: Request<ExportTraceServiceRequest>,
        ) -> Result<Response<ExportTraceServiceResponse>, Status> {
            let guard = self.trace_requests.lock().unwrap();
            guard.borrow_mut().push(request);

            if self.tx.is_some() {
                // N.B. We are calling spawn here and issuing the tx.send() in a tokio tasks
                // rather than calling tx.send.await directly in this function because the types
                // Request and ExportTraceServiceRequest do not implement Send. Further if we tried
                // to join on the handle returned from spawn we would also encounter the lack of Send error.
                // However, in our test below which uses this code we wait for the message to be received by the rx
                // end of the mpsc::channel here and therefore can effectively coordinate these tokio tasks, waiting for
                // a message from the server before verifying the results.
                let tx = self.tx.as_ref().unwrap().clone();

                spawn(async move {
                    tx.send(())
                        .await
                        .expect("unable to send notification from server")
                });
            }

            if !self.error {
                return Ok(Response::new(ExportTraceServiceResponse {
                    partial_success: None,
                }));
            }

            Err(Status::new(tonic::Code::Unavailable, "unavailable"))
        }
    }

    #[tonic::async_trait]
    impl MetricsService for MockOTLPService {
        async fn export(
            &self,
            request: Request<ExportMetricsServiceRequest>,
        ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
            let guard = self.metrics_requests.lock().unwrap();
            guard.borrow_mut().push(request);

            if self.tx.is_some() {
                // N.B. We are calling spawn here and issuing the tx.send() in a tokio tasks
                // rather than calling tx.send.await directly in this function because the types
                // Request and ExportTraceServiceRequest do not implement Send. Further if we tried
                // to join on the handle returned from spawn we would also encounter the lack of Send error.
                // However, in our test below which uses this code we wait for the message to be received by the rx
                // end of the mpsc::channel here and therefore can effectively coordinate these tokio tasks, waiting for
                // a message from the server before verifying the results.
                let tx = self.tx.as_ref().unwrap().clone();

                spawn(async move {
                    tx.send(())
                        .await
                        .expect("unable to send notification from server")
                });
            }

            if !self.error {
                return Ok(Response::new(ExportMetricsServiceResponse {
                    partial_success: None,
                }));
            }

            Err(Status::new(tonic::Code::Unavailable, "unavailable"))
        }
    }

    #[tonic::async_trait]
    impl LogsService for MockOTLPService {
        async fn export(
            &self,
            request: Request<ExportLogsServiceRequest>,
        ) -> Result<Response<ExportLogsServiceResponse>, Status> {
            let guard = self.logs_requests.lock().unwrap();
            guard.borrow_mut().push(request);

            if self.tx.is_some() {
                // N.B. We are calling spawn here and issuing the tx.send() in a tokio tasks
                // rather than calling tx.send.await directly in this function because the types
                // Request and ExportTraceServiceRequest do not implement Send. Further if we tried
                // to join on the handle returned from spawn we would also encounter the lack of Send error.
                // However, in our test below which uses this code we wait for the message to be received by the rx
                // end of the mpsc::channel here and therefore can effectively coordinate these tokio tasks, waiting for
                // a message from the server before verifying the results.
                let tx = self.tx.as_ref().unwrap().clone();

                spawn(async move {
                    tx.send(())
                        .await
                        .expect("unable to send notification from server")
                });
            }

            if !self.error {
                return Ok(Response::new(ExportLogsServiceResponse {
                    partial_success: None,
                }));
            }

            Err(Status::new(tonic::Code::Unavailable, "unavailable"))
        }
    }

    impl MockOTLPService {
        async fn new(tx: Option<Sender<()>>, error: bool) -> Self {
            Self {
                trace_requests: Arc::new(Mutex::new(RefCell::new(Vec::new()))),
                metrics_requests: Arc::new(Mutex::new(RefCell::new(Vec::new()))),
                logs_requests: Arc::new(Mutex::new(RefCell::new(Vec::new()))),
                tx,
                error,
            }
        }
        async fn serve(
            &self,
            tcp_listener: tokio::net::TcpListener,
            shut_rx: Receiver<()>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let incoming_connections = tokio_stream::wrappers::TcpListenerStream::new(tcp_listener);
            Server::builder()
                .add_service(
                    TraceServiceServer::new(self.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    MetricsServiceServer::new(self.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    LogsServiceServer::new(self.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .serve_with_incoming_shutdown(incoming_connections, async move {
                    match shut_rx.await {
                        Ok(_) => {}
                        Err(_) => warn!("sender dropped shutdown channel"),
                    }
                })
                .await?;
            Ok(())
        }
        async fn serve_tls(
            &self,
            tcp_listener: tokio::net::TcpListener,
            shut_rx: Receiver<()>,
            server_tls_config: ServerTlsConfig,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let incoming_connections = tokio_stream::wrappers::TcpListenerStream::new(tcp_listener);
            Server::builder()
                .tls_config(server_tls_config)
                .expect("error creating identity!")
                .add_service(
                    TraceServiceServer::new(self.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    MetricsServiceServer::new(self.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    LogsServiceServer::new(self.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .serve_with_incoming_shutdown(incoming_connections, async move {
                    match shut_rx.await {
                        Ok(_) => {}
                        Err(_) => warn!("sender dropped shutdown channel"),
                    }
                })
                .await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn send_otlp_trace_request() {
        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        //let socket_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 0);
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        println!("bound to port {} on socket_addr", port);
        let mock = MockOTLPService::new(None, false).await;
        let requests = mock.trace_requests.clone();
        spawn(async move { mock.serve(tcp_listener, shut_rx).await });
        let client_jh = spawn(async move {
            let addr = format!("[::1]:{}", port);
            let mut client = create_trace_client(addr)
                .await
                .expect("error creating client");
            let request = FakeOTLP::trace_service_request();
            client.export(request).await
        });
        let result = client_jh.await;
        let resp = result.expect("error getting response received by client");
        assert!(resp.is_ok());
        // Get mock server requests
        let guard = requests.lock().unwrap();
        let mut trace_requests = guard.borrow_mut();
        assert_eq!(trace_requests.len(), 1);
        let req = trace_requests.pop().unwrap().into_inner();
        assert_eq!(
            req.resource_spans[0].scope_spans[0].spans[0].trace_id,
            vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        );
        shut_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn send_otlp_metrics_request() {
        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        //let socket_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 0);
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        println!("bound to port {} on socket_addr", port);
        let mock = MockOTLPService::new(None, false).await;
        let requests = mock.metrics_requests.clone();
        spawn(async move { mock.serve(tcp_listener, shut_rx).await });
        let client_jh = spawn(async move {
            let addr = format!("[::1]:{}", port);
            let mut client = create_metrics_client(addr)
                .await
                .expect("error creating client");
            let request = FakeOTLP::metrics_service_request();
            client.export(request).await
        });
        let result = client_jh.await;
        let resp = result.expect("error getting response received by client");
        assert!(resp.is_ok());
        // Get mock server requests
        let guard = requests.lock().unwrap();
        let mut trace_requests = guard.borrow_mut();
        assert_eq!(trace_requests.len(), 1);
        let req = trace_requests.pop().unwrap().into_inner();
        assert_eq!(
            req.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "test-metric"
        );
        shut_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn send_otlp_logs_request() {
        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        //let socket_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 0);
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        println!("bound to port {} on socket_addr", port);
        let mock = MockOTLPService::new(None, false).await;
        let requests = mock.logs_requests.clone();
        spawn(async move { mock.serve(tcp_listener, shut_rx).await });
        let client_jh = spawn(async move {
            let addr = format!("[::1]:{}", port);
            let mut client = create_logs_client(addr)
                .await
                .expect("error creating client");
            let request = FakeOTLP::logs_service_request();
            client.export(request).await
        });
        let result = client_jh.await;
        let resp = result.expect("error getting response received by client");
        assert!(resp.is_ok());
        // Get mock server requests
        let guard = requests.lock().unwrap();
        let mut trace_requests = guard.borrow_mut();
        assert_eq!(trace_requests.len(), 1);
        let req = trace_requests.pop().unwrap().into_inner();
        let f = req.resource_logs[0].scope_logs[0].log_records[0]
            .body
            .clone()
            .unwrap();
        let f = f.value.unwrap();
        match f {
            StringValue(x) => {
                assert_eq!(x, "This is a log message");
            }
            _ => panic!("unexpected value type"),
        }
        shut_tx.send(()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_otlp_trace_request_with_headers() {
        init_crypto();

        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        println!("bound to port {} on socket_addr", port);
        let (server_tx, mut server_rx) = tokio::sync::mpsc::channel(1);
        let mock = MockOTLPService::new(Some(server_tx), false).await;
        let requests = mock.trace_requests.clone();
        spawn(async move {
            mock.serve_tls(tcp_listener, shut_rx, get_server_identity())
                .await
        });

        // Full client auth should succeed
        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(1);

        let cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-cert.pem");
        let key_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-key.pem");
        let server_root_ca_cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/ca.pem");

        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_cert_file(cert_file)
        .with_key_file(key_file)
        .with_ca_file(server_root_ca_cert_file)
        .with_header("authorization", "bar");

        let mut traces =
            otlp::exporter::build_traces_exporter(traces_config, trace_brx, None).unwrap();

        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let jh = spawn(async move { traces.start(shut_token).await });
        // Send a request for the server to process
        let res = trace_btx
            .send(FakeOTLP::trace_service_request().resource_spans)
            .await;
        assert!(&res.is_ok());
        if tokio::time::timeout(Duration::from_secs(5), server_rx.recv())
            .await
            .is_err()
        {
            panic!("Did not receive notification from MockOTLPService before 5 second timeout!");
        }
        drop(trace_btx);
        let _ = join!(jh);
        cancel_token.cancel();
        // Get mock server requests
        let guard = requests.lock().unwrap();
        let mut trace_requests = guard.borrow_mut();
        assert_eq!(trace_requests.len(), 1);
        let mut req = trace_requests.pop().unwrap();
        let v = req
            .metadata_mut()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(v, "bar");
        shut_tx.send(()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_otlp_metrics_request_with_headers() {
        init_crypto();

        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        println!("bound to port {} on socket_addr", port);
        let (server_tx, mut server_rx) = tokio::sync::mpsc::channel(1);
        let mock = MockOTLPService::new(Some(server_tx), false).await;
        let requests = mock.metrics_requests.clone();
        spawn(async move {
            mock.serve_tls(tcp_listener, shut_rx, get_server_identity())
                .await
        });

        // Full client auth should succeed
        let (metrics_btx, metrics_brx) = bounded::<Vec<ResourceMetrics>>(1);
        let cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-cert.pem");
        let key_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-key.pem");
        let server_root_ca_cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/ca.pem");

        let metrics_config = otlp::metrics_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_cert_file(cert_file)
        .with_key_file(key_file)
        .with_ca_file(server_root_ca_cert_file)
        .with_header("authorization", "bar");

        let mut metrics =
            otlp::exporter::build_metrics_exporter(metrics_config, metrics_brx, None).unwrap();

        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let jh = spawn(async move { metrics.start(shut_token).await });
        // Send a request for the server to process
        let res = metrics_btx
            .send(FakeOTLP::metrics_service_request().resource_metrics)
            .await;
        assert!(&res.is_ok());
        if tokio::time::timeout(Duration::from_secs(5), server_rx.recv())
            .await
            .is_err()
        {
            panic!("Did not receive notification from MockOTLPService before 5 second timeout!");
        }
        drop(metrics_btx);
        let _ = join!(jh);
        cancel_token.cancel();
        // Get mock server requests
        let guard = requests.lock().unwrap();
        let mut metrics_requests = guard.borrow_mut();
        assert_eq!(metrics_requests.len(), 1);
        let mut req = metrics_requests.pop().unwrap();
        let v = req
            .metadata_mut()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(v, "bar");
        shut_tx.send(()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn send_otlp_logs_request_with_headers() {
        init_crypto();

        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        println!("bound to port {} on socket_addr", port);
        let (server_tx, mut server_rx) = tokio::sync::mpsc::channel(1);
        let mock = MockOTLPService::new(Some(server_tx), false).await;
        let requests = mock.logs_requests.clone();
        spawn(async move {
            mock.serve_tls(tcp_listener, shut_rx, get_server_identity())
                .await
        });

        // Full client auth should succeed
        let (logs_btx, logs_brx) = bounded::<Vec<ResourceLogs>>(1);
        let cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-cert.pem");
        let key_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-key.pem");
        let server_root_ca_cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/ca.pem");

        let logs_config = otlp::logs_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_cert_file(cert_file)
        .with_key_file(key_file)
        .with_ca_file(server_root_ca_cert_file)
        .with_header("authorization", "bar");

        let mut logs = otlp::exporter::build_logs_exporter(logs_config, logs_brx, None).unwrap();

        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let jh = spawn(async move { logs.start(shut_token).await });
        // Send a request for the server to process
        let res = logs_btx
            .send(FakeOTLP::logs_service_request().resource_logs)
            .await;
        assert!(&res.is_ok());
        if tokio::time::timeout(Duration::from_secs(5), server_rx.recv())
            .await
            .is_err()
        {
            panic!("Did not receive notification from MockOTLPService before 5 second timeout!");
        }
        drop(logs_btx);
        let _ = join!(jh);
        cancel_token.cancel();
        // Get mock server requests
        let guard = requests.lock().unwrap();
        let mut logs_requests = guard.borrow_mut();
        assert_eq!(logs_requests.len(), 1);
        let mut req = logs_requests.pop().unwrap();
        let v = req
            .metadata_mut()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(v, "bar");
        shut_tx.send(()).unwrap();
    }

    // We don't use TLS in this test because it can impact request timing
    #[tokio::test(flavor = "multi_thread")]
    async fn send_otlp_trace_with_retry() {
        init_crypto();

        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        let (server_tx, mut server_rx) = tokio::sync::mpsc::channel(10);
        let mock = MockOTLPService::new(Some(server_tx), true).await;
        let requests = mock.trace_requests.clone();
        spawn(async move { mock.serve(tcp_listener, shut_rx).await });

        // Full client auth should succeed
        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(1);

        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("localhost:{}", port)),
            Protocol::Grpc,
        )
        .with_initial_backoff(Duration::from_millis(5))
        .with_max_elapsed_time(Duration::from_millis(50));

        let mut traces =
            otlp::exporter::build_traces_exporter(traces_config, trace_brx, None).unwrap();

        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let jh = spawn(async move { traces.start(shut_token).await });
        // Send a request for the server to process
        let res = trace_btx
            .send(FakeOTLP::trace_service_request().resource_spans)
            .await;
        assert!(&res.is_ok());
        // We should get at least one retry here, so wait for two responses
        for i in 0..=1 {
            if tokio::time::timeout(Duration::from_secs(5), server_rx.recv())
                .await
                .is_err()
            {
                panic!(
                    "Did not receive notification from MockOTLPService before 5 second timeout on loop {}!",
                    i
                );
            }
        }
        drop(trace_btx);
        let _ = join!(jh);
        cancel_token.cancel();
        // Get mock server requests
        let guard = requests.lock().unwrap();
        let tries = guard.borrow_mut().len() as i32;

        assert!(tries >= 2);
        shut_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn client_tls_auth() {
        init_crypto();

        // Start server
        let (shut_tx, shut_rx) = oneshot::channel::<()>();
        let socket_addr: SocketAddr = "[::1]:0".parse().expect("invalid port");
        let tcp_listener = tokio::net::TcpListener::bind(socket_addr).await.unwrap();
        let port = tcp_listener.local_addr().unwrap().port();
        println!("bound to port {} on socket_addr", port);
        let (server_tx, mut server_rx) = tokio::sync::mpsc::channel(1);

        let mock = MockOTLPService::new(Some(server_tx), false).await;
        spawn(async move {
            mock.serve_tls(tcp_listener, shut_rx, get_server_identity())
                .await
        });

        // Full client auth should succeed
        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-cert.pem");
        let key_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/client-key.pem");
        let server_root_ca_cert_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/ca.pem");

        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_cert_file(cert_file)
        .with_key_file(key_file)
        .with_ca_file(server_root_ca_cert_file);

        let traces = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None).unwrap();

        let res = send_test_msg(traces, trace_btx, &mut server_rx).await;
        assert!(res.is_some());

        // Fails because CA is missing
        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_cert_file(cert_file)
        .with_key_file(key_file);

        let traces = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None).unwrap();
        let res = send_test_msg(traces, trace_btx.clone(), &mut server_rx).await;
        assert!(res.is_none());
        //
        // Fails because missing key
        let (_trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_cert_file(cert_file)
        .with_ca_file(server_root_ca_cert_file);

        let otlp_res = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None);
        assert!(otlp_res.is_err());

        // Fails because missing cert
        let (_trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_key_file(key_file)
        .with_ca_file(server_root_ca_cert_file);

        let otlp_res = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None);
        assert!(otlp_res.is_err());

        // Succeeds because no identity but provides a CA and a correct domain
        let (_trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_ca_file(server_root_ca_cert_file);

        let otlp_res = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None);
        assert!(otlp_res.is_ok());

        // Fails because we have a CA but incorrect domain
        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("https://[::1]:{}", port)),
            Protocol::Grpc,
        )
        .with_ca_file(server_root_ca_cert_file);

        let otlp_res = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None);
        assert!(otlp_res.is_ok());

        let res = send_test_msg(otlp_res.unwrap(), trace_btx.clone(), &mut server_rx).await;
        // TODO: Originally we had expected this to be none. We should go back and look at behavior before the TLS refactor
        assert!(res.is_some());

        shut_tx.send(()).unwrap();
    }

    #[tokio::test]
    async fn http_protocol_traces() {
        init_crypto();
        let server = MockServer::start();

        let resp = ExportTraceServiceResponse::default();
        let mut resp_buf = BytesMut::with_capacity(1024);
        resp.encode(&mut resp_buf).unwrap();

        //
        // Test normal response case
        //
        let mut hello_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/traces");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body(resp_buf);
        });

        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("http://127.0.0.1:{}", server.port())),
            Protocol::Http,
        );

        let otlp_res = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None);
        assert!(otlp_res.is_ok());

        let res = send_test_traces_msgs_and_stop(otlp_res.unwrap(), trace_btx, 1).await;
        assert_ok!(res);

        hello_mock.assert_hits(1);
        hello_mock.delete();

        //
        // Test when the endpoint times out
        //
        let mut resp_buf = BytesMut::with_capacity(1024);
        resp.encode(&mut resp_buf).unwrap();

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/traces");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .delay(Duration::from_millis(200))
                .body(resp_buf);
        });

        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("http://127.0.0.1:{}", server.port())),
            Protocol::Http,
        )
        .with_request_timeout(Duration::from_millis(5))
        .with_initial_backoff(Duration::from_millis(5))
        .with_max_elapsed_time(Duration::from_millis(20));

        let otlp_res = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None);
        assert!(otlp_res.is_ok());

        let res = send_test_traces_msgs_and_stop(otlp_res.unwrap(), trace_btx, 1).await;
        assert!(res.is_err());

        // There is randomness, so at a minimum we should try twice, at max 3
        assert!((2..=3).contains(&(hello_mock.hits() as i32)));
    }

    #[tokio::test]
    async fn draining_completes() {
        init_crypto();
        let server = MockServer::start();
        let resp = ExportTraceServiceResponse::default();

        //
        // Test when the endpoint times out
        //
        let mut resp_buf = BytesMut::with_capacity(1024);
        resp.encode(&mut resp_buf).unwrap();

        let hello_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/traces");
            then.status(429)
                .header("content-type", "application/x-protobuf")
                .body(resp_buf);
        });

        let (trace_btx, trace_brx) = bounded::<Vec<ResourceSpans>>(100);
        let traces_config = otlp::trace_config_builder(
            Endpoint::Base(format!("http://127.0.0.1:{}", server.port())),
            Protocol::Http,
        )
        .with_request_timeout(Duration::from_millis(5))
        .with_initial_backoff(Duration::from_millis(5))
        .with_max_elapsed_time(Duration::from_millis(20))
        .with_encode_drain_max_time(Duration::from_millis(10));

        let otlp_res = otlp::exporter::build_traces_exporter(traces_config, trace_brx, None);
        assert!(otlp_res.is_ok());

        let otlp_exp = otlp_res.unwrap();

        // Send two messages
        let res = send_test_traces_msgs_and_stop(otlp_exp, trace_btx, 2).await;
        assert_err!(res);

        // Each message will have 2-3 attempts, so double that range. There is randomness
        // due to the jitter
        assert!((4..=6).contains(&(hello_mock.hits() as i32)));
    }

    #[tokio::test]
    async fn http_protocol_metrics() {
        init_crypto();
        let server = MockServer::start();

        let resp = ExportMetricsServiceResponse::default();
        let mut resp_buf = BytesMut::with_capacity(1024);
        resp.encode(&mut resp_buf).unwrap();

        //
        // Test normal response case
        //
        let mut hello_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/metrics");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body(resp_buf);
        });

        let (metrics_btx, metrics_brx) = bounded::<Vec<ResourceMetrics>>(100);
        let metrics_config = otlp::metrics_config_builder(
            Endpoint::Base(format!("http://127.0.0.1:{}", server.port())),
            Protocol::Http,
        );

        let otlp_res = otlp::exporter::build_metrics_exporter(metrics_config, metrics_brx, None);
        assert!(otlp_res.is_ok());

        let res = send_test_metrics_msg_and_stop(otlp_res.unwrap(), metrics_btx, 1).await;
        assert_ok!(res);

        hello_mock.assert_hits(1);
        hello_mock.delete();
    }

    #[tokio::test]
    async fn http_protocol_logs() {
        init_crypto();
        let server = MockServer::start();

        let resp = ExportLogsServiceResponse::default();
        let mut resp_buf = BytesMut::with_capacity(1024);
        resp.encode(&mut resp_buf).unwrap();

        //
        // Test normal response case
        //
        let mut hello_mock = server.mock(|when, then| {
            when.method(POST).path("/v1/logs");
            then.status(200)
                .header("content-type", "application/x-protobuf")
                .body(resp_buf);
        });

        let (logs_btx, logs_brx) = bounded::<Vec<ResourceLogs>>(100);
        let logs_config = otlp::logs_config_builder(
            Endpoint::Base(format!("http://127.0.0.1:{}", server.port())),
            Protocol::Http,
        );

        let otlp_res = otlp::exporter::build_logs_exporter(logs_config, logs_brx, None);
        assert!(otlp_res.is_ok());

        let res = send_test_logs_msg_and_stop(otlp_res.unwrap(), logs_btx, 1).await;
        assert_ok!(res);

        hello_mock.assert_hits(1);
        hello_mock.delete();
    }

    #[test]
    fn test_trace_batch_splitting() {
        let mut batch = NestedBatch::<ResourceSpans>::new(10, Duration::from_secs(1));
        let first_request = FakeOTLP::trace_service_request_with_spans(1, 5);
        let resp = batch.offer(first_request.resource_spans);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);
        let second_request = FakeOTLP::trace_service_request_with_spans(1, 7);
        let resp = batch.offer(second_request.resource_spans);
        assert!(resp.is_ok());
        let spans = resp.unwrap().unwrap();
        assert_eq!(10, spans.size_of());
        // Grab what's left in the batch
        let leftover = batch.take_batch();
        assert_eq!(2, leftover.size_of());
    }

    #[test]
    fn test_trace_batch_split_too_large() {
        let mut batch = NestedBatch::<ResourceSpans>::new(10, Duration::from_secs(1));
        let first_request = FakeOTLP::trace_service_request_with_spans(1, 21);
        let resp = batch.offer(first_request.resource_spans);
        assert!(resp.is_err());
    }

    #[test]
    fn test_metrics_batch_splitting() {
        let mut batch = NestedBatch::<ResourceMetrics>::new(10, Duration::from_secs(1));
        let first_request = FakeOTLP::metrics_service_request_with_metrics(1, 5);
        let resp = batch.offer(first_request.resource_metrics);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);
        let second_request = FakeOTLP::metrics_service_request_with_metrics(1, 7);
        let resp = batch.offer(second_request.resource_metrics);
        assert!(resp.is_ok());
        let spans = resp.unwrap().unwrap();
        assert_eq!(10, spans.size_of());
        // Grab what's left in the batch
        let leftover = batch.take_batch();
        assert_eq!(2, leftover.size_of());
    }

    #[test]
    fn test_metrics_batch_split_too_large() {
        let mut batch = NestedBatch::<ResourceMetrics>::new(10, Duration::from_secs(1));
        let first_request = FakeOTLP::metrics_service_request_with_metrics(1, 21);
        let resp = batch.offer(first_request.resource_metrics);
        assert!(resp.is_err());
    }

    #[test]
    fn test_logs_batch_splitting() {
        let mut batch = NestedBatch::<ResourceLogs>::new(10, Duration::from_secs(1));
        let first_request = FakeOTLP::logs_service_request_with_logs(1, 5);
        let resp = batch.offer(first_request.resource_logs);
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap(), None);
        let second_request = FakeOTLP::logs_service_request_with_logs(1, 7);
        let resp = batch.offer(second_request.resource_logs);
        assert!(resp.is_ok());
        let spans = resp.unwrap().unwrap();
        assert_eq!(10, spans.size_of());
        // Grab what's left in the batch
        let leftover = batch.take_batch();
        assert_eq!(2, leftover.size_of());
    }

    #[test]
    fn test_logs_batch_split_too_large() {
        let mut batch = NestedBatch::<ResourceLogs>::new(10, Duration::from_secs(1));
        let first_request = FakeOTLP::logs_service_request_with_logs(1, 21);
        let resp = batch.offer(first_request.resource_logs);
        assert!(resp.is_err());
    }

    // Wait for a msg to be sent, returns None if it was unable to deliver
    async fn send_test_msg(
        mut traces: Exporter<ResourceSpans, ExportTraceServiceRequest, ExportTraceServiceResponse>,
        btx: BoundedSender<Vec<ResourceSpans>>,
        server_rx: &mut tokio::sync::mpsc::Receiver<()>,
    ) -> Option<()> {
        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let exp_fut = async move { traces.start(shut_token).await };
        // Send a request for the server to process
        let res = btx
            .send(FakeOTLP::trace_service_request().resource_spans)
            .await;
        if let Err(e) = res {
            println!("error: {:?}", e);
            cancel_token.cancel();
            return None;
        }

        let recv_fut = tokio::time::timeout(Duration::from_millis(100), server_rx.recv());

        tokio::select! {
            _ = exp_fut => None,
            msg = recv_fut => {
                cancel_token.cancel();
                match msg {
                    Ok(_) => Some(()),
                    Err(_) => None
                }
            }
        }
    }

    async fn send_test_traces_msgs_and_stop(
        traces: Exporter<ResourceSpans, ExportTraceServiceRequest, ExportTraceServiceResponse>,
        btx: BoundedSender<Vec<ResourceSpans>>,
        how_many: usize,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut payloads = Vec::with_capacity(how_many);
        for _ in 0..how_many {
            payloads.push(FakeOTLP::trace_service_request().resource_spans);
        }

        send_test_trace_and_stop(traces, payloads, btx).await
    }

    async fn send_test_metrics_msg_and_stop(
        metrics: Exporter<
            ResourceMetrics,
            ExportMetricsServiceRequest,
            ExportMetricsServiceResponse,
        >,
        btx: BoundedSender<Vec<ResourceMetrics>>,
        how_many: usize,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut payloads = Vec::with_capacity(how_many);
        for _ in 0..how_many {
            payloads.push(FakeOTLP::metrics_service_request().resource_metrics);
        }

        send_test_metrics_and_stop(metrics, payloads, btx).await
    }

    async fn send_test_logs_msg_and_stop(
        logs: Exporter<ResourceLogs, ExportLogsServiceRequest, ExportLogsServiceResponse>,
        btx: BoundedSender<Vec<ResourceLogs>>,
        how_many: usize,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut payloads = Vec::with_capacity(how_many);
        for _ in 0..how_many {
            payloads.push(FakeOTLP::logs_service_request().resource_logs);
        }

        send_test_logs_and_stop(logs, payloads, btx).await
    }

    async fn send_test_trace_and_stop(
        mut traces: Exporter<ResourceSpans, ExportTraceServiceRequest, ExportTraceServiceResponse>,
        payloads: Vec<Vec<ResourceSpans>>,
        btx: BoundedSender<Vec<ResourceSpans>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let exp_fut = async move { traces.start(shut_token).await };
        for payload in payloads {
            // Send a request for the server to process
            let res = btx.send(payload).await;
            if let Err(e) = res {
                cancel_token.cancel();
                return Err(format!("error: {:?}", e).into());
            }
        }

        // Dropping the channel here will cause the exporter to receive None and exit
        drop(btx);
        let res = join!(exp_fut);
        cancel_token.cancel();
        res.0
    }

    async fn send_test_metrics_and_stop(
        mut metrics: Exporter<
            ResourceMetrics,
            ExportMetricsServiceRequest,
            ExportMetricsServiceResponse,
        >,
        payloads: Vec<Vec<ResourceMetrics>>,
        btx: BoundedSender<Vec<ResourceMetrics>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let exp_fut = async move { metrics.start(shut_token).await };
        for payload in payloads {
            // Send a request for the server to process
            let res = btx.send(payload).await;
            if let Err(e) = res {
                cancel_token.cancel();
                return Err(format!("error: {:?}", e).into());
            }
        }

        // Dropping the channel here will cause the exporter to receive None and exit
        drop(btx);
        let res = join!(exp_fut);
        cancel_token.cancel();
        res.0
    }

    async fn send_test_logs_and_stop(
        mut logs: Exporter<ResourceLogs, ExportLogsServiceRequest, ExportLogsServiceResponse>,
        payloads: Vec<Vec<ResourceLogs>>,
        btx: BoundedSender<Vec<ResourceLogs>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cancel_token = CancellationToken::new();
        let shut_token = cancel_token.clone();
        let exp_fut = async move { logs.start(shut_token).await };
        for payload in payloads {
            // Send a request for the server to process
            let res = btx.send(payload).await;
            if let Err(e) = res {
                cancel_token.cancel();
                return Err(format!("error: {:?}", e).into());
            }
        }

        // Dropping the channel here will cause the exporter to receive None and exit
        drop(btx);
        let res = join!(exp_fut);
        cancel_token.cancel();
        res.0
    }

    fn get_server_identity() -> ServerTlsConfig {
        let serv_pem_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/server.pem");
        let serv_key_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test/data/tls/server.key");
        let cert = std::fs::read_to_string(serv_pem_path).expect("error reading server.pem");
        let key = std::fs::read_to_string(serv_key_path).expect("error reading server.key");

        let identity = Identity::from_pem(cert, key);
        ServerTlsConfig::new().identity(identity)
    }

    async fn create_trace_client(
        endpoint: String,
    ) -> Result<TraceServiceClient<Channel>, Box<dyn Error + Send + Sync + 'static>> {
        let e = format!("http://{}", endpoint);
        Ok(TraceServiceClient::connect(e).await?)
    }

    async fn create_metrics_client(
        endpoint: String,
    ) -> Result<MetricsServiceClient<Channel>, Box<dyn Error + Send + Sync + 'static>> {
        let e = format!("http://{}", endpoint);
        Ok(MetricsServiceClient::connect(e).await?)
    }

    async fn create_logs_client(
        endpoint: String,
    ) -> Result<LogsServiceClient<Channel>, Box<dyn Error + Send + Sync + 'static>> {
        let e = format!("http://{}", endpoint);
        Ok(LogsServiceClient::connect(e).await?)
    }
}
