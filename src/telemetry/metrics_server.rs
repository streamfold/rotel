// SPDX-License-Identifier: Apache-2.0

use crate::listener::Listener;

use http::Method;
use http_body_util::Full;
use hyper::body::Body;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use opentelemetry_prometheus_text_exporter::PrometheusExporter;

use std::error::Error as StdError;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::sync::CancellationToken;
use tower::Service;
use tracing::error;

/// MetricsServer provides an HTTP endpoint for exposing internal metrics
pub struct MetricsServer {
    addr: SocketAddr,
    exporter: PrometheusExporter,
}

impl MetricsServer {
    /// Creates a new MetricsServer that will listen on localhost:9090
    pub fn new(exporter: PrometheusExporter) -> Self {
        Self {
            addr: "127.0.0.1:9090".parse().unwrap(),
            exporter,
        }
    }

    /// Creates a new MetricsServer with a custom address
    pub fn with_addr(addr: SocketAddr, exporter: PrometheusExporter) -> Self {
        Self { addr, exporter }
    }

    /// Starts the metrics server and serves requests until cancelled
    pub async fn serve(
        &self,
        listener: Listener,
        cancellation: CancellationToken,
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
        let svc = MetricsService::new(self.exporter.clone());

        // To bridge Tower->Hyper we must wrap the tower service
        let svc = TowerToHyperService::new(svc);

        let timer = hyper_util::rt::TokioTimer::new();
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();

        let mut builder = Builder::new(TokioExecutor::new());
        builder.http1().timer(timer.clone());
        builder.http2().timer(timer);

        let listener = listener.into_async()?;
        // We start a loop to continuously accept incoming connections
        loop {
            let stream = tokio::select! {
                r = listener.accept() => {
                    match r {
                        Ok((stream, _)) => stream,
                        Err(e) => return Err(e.into()),
                    }
                },
                _ = cancellation.cancelled() => break
            };

            let io = TokioIo::new(stream);

            let conn = builder.serve_connection(io, svc.clone());
            let fut = graceful.watch(conn.into_owned());

            tokio::spawn(async move {
                let _ = fut.await.map_err(|e| {
                    error!("error serving metrics connection: {:?}", e);
                });
            });
        }

        // gracefully shutdown existing connections
        graceful.shutdown().await;

        Ok(())
    }

    /// Returns the configured address for this server
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Service implementation for handling metrics requests
#[derive(Clone)]
struct MetricsService {
    exporter: PrometheusExporter,
}

impl MetricsService {
    fn new(exporter: PrometheusExporter) -> Self {
        Self { exporter }
    }
}

impl<H> Service<Request<H>> for MetricsService
where
    H: Body + Send + Sync + 'static,
    <H as Body>::Data: Send + Sync + Clone,
    <H as Body>::Error: Display + Debug + Send + Sync + ToString,
{
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<H>) -> Self::Future {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/metrics") => {
                let mut output = Vec::new();
                match self.exporter.export(&mut output) {
                    Ok(_) => {
                        let response = Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "text/plain; charset=utf-8")
                            .body(Full::new(Bytes::from(output)))
                            .unwrap();
                        Box::pin(futures::future::ok(response))
                    }
                    Err(err) => {
                        let response = Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Full::new(Bytes::from(format!("Failed to export: {}", err))))
                            .unwrap();
                        Box::pin(futures::future::ok(response))
                    }
                }
            }
            // Return 404 Not Found for other routes
            _ => {
                let response = Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from_static(b"Not Found")))
                    .unwrap();
                Box::pin(futures::future::ok(response))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use hyper_util::client::legacy::Client;
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::rt::TokioTimer;
    use opentelemetry::global;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_metrics_endpoint() {
        // Create a PrometheusExporter for testing
        let exporter = PrometheusExporter::default();

        let provider = SdkMeterProvider::builder()
            .with_reader(exporter.clone())
            .build();
        global::set_meter_provider(provider);

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = Listener::listen_async(addr).await.unwrap();
        let bound_addr = listener.bound_address().unwrap();

        let server = MetricsServer::with_addr(bound_addr, exporter);
        let cancellation = CancellationToken::new();
        let cancel_handle = cancellation.clone();

        let server_handle = tokio::spawn(async move { server.serve(listener, cancellation).await });

        // Give the server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test GET /metrics
        let client: Client<HttpConnector, Full<Bytes>> =
            hyper_util::client::legacy::Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(2))
                .pool_max_idle_per_host(2)
                .timer(TokioTimer::new())
                .build::<_, Full<Bytes>>(HttpConnector::new());
        let uri = format!("http://{}/metrics", bound_addr);
        let response = timeout(Duration::from_secs(5), client.get(uri.parse().unwrap()))
            .await
            .expect("Request timed out")
            .expect("Request failed");

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("Failed to read body")
            .to_bytes();

        // Should return Prometheus formatted metrics (may be empty if no metrics recorded)
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        // The response should be valid text (empty or containing prometheus format)
        assert!(body_str.is_empty() || body_str.contains("# HELP") || body_str.contains("# TYPE"));

        // Test 404 on other paths
        let uri = format!("http://{}/invalid", bound_addr);
        let response = timeout(Duration::from_secs(5), client.get(uri.parse().unwrap()))
            .await
            .expect("Request timed out")
            .expect("Request failed");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // Cancel the server
        cancel_handle.cancel();
        timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server shutdown timed out")
            .expect("Server task failed")
            .expect("Server returned error");
    }

    #[tokio::test]
    async fn test_metrics_endpoint_with_actual_metrics() {
        // Create a PrometheusExporter for testing
        let exporter = PrometheusExporter::default();

        let provider = SdkMeterProvider::builder()
            .with_reader(exporter.clone())
            .build();

        // Create and record some test metrics using the provider directly
        let meter = provider.meter("test_meter");
        let counter = meter.u64_counter("test_counter").build();
        counter.add(42, &[]);

        let gauge = meter.i64_gauge("test_gauge").build();
        gauge.record(100, &[]);

        // Give metrics time to be collected
        tokio::time::sleep(Duration::from_millis(50)).await;

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = Listener::listen_async(addr).await.unwrap();
        let bound_addr = listener.bound_address().unwrap();

        let server = MetricsServer::with_addr(bound_addr, exporter);
        let cancellation = CancellationToken::new();
        let cancel_handle = cancellation.clone();

        let server_handle = tokio::spawn(async move { server.serve(listener, cancellation).await });

        // Give the server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test GET /metrics
        let client: Client<HttpConnector, Full<Bytes>> =
            hyper_util::client::legacy::Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(2))
                .pool_max_idle_per_host(2)
                .timer(TokioTimer::new())
                .build::<_, Full<Bytes>>(HttpConnector::new());
        let uri = format!("http://{}/metrics", bound_addr);
        let response = timeout(Duration::from_secs(5), client.get(uri.parse().unwrap()))
            .await
            .expect("Request timed out")
            .expect("Request failed");

        assert_eq!(response.status(), StatusCode::OK);
        let body_bytes = response
            .into_body()
            .collect()
            .await
            .expect("Failed to read body")
            .to_bytes();

        // Should return Prometheus formatted metrics with our test metrics
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body_str.contains("test_counter"));
        assert!(body_str.contains("test_gauge"));

        // Cancel the server
        cancel_handle.cancel();
        timeout(Duration::from_secs(5), server_handle)
            .await
            .expect("Server shutdown timed out")
            .expect("Server task failed")
            .expect("Server returned error");
    }
}
