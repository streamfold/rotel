// SPDX-License-Identifier: Apache-2.0

use crate::listener::Listener;
use crate::receivers::otlp_output::OTLPOutput;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::default::Default;
use std::error::Error;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::Router;
use tonic::transport::Server;
use tonic::{codec::CompressionEncoding, Request, Response, Status};

#[derive(Default)]
pub struct OTLPGrpcServerBuilder {
    max_recv_msg_size_mib: Option<usize>,
    traces_output: Option<OTLPOutput<Vec<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Vec<ResourceLogs>>>,
}

impl OTLPGrpcServerBuilder {}

impl OTLPGrpcServerBuilder {
    pub fn with_max_recv_msg_size_mib(mut self, max_recv_msg_size_mib: usize) -> Self {
        self.max_recv_msg_size_mib = Some(max_recv_msg_size_mib);
        self
    }

    pub fn with_traces_output(mut self, output: Option<OTLPOutput<Vec<ResourceSpans>>>) -> Self {
        self.traces_output = output;
        self
    }

    pub fn with_metrics_output(mut self, output: Option<OTLPOutput<Vec<ResourceMetrics>>>) -> Self {
        self.metrics_output = output;
        self
    }

    pub fn with_logs_output(mut self, output: Option<OTLPOutput<Vec<ResourceLogs>>>) -> Self {
        self.logs_output = output;
        self
    }

    pub fn build(self) -> OTLPGrpcServer {
        OTLPGrpcServer {
            traces_output: self.traces_output,
            metrics_output: self.metrics_output,
            logs_output: self.logs_output,
            max_recv_msg_size_mib: self.max_recv_msg_size_mib,
        }
    }
}

pub struct OTLPGrpcServer {
    traces_output: Option<OTLPOutput<Vec<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Vec<ResourceLogs>>>,
    max_recv_msg_size_mib: Option<usize>,
}

impl OTLPGrpcServer {
    pub fn builder() -> OTLPGrpcServerBuilder {
        Default::default()
    }

    pub async fn serve(
        &self,
        listener: Listener,
        cancellation: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let svc = CollectorService::new(
            self.traces_output.clone(),
            self.metrics_output.clone(),
            self.logs_output.clone(),
        );
        let stream = listener.into_stream()?;

        let mut builder = Server::builder();
        let mut router: Option<Router> = None;
        if self.traces_output.is_some() {
            let mut trace_svc = TraceServiceServer::new(svc.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);
            if let Some(max_recv_msg_size_mib) = self.max_recv_msg_size_mib {
                trace_svc =
                    trace_svc.max_decoding_message_size(max_recv_msg_size_mib * 1024 * 1024);
            }
            router = Some(builder.add_service(trace_svc));
        }

        if self.metrics_output.is_some() {
            let mut metric_svc = MetricsServiceServer::new(svc.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);
            if let Some(max_recv_msg_size_mib) = self.max_recv_msg_size_mib {
                metric_svc =
                    metric_svc.max_decoding_message_size(max_recv_msg_size_mib * 1024 * 1024);
            }
            if router.is_some() {
                router = Some(router.unwrap().add_service(metric_svc));
            } else {
                router = Some(builder.add_service(metric_svc))
            }
        }

        if self.logs_output.is_some() {
            let mut logs_svc = LogsServiceServer::new(svc.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);
            if let Some(max_recv_msg_size_mib) = self.max_recv_msg_size_mib {
                logs_svc = logs_svc.max_decoding_message_size(max_recv_msg_size_mib * 1024 * 1024);
            }
            if router.is_some() {
                router = Some(router.unwrap().add_service(logs_svc));
            } else {
                router = Some(builder.add_service(logs_svc))
            }
        }

        match router {
            None => Err("No Service Servers configured for OTLP metrics receiver".into()),
            Some(_) => {
                router
                    .unwrap()
                    .serve_with_incoming_shutdown(
                        stream,
                        async move { cancellation.cancelled().await },
                    )
                    .await?;
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
struct CollectorService {
    traces_tx: Option<OTLPOutput<Vec<ResourceSpans>>>,
    metrics_tx: Option<OTLPOutput<Vec<ResourceMetrics>>>,
    logs_tx: Option<OTLPOutput<Vec<ResourceLogs>>>,
}

impl CollectorService {
    fn new(
        traces_tx: Option<OTLPOutput<Vec<ResourceSpans>>>,
        metrics_tx: Option<OTLPOutput<Vec<ResourceMetrics>>>,
        logs_tx: Option<OTLPOutput<Vec<ResourceLogs>>>,
    ) -> Self {
        Self {
            traces_tx,
            metrics_tx,
            logs_tx,
        }
    }
}

#[tonic::async_trait]
impl TraceService for CollectorService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let trace_request = request.into_inner();
        match &self.traces_tx {
            None => Err(Status::unavailable("OTLP trace receiver is disabled")),
            Some(traces_tx) => match traces_tx.send(trace_request.resource_spans).await {
                Ok(_) => Ok(Response::new(ExportTraceServiceResponse {
                    partial_success: None,
                })),
                Err(_) => Err(Status::unavailable("channel was disconnected")),
            },
        }
    }
}

#[tonic::async_trait]
impl MetricsService for CollectorService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let metrics_request = request.into_inner();
        match &self.metrics_tx {
            None => Err(Status::unavailable("OTLP metrics receiver is disabled")),
            Some(metrics_tx) => match metrics_tx.send(metrics_request.resource_metrics).await {
                Ok(_) => Ok(Response::new(ExportMetricsServiceResponse {
                    partial_success: None,
                })),
                Err(_) => Err(Status::unavailable("channel was disconnected")),
            },
        }
    }
}

#[tonic::async_trait]
impl LogsService for CollectorService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let logs_request = request.into_inner();
        match &self.logs_tx {
            None => Err(Status::unavailable("OTLP logs receiver is disabled")),
            Some(logs_tx) => match logs_tx.send(logs_request.resource_logs).await {
                Ok(_) => Ok(Response::new(ExportLogsServiceResponse {
                    partial_success: None,
                })),
                Err(_) => Err(Status::unavailable("channel was disconnected")),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bounded_channel::bounded;
    use crate::listener::Listener;
    use crate::receivers::otlp_grpc::OTLPGrpcServer;
    use crate::receivers::otlp_output::OTLPOutput;
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
    use opentelemetry_proto::tonic::collector::logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    };
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
    use opentelemetry_proto::tonic::collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    };
    use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
    use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use std::net::SocketAddr;
    use tokio_test::{assert_err, assert_ok};
    use tokio_util::sync::CancellationToken;
    use tonic::{Response, Status};
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn max_trace_size() {
        let (trace_in_tx, _traces_in_rx) = bounded::<Vec<ResourceSpans>>(10);
        let trace_output = OTLPOutput::new(trace_in_tx);
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        // defaults should allow our message through
        let srv = OTLPGrpcServer::builder()
            .with_traces_output(Some(trace_output.clone()))
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::trace_service_request_with_spans(2000, 1);
        let send_fut = send_trace_msg(addr, req);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        // set to invalid minimum size

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_max_recv_msg_size_mib(1)
            .with_traces_output(Some(trace_output.clone()))
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel = CancellationToken::new();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::trace_service_request_with_spans(2000, 1);
        let send_fut = send_trace_msg(addr, req);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_err!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();
    }

    #[tokio::test]
    async fn max_metrics_size() {
        let (metrics_in_tx, _metrics_in_rx) = bounded::<Vec<ResourceMetrics>>(10);
        let metrics_output = OTLPOutput::new(metrics_in_tx.clone());
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        // defaults should allow our message through
        let srv = OTLPGrpcServer::builder()
            .with_metrics_output(Some(metrics_output.clone()))
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::metrics_service_request_with_metrics(2000, 1);
        let send_fut = send_metrics_msg(addr, req);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        // set to invalid minimum size

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_max_recv_msg_size_mib(1)
            .with_metrics_output(Some(metrics_output.clone()))
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel = CancellationToken::new();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::metrics_service_request_with_metrics(2000, 2);
        let send_fut = send_metrics_msg(addr, req);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_err!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();
    }

    #[tokio::test]
    async fn max_log_size() {
        let (logs_in_tx, _logs_in_rx) = bounded::<Vec<ResourceLogs>>(10);
        let logs_output = OTLPOutput::new(logs_in_tx.clone());
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        // defaults should allow our message through
        let srv = OTLPGrpcServer::builder()
            .with_logs_output(Some(logs_output.clone()))
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::logs_service_request_with_logs(2000, 1);
        let send_fut = send_logs_msg(addr, req);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        // set to invalid minimum size

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_max_recv_msg_size_mib(1)
            .with_logs_output(Some(logs_output.clone()))
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel = CancellationToken::new();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::logs_service_request_with_logs(2000, 10);
        let send_fut = send_logs_msg(addr, req);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_err!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();
    }

    async fn send_trace_msg(
        addr: SocketAddr,
        msg: ExportTraceServiceRequest,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let addr = format!("http://{}", addr);
        let mut client = TraceServiceClient::connect(addr).await.unwrap();

        client.export(msg).await
    }

    async fn send_metrics_msg(
        addr: SocketAddr,
        msg: ExportMetricsServiceRequest,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let addr = format!("http://{}", addr);
        let mut client = MetricsServiceClient::connect(addr).await.unwrap();

        client.export(msg).await
    }

    async fn send_logs_msg(
        addr: SocketAddr,
        msg: ExportLogsServiceRequest,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let addr = format!("http://{}", addr);
        let mut client = LogsServiceClient::connect(addr).await.unwrap();

        client.export(msg).await
    }
}
