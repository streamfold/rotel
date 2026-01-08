// SPDX-License-Identifier: Apache-2.0

use crate::listener::Listener;
use crate::receivers::get_meter;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::batch::BatchSizer;
use crate::topology::payload::{Message, RequestContext};
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
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
    ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService,
};
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use std::collections::HashMap;
use std::default::Default;
use std::error::Error;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::Router;
use tonic::{Request, Response, Status, codec::CompressionEncoding};

#[derive(Default)]
pub struct OTLPGrpcServerBuilder {
    max_recv_msg_size_mib: Option<usize>,
    traces_output: Option<OTLPOutput<Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Message<ResourceLogs>>>,
    include_metadata: bool,
    headers_to_include: Vec<String>,
}

impl OTLPGrpcServerBuilder {}

impl OTLPGrpcServerBuilder {
    pub fn with_max_recv_msg_size_mib(mut self, max_recv_msg_size_mib: usize) -> Self {
        self.max_recv_msg_size_mib = Some(max_recv_msg_size_mib);
        self
    }

    pub fn with_traces_output(
        mut self,
        output: Option<OTLPOutput<Message<ResourceSpans>>>,
    ) -> Self {
        self.traces_output = output;
        self
    }

    pub fn with_metrics_output(
        mut self,
        output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    ) -> Self {
        self.metrics_output = output;
        self
    }

    pub fn with_logs_output(mut self, output: Option<OTLPOutput<Message<ResourceLogs>>>) -> Self {
        self.logs_output = output;
        self
    }

    pub fn with_include_metadata(mut self, include: bool) -> Self {
        self.include_metadata = include;
        self
    }

    pub fn with_headers_to_include(mut self, headers: Vec<String>) -> Self {
        self.headers_to_include = headers;
        self
    }

    pub fn build(self) -> OTLPGrpcServer {
        OTLPGrpcServer {
            traces_output: self.traces_output,
            metrics_output: self.metrics_output,
            logs_output: self.logs_output,
            max_recv_msg_size_mib: self.max_recv_msg_size_mib,
            include_metadata: self.include_metadata,
            headers_to_include: self.headers_to_include,
        }
    }
}

pub struct OTLPGrpcServer {
    traces_output: Option<OTLPOutput<Message<ResourceSpans>>>,
    metrics_output: Option<OTLPOutput<Message<ResourceMetrics>>>,
    logs_output: Option<OTLPOutput<Message<ResourceLogs>>>,
    max_recv_msg_size_mib: Option<usize>,
    include_metadata: bool,
    headers_to_include: Vec<String>,
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
            self.include_metadata,
            self.headers_to_include.clone(),
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
    traces_tx: Option<OTLPOutput<Message<ResourceSpans>>>,
    metrics_tx: Option<OTLPOutput<Message<ResourceMetrics>>>,
    logs_tx: Option<OTLPOutput<Message<ResourceLogs>>>,
    accepted_spans_records_counter: Counter<u64>,
    accepted_metric_points_counter: Counter<u64>,
    accepted_log_records_counter: Counter<u64>,
    refused_spans_records_counter: Counter<u64>,
    refused_metric_points_counter: Counter<u64>,
    refused_log_records_counter: Counter<u64>,
    tags: [KeyValue; 1],
    include_metadata: bool,
    headers_to_include: Vec<String>,
}

impl CollectorService {
    fn new(
        traces_tx: Option<OTLPOutput<Message<ResourceSpans>>>,
        metrics_tx: Option<OTLPOutput<Message<ResourceMetrics>>>,
        logs_tx: Option<OTLPOutput<Message<ResourceLogs>>>,
        include_metadata: bool,
        headers_to_include: Vec<String>,
    ) -> Self {
        Self {
            traces_tx,
            metrics_tx,
            logs_tx,
            accepted_spans_records_counter: get_meter()
                .u64_counter("rotel_receiver_accepted_spans")
                .with_description(
                    "Number of spans successfully ingested and pushed into the pipeline",
                )
                .with_unit("spans")
                .build(),
            accepted_metric_points_counter: get_meter()
                .u64_counter("rotel_receiver_accepted_metric_points")
                .with_description(
                    "Number of metric points successfully ingested and pushed into the pipeline.",
                )
                .with_unit("metric_points")
                .build(),
            accepted_log_records_counter: get_meter()
                .u64_counter("rotel_receiver_accepted_log_records")
                .with_description(
                    "Number of metric points successfully ingested and pushed into the pipeline.",
                )
                .with_unit("log_records")
                .build(),
            refused_spans_records_counter: get_meter()
                .u64_counter("rotel_receiver_refused_spans")
                .with_description("Number of spans that could not be pushed into the pipeline.")
                .with_unit("spans")
                .build(),
            refused_metric_points_counter: get_meter()
                .u64_counter("rotel_receiver_refused_metric_points")
                .with_description(
                    "Number of metric points that could not be pushed into the pipeline.",
                )
                .with_unit("metric_points")
                .build(),
            refused_log_records_counter: get_meter()
                .u64_counter("rotel_receiver_refused_log_records")
                .with_description("Number of logs that could not be pushed into the pipeline.")
                .with_unit("log_records")
                .build(),
            tags: [KeyValue::new("protocol", "grpc")],
            include_metadata,
            headers_to_include,
        }
    }

    fn extract_context_from_request<T>(&self, request: &Request<T>) -> Option<RequestContext> {
        if !self.include_metadata || self.headers_to_include.is_empty() {
            return None;
        }
        let mut metadata_map = HashMap::new();
        let request_metadata = request.metadata();
        for key in &self.headers_to_include {
            let normalized = key.to_lowercase();
            if let Some(value) = request_metadata.get(&normalized) {
                if let Ok(value_str) = value.to_str() {
                    metadata_map.insert(normalized, value_str.to_string());
                }
            }
        }
        if !metadata_map.is_empty() {
            Some(RequestContext::Grpc(metadata_map))
        } else {
            None
        }
    }
}

#[tonic::async_trait]
impl TraceService for CollectorService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req_context = self.extract_context_from_request(&request);
        let trace_request = request.into_inner();
        match &self.traces_tx {
            None => Err(Status::unavailable("OTLP trace receiver is disabled")),
            Some(traces_tx) => {
                let count = BatchSizer::size_of(trace_request.resource_spans.as_slice()) as u64;
                match traces_tx
                    .send(Message {
                        metadata: None,
                        payload: trace_request.resource_spans,
                        request_context: req_context,
                    })
                    .await
                {
                    Ok(_) => {
                        self.accepted_spans_records_counter.add(count, &self.tags);
                        Ok(Response::new(ExportTraceServiceResponse {
                            partial_success: None,
                        }))
                    }
                    Err(_) => {
                        self.refused_spans_records_counter.add(count, &self.tags);
                        Err(Status::unavailable("channel was disconnected"))
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl MetricsService for CollectorService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req_context = self.extract_context_from_request(&request);
        let metrics_request = request.into_inner();
        match &self.metrics_tx {
            None => Err(Status::unavailable("OTLP metrics receiver is disabled")),
            Some(metrics_tx) => {
                let count = BatchSizer::size_of(metrics_request.resource_metrics.as_slice()) as u64;
                match metrics_tx
                    .send(Message {
                        metadata: None,
                        request_context: req_context,
                        payload: metrics_request.resource_metrics,
                    })
                    .await
                {
                    Ok(_) => {
                        self.accepted_metric_points_counter.add(count, &self.tags);
                        Ok(Response::new(ExportMetricsServiceResponse {
                            partial_success: None,
                        }))
                    }
                    Err(_) => {
                        self.refused_metric_points_counter.add(count, &self.tags);
                        Err(Status::unavailable("channel was disconnected"))
                    }
                }
            }
        }
    }
}

#[tonic::async_trait]
impl LogsService for CollectorService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req_context = self.extract_context_from_request(&request);
        let logs_request = request.into_inner();
        match &self.logs_tx {
            None => Err(Status::unavailable("OTLP logs receiver is disabled")),
            Some(logs_tx) => {
                let count = BatchSizer::size_of(logs_request.resource_logs.as_slice()) as u64;
                match logs_tx
                    .send(Message {
                        metadata: None,
                        payload: logs_request.resource_logs,
                        request_context: req_context,
                    })
                    .await
                {
                    Ok(_) => {
                        self.accepted_log_records_counter.add(count, &self.tags);
                        Ok(Response::new(ExportLogsServiceResponse {
                            partial_success: None,
                        }))
                    }
                    Err(_) => {
                        self.refused_log_records_counter.add(count, &self.tags);
                        Err(Status::unavailable("channel was disconnected"))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bounded_channel::bounded;
    use crate::listener::Listener;
    use crate::receivers::otlp::otlp_grpc::OTLPGrpcServer;
    use crate::receivers::otlp_output::OTLPOutput;
    use crate::topology::payload::{Message, RequestContext};
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
    use tonic::metadata::{MetadataKey, MetadataValue};
    use tonic::{Response, Status};
    use utilities::otlp::FakeOTLP;

    #[tokio::test]
    async fn max_trace_size() {
        let (trace_in_tx, _traces_in_rx) = bounded::<Message<ResourceSpans>>(10);
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
        let (metrics_in_tx, _metrics_in_rx) = bounded::<Message<ResourceMetrics>>(10);
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
        let (logs_in_tx, _logs_in_rx) = bounded::<Message<ResourceLogs>>(10);
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

    async fn send_trace_msg_with_metadata(
        addr: SocketAddr,
        msg: ExportTraceServiceRequest,
        metadata: Vec<(&str, &str)>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let addr = format!("http://{}", addr);
        let mut client = TraceServiceClient::connect(addr).await.unwrap();

        let mut request = tonic::Request::new(msg);
        for (key, value) in metadata {
            // Use from_bytes for non-static strings
            if let Ok(metadata_key) =
                MetadataKey::<tonic::metadata::Ascii>::from_bytes(key.as_bytes())
            {
                if let Ok(metadata_value) = MetadataValue::<tonic::metadata::Ascii>::try_from(value)
                {
                    request.metadata_mut().insert(metadata_key, metadata_value);
                }
            }
        }

        client.export(request).await
    }

    async fn send_metrics_msg_with_metadata(
        addr: SocketAddr,
        msg: ExportMetricsServiceRequest,
        metadata: Vec<(&str, &str)>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let addr = format!("http://{}", addr);
        let mut client = MetricsServiceClient::connect(addr).await.unwrap();

        let mut request = tonic::Request::new(msg);
        for (key, value) in metadata {
            // Use from_bytes for non-static strings
            if let Ok(metadata_key) =
                MetadataKey::<tonic::metadata::Ascii>::from_bytes(key.as_bytes())
            {
                if let Ok(metadata_value) = MetadataValue::<tonic::metadata::Ascii>::try_from(value)
                {
                    request.metadata_mut().insert(metadata_key, metadata_value);
                }
            }
        }

        client.export(request).await
    }

    async fn send_logs_msg_with_metadata(
        addr: SocketAddr,
        msg: ExportLogsServiceRequest,
        metadata: Vec<(&str, &str)>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let addr = format!("http://{}", addr);
        let mut client = LogsServiceClient::connect(addr).await.unwrap();

        let mut request = tonic::Request::new(msg);
        for (key, value) in metadata {
            // Use from_bytes for non-static strings
            if let Ok(metadata_key) =
                MetadataKey::<tonic::metadata::Ascii>::from_bytes(key.as_bytes())
            {
                if let Ok(metadata_value) = MetadataValue::<tonic::metadata::Ascii>::try_from(value)
                {
                    request.metadata_mut().insert(metadata_key, metadata_value);
                }
            }
        }

        client.export(request).await
    }

    #[tokio::test]
    async fn grpc_metadata_extracted_for_traces() {
        let example_headers = FakeOTLP::example_headers();
        let header_names: Vec<String> = example_headers.keys().cloned().collect();

        let (trace_in_tx, mut trace_rx) = bounded::<Message<ResourceSpans>>(10);
        let trace_output = OTLPOutput::new(trace_in_tx);
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_traces_output(Some(trace_output))
            .with_include_metadata(true)
            .with_headers_to_include(header_names.clone())
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::trace_service_request_with_spans(1, 1);
        // Convert example headers to Vec of (&str, &str) for the helper function
        let metadata_vec: Vec<(&str, &str)> = example_headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let send_fut = send_trace_msg_with_metadata(addr, req, metadata_vec);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present
        assert!(msg.request_context.is_some());
        let request_context = msg.request_context.as_ref().unwrap();

        // Verify all example headers are present
        for (key, expected_value) in &example_headers {
            match request_context {
                RequestContext::Http(h) => {
                    panic!("expected grpc request headers, got http {:?}", h)
                }
                RequestContext::Grpc(h) => {
                    assert_eq!(
                        h.get(key),
                        Some(expected_value),
                        "Metadata key {} should be present with value {}",
                        key,
                        expected_value
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn grpc_metadata_not_extracted_when_disabled() {
        let example_headers = FakeOTLP::example_headers();
        let header_names: Vec<String> = example_headers.keys().cloned().collect();
        let first_header = header_names.first().unwrap();

        let (trace_in_tx, mut trace_rx) = bounded::<Message<ResourceSpans>>(10);
        let trace_output = OTLPOutput::new(trace_in_tx);
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_traces_output(Some(trace_output))
            .with_include_metadata(false)
            .with_headers_to_include(vec![first_header.clone()])
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let send_fut = send_trace_msg_with_metadata(
            addr,
            req,
            vec![(
                first_header.as_str(),
                example_headers.get(first_header).unwrap().as_str(),
            )],
        );

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is NOT present when disabled
        assert!(msg.metadata.is_none());
    }

    #[tokio::test]
    async fn grpc_metadata_not_extracted_when_no_keys_specified() {
        let example_headers = FakeOTLP::example_headers();
        let first_header = example_headers.keys().next().unwrap();

        let (trace_in_tx, mut trace_rx) = bounded::<Message<ResourceSpans>>(10);
        let trace_output = OTLPOutput::new(trace_in_tx);
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_traces_output(Some(trace_output))
            .with_include_metadata(true)
            .with_headers_to_include(vec![])
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::trace_service_request_with_spans(1, 1);
        let send_fut = send_trace_msg_with_metadata(
            addr,
            req,
            vec![(
                first_header.as_str(),
                example_headers.get(first_header).unwrap().as_str(),
            )],
        );

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is NOT present when no keys specified
        assert!(msg.metadata.is_none());
    }

    #[tokio::test]
    async fn grpc_metadata_extracted_for_metrics() {
        let (metrics_in_tx, mut metrics_rx) = bounded::<Message<ResourceMetrics>>(10);
        let metrics_output = OTLPOutput::new(metrics_in_tx);
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_metrics_output(Some(metrics_output))
            .with_include_metadata(true)
            .with_headers_to_include(vec!["my-custom-header".to_string()])
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::metrics_service_request_with_metrics(1, 1);
        let send_fut =
            send_metrics_msg_with_metadata(addr, req, vec![("my-custom-header", "metrics-value")]);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        let msg = metrics_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present
        assert!(msg.request_context.is_some());
        let req_context = msg.request_context.as_ref().unwrap();
        match req_context {
            RequestContext::Http(h) => {
                panic!("expected grpc request headers, got http {:?}", h)
            }
            RequestContext::Grpc(h) => {
                assert_eq!(
                    h.get("my-custom-header"),
                    Some(&"metrics-value".to_string())
                );
            }
        }
    }

    #[tokio::test]
    async fn grpc_metadata_extracted_for_logs() {
        let (logs_in_tx, mut logs_rx) = bounded::<Message<ResourceLogs>>(10);
        let logs_output = OTLPOutput::new(logs_in_tx);
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_logs_output(Some(logs_output))
            .with_include_metadata(true)
            .with_headers_to_include(vec!["my-custom-header".to_string()])
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::logs_service_request_with_logs(1, 1);
        let send_fut =
            send_logs_msg_with_metadata(addr, req, vec![("my-custom-header", "logs-value")]);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        let msg = logs_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present
        assert!(msg.request_context.is_some());
        let req_context = msg.request_context.as_ref().unwrap();
        match req_context {
            RequestContext::Http(h) => {
                panic!("expected grpc request headers, got http {:?}", h)
            }
            RequestContext::Grpc(h) => {
                assert_eq!(h.get("my-custom-header"), Some(&"logs-value".to_string()));
            }
        }
    }

    #[tokio::test]
    async fn grpc_metadata_key_case_insensitive() {
        let (trace_in_tx, mut trace_rx) = bounded::<Message<ResourceSpans>>(10);
        let trace_output = OTLPOutput::new(trace_in_tx);
        let cancel = CancellationToken::new();

        let listener = Listener::listen_async("[::1]:0".parse().unwrap())
            .await
            .unwrap();

        let srv = OTLPGrpcServer::builder()
            .with_traces_output(Some(trace_output))
            .with_include_metadata(true)
            .with_headers_to_include(vec!["My-Custom-Header".to_string()])
            .build();
        let addr = listener.bound_address().unwrap();
        let cancel_token = cancel.clone();
        let srv_fut = async move { srv.serve(listener, cancel_token).await };
        tokio::pin!(srv_fut);

        let req = FakeOTLP::trace_service_request_with_spans(1, 1);
        // Send metadata with lowercase key
        let send_fut =
            send_trace_msg_with_metadata(addr, req, vec![("my-custom-header", "test-value")]);

        tokio::select! {
            _ = &mut srv_fut => {},
            msg = send_fut => {
                assert_ok!(msg);
                cancel.cancel();
            }
        }
        srv_fut.await.unwrap();

        let msg = trace_rx.next().await.unwrap();
        assert_eq!(1, msg.len());

        // Verify metadata is present and key is normalized to lowercase
        assert!(msg.request_context.is_some());
        let req_context = msg.request_context.as_ref().unwrap();
        match req_context {
            RequestContext::Http(h) => {
                panic!("expected grpc request headers, got http {:?}", h)
            }
            RequestContext::Grpc(h) => {
                // Should be stored as lowercase
                assert_eq!(h.get("my-custom-header"), Some(&"test-value".to_string()));
            }
        }
    }
}
