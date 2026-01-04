pub mod otlp_grpc;
pub mod otlp_http;

use std::net::SocketAddr;

#[derive(Debug)]
pub struct OTLPReceiverConfig {
    pub otlp_grpc_endpoint: SocketAddr,
    pub otlp_http_endpoint: SocketAddr,
    pub otlp_grpc_max_recv_msg_size_mib: u64,
    pub otlp_receiver_traces_disabled: bool,
    pub otlp_receiver_metrics_disabled: bool,
    pub otlp_receiver_logs_disabled: bool,
    pub otlp_receiver_traces_http_path: String,
    pub otlp_receiver_metrics_http_path: String,
    pub otlp_receiver_logs_http_path: String,
    pub otlp_http_include_metadata: bool,
    pub otlp_http_headers_to_include: Vec<String>,
    pub otlp_grpc_include_metadata: bool,
    pub otlp_grpc_metadata_keys_to_include: Vec<String>,
}
