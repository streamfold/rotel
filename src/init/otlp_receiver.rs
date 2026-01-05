use crate::init::parse;
use crate::receivers::otlp::OTLPReceiverConfig;
use clap::Args;
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct OTLPReceiverArgs {
    /// OTLP gRPC endpoint
    #[arg(long, env = "ROTEL_OTLP_GRPC_ENDPOINT", default_value = "localhost:4317", value_parser = parse::parse_endpoint
    )]
    pub otlp_grpc_endpoint: SocketAddr,

    /// OTLP HTTP endpoint
    #[arg(long, env = "ROTEL_OTLP_HTTP_ENDPOINT", default_value = "localhost:4318", value_parser = parse::parse_endpoint
    )]
    pub otlp_http_endpoint: SocketAddr,

    /// OTLP GRPC max recv msg size MB
    #[arg(
        long,
        env = "ROTEL_OTLP_GRPC_MAX_RECV_MSG_SIZE_MIB",
        default_value = "4"
    )]
    pub otlp_grpc_max_recv_msg_size_mib: u64,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_TRACES_DISABLED",
        default_value = "false"
    )]
    pub otlp_receiver_traces_disabled: bool,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_METRICS_DISABLED",
        default_value = "false"
    )]
    pub otlp_receiver_metrics_disabled: bool,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_LOGS_DISABLED",
        default_value = "false"
    )]
    pub otlp_receiver_logs_disabled: bool,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_TRACES_HTTP_PATH",
        default_value = "/v1/traces"
    )]
    pub otlp_receiver_traces_http_path: String,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_METRICS_HTTP_PATH",
        default_value = "/v1/metrics"
    )]
    pub otlp_receiver_metrics_http_path: String,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_LOGS_HTTP_PATH",
        default_value = "/v1/logs"
    )]
    pub otlp_receiver_logs_http_path: String,

    /// Enable including HTTP request headers in message metadata (context).
    /// When enabled, specified headers are stored in context and can be accessed by processors.
    /// This follows the OTel Collector pattern where processors pull from context to add attributes.
    /// Example: set to true to enable metadata extraction
    #[arg(
        long,
        env = "ROTEL_OTLP_HTTP_INCLUDE_METADATA",
        default_value = "false"
    )]
    pub otlp_http_include_metadata: bool,

    /// Comma-separated list of HTTP headers to include in metadata when include_metadata is enabled.
    /// Headers are stored in context and can be accessed by processors using from_context.
    /// Example: "my-custom-header,another-header"
    #[arg(long, env = "ROTEL_OTLP_HTTP_HEADERS_TO_INCLUDE", default_value = "")]
    pub otlp_http_headers_to_include: String,

    /// Enable including gRPC request metadata in message metadata (context).
    /// When enabled, specified metadata keys are stored in context and can be accessed by processors.
    /// This follows the OTel Collector pattern where processors pull from context to add attributes.
    /// Example: set to true to enable metadata extraction
    #[arg(
        long,
        env = "ROTEL_OTLP_GRPC_INCLUDE_METADATA",
        default_value = "false"
    )]
    pub otlp_grpc_include_metadata: bool,

    /// Comma-separated list of gRPC headers to include in metadata when include_metadata is enabled.
    /// Headers are stored in context and can be accessed by processors using from_context.
    /// Example: "my-custom-header,another-header"
    #[arg(long, env = "ROTEL_OTLP_GRPC_HEADERS_TO_INCLUDE", default_value = "")]
    pub otlp_grpc_headers_to_include: String,
}

impl Default for OTLPReceiverArgs {
    fn default() -> Self {
        OTLPReceiverArgs {
            otlp_grpc_endpoint: "127.0.0.1:4317".parse().unwrap(),
            otlp_http_endpoint: "127.0.0.1:4318".parse().unwrap(),
            otlp_grpc_max_recv_msg_size_mib: 4,
            otlp_receiver_traces_disabled: false,
            otlp_receiver_metrics_disabled: false,
            otlp_receiver_logs_disabled: false,
            otlp_receiver_traces_http_path: "/v1/traces".to_string(),
            otlp_receiver_metrics_http_path: "/v1/metrics".to_string(),
            otlp_receiver_logs_http_path: "/v1/logs".to_string(),
            otlp_http_include_metadata: false,
            otlp_http_headers_to_include: String::new(),
            otlp_grpc_include_metadata: false,
            otlp_grpc_headers_to_include: String::new(),
        }
    }
}

impl From<&OTLPReceiverArgs> for OTLPReceiverConfig {
    fn from(value: &OTLPReceiverArgs) -> Self {
        OTLPReceiverConfig {
            otlp_grpc_endpoint: value.otlp_grpc_endpoint,
            otlp_http_endpoint: value.otlp_http_endpoint,
            otlp_grpc_max_recv_msg_size_mib: value.otlp_grpc_max_recv_msg_size_mib,
            otlp_receiver_traces_disabled: value.otlp_receiver_traces_disabled,
            otlp_receiver_metrics_disabled: value.otlp_receiver_metrics_disabled,
            otlp_receiver_logs_disabled: value.otlp_receiver_logs_disabled,
            otlp_receiver_traces_http_path: value.otlp_receiver_traces_http_path.to_owned(),
            otlp_receiver_metrics_http_path: value.otlp_receiver_metrics_http_path.to_owned(),
            otlp_receiver_logs_http_path: value.otlp_receiver_logs_http_path.to_owned(),
            otlp_http_include_metadata: value.otlp_http_include_metadata,
            otlp_http_headers_to_include: value
                .otlp_http_headers_to_include
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            otlp_grpc_include_metadata: value.otlp_grpc_include_metadata,
            otlp_grpc_headers_to_include: value
                .otlp_grpc_headers_to_include
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        }
    }
}
