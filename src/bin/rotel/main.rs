// SPDX-License-Identifier: Apache-2.0

use crate::listener::Listener;
use clap::{Args, Parser, ValueEnum};
use opentelemetry::global;
use rotel::bounded_channel::bounded;
use rotel::exporters::blackhole::BlackholeExporter;
use rotel::exporters::otlp::config::{
    OTLPExporterLogsConfig, OTLPExporterMetricsConfig, OTLPExporterTracesConfig,
};
use rotel::exporters::otlp::{CompressionEncoding, Endpoint, Protocol};
use rotel::receivers::otlp_grpc::OTLPGrpcServer;
use rotel::receivers::otlp_http::OTLPHttpServer;
use rotel::receivers::otlp_output::OTLPOutput;
use rotel::topology::debug::DebugLogger;
use rotel::{listener, telemetry, topology};
use std::cmp::max;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::process::{exit, ExitCode};
use std::time::Duration;
use tokio::select;
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::task::JoinSet;
use tokio::time::{timeout_at, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

use gethostname::gethostname;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::Resource;
use rotel::exporters::datadog::{DatadogTraceExporter, Region};
use rotel::exporters::otlp;
use rotel::topology::batch::BatchConfig;

#[cfg(feature = "pprof")]
mod pprof;

// Used when daemonized
static WORKDING_DIR: &str = "/"; // TODO

const SENDING_QUEUE_SIZE: usize = 1_000;

#[derive(Debug, Args, Clone)]
pub struct AgentRun {
    /// Daemonize
    #[arg(long, env = "ROTEL_DAEMON", default_value = "false")]
    daemon: bool,

    /// PID file
    #[arg(long, env = "ROTEL_PID_FILE", default_value = "/tmp/rotel-agent.pid")]
    pid_file: String,

    /// Log file
    #[arg(long, env = "ROTEL_LOG_FILE", default_value = "/tmp/rotel-agent.log")]
    log_file: String,

    /// Debug log
    #[arg(value_enum, long, env = "ROTEL_DEBUG_LOG", default_value = "none")]
    debug_log: Vec<DebugLogParam>,

    /// OTLP gRPC endpoint
    #[arg(long, env = "ROTEL_OTLP_GRPC_ENDPOINT", default_value = "localhost:4317", value_parser = parse_endpoint
    )]
    otlp_grpc_endpoint: SocketAddr,

    /// OTLP HTTP endpoint
    #[arg(long, env = "ROTEL_OTLP_HTTP_ENDPOINT", default_value = "localhost:4318", value_parser = parse_endpoint
    )]
    otlp_http_endpoint: SocketAddr,

    /// OTLP Host
    ///

    /// OTLP GRPC max recv msg size MB
    #[arg(
        long,
        env = "ROTEL_OTLP_GRPC_MAX_RECV_MSG_SIZE_MIB",
        default_value = "4"
    )]
    otlp_grpc_max_recv_msg_size_mib: u64,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_TRACES_DISABLED",
        default_value = "false"
    )]
    otlp_receiver_traces_disabled: bool,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_METRICS_DISABLED",
        default_value = "false"
    )]
    otlp_receiver_metrics_disabled: bool,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_LOGS_DISABLED",
        default_value = "false"
    )]
    otlp_receiver_logs_disabled: bool,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_TRACES_HTTP_PATH",
        default_value = "/v1/traces"
    )]
    otlp_receiver_traces_http_path: String,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_METRICS_HTTP_PATH",
        default_value = "/v1/metrics"
    )]
    otlp_receiver_metrics_http_path: String,

    #[arg(
        long,
        env = "ROTEL_OTLP_RECEIVER_LOGS_HTTP_PATH",
        default_value = "/v1/logs"
    )]
    otlp_receiver_logs_http_path: String,

    /// Exporter
    #[arg(value_enum, long, env = "ROTEL_EXPORTER", default_value = "otlp")]
    exporter: Exporter,

    /// OTLP Exporter Endpoint - Used as default for all OTLP data types unless more specific flag specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_ENDPOINT")]
    otlp_exporter_endpoint: Option<String>,

    /// OTLP Exporter Traces Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_ENDPOINT")]
    otlp_exporter_traces_endpoint: Option<String>,

    /// OTLP Exporter Metrics Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_ENDPOINT")]
    otlp_exporter_metrics_endpoint: Option<String>,

    /// OTLP Exporter Logs Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_ENDPOINT")]
    otlp_exporter_logs_endpoint: Option<String>,

    /// OTLP Exporter Protocol - Used as default for all OTLP data types unless more specific flag specified
    #[arg(
        value_enum,
        long,
        env = "ROTEL_OTLP_EXPORTER_PROTOCOL",
        default_value = "grpc"
    )]
    otlp_exporter_protocol: OTLPExporterProtocol,

    /// OTLP Exporter Traces Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_TRACES_PROTOCOL")]
    otlp_exporter_traces_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Metrics Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_METRICS_PROTOCOL")]
    otlp_exporter_metrics_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Logs Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_LOGS_PROTOCOL")]
    otlp_exporter_logs_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Headers - Used as default for all OTLP data types unless more specific flag specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_CUSTOM_HEADERS", value_parser = parse_key_val::<String, String>, value_delimiter = ','
    )]
    otlp_exporter_custom_headers: Vec<(String, String)>,

    /// OTLP Exporter Traces Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_CUSTOM_HEADERS", value_parser = parse_key_val::<String, String>, value_delimiter = ','
    )]
    otlp_exporter_traces_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Metrics Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_CUSTOM_HEADERS", value_parser = parse_key_val::<String, String>, value_delimiter = ','
    )]
    otlp_exporter_metrics_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Logs Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_CUSTOM_HEADERS", value_parser = parse_key_val::<String, String>, value_delimiter = ',')]
    otlp_exporter_logs_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Compression - Used as default for all OTLP data types unless more specific flag specified
    #[arg(
        value_enum,
        long,
        env = "ROTEL_OTLP_EXPORTER_COMPRESSION",
        default_value = "gzip"
    )]
    otlp_exporter_compression: CompressionEncoding,

    /// OTLP Exporter Traces Compression - Overrides otlp_exporter_compression if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_TRACES_COMPRESSION")]
    otlp_exporter_traces_compression: Option<CompressionEncoding>,

    /// OTLP Exporter Metrics Compression - Overrides otlp_exporter_compression if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_METRICS_COMPRESSION")]
    otlp_exporter_metrics_compression: Option<CompressionEncoding>,

    /// OTLP Exporter Logs Compression - Overrides otlp_exporter_compression if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_LOGS_COMPRESSION")]
    otlp_exporter_logs_compression: Option<CompressionEncoding>,

    #[clap(flatten)]
    otlp_exporter_cert_group: CertGroup,

    #[clap(flatten)]
    otlp_exporter_key_group: KeyGroup,

    #[clap(flatten)]
    otlp_exporter_ca_group: CaGroup,

    #[clap(flatten)]
    otlp_exporter_traces_cert_group: TracesCertGroup,

    #[clap(flatten)]
    otlp_exporter_traces_key_group: TracesKeyGroup,

    #[clap(flatten)]
    otlp_exporter_traces_ca_group: TracesCaGroup,

    #[clap(flatten)]
    otlp_exporter_metrics_cert_group: MetricsCertGroup,

    #[clap(flatten)]
    otlp_exporter_metrics_key_group: MetricsKeyGroup,

    #[clap(flatten)]
    otlp_exporter_metrics_ca_group: MetricsCaGroup,

    #[clap(flatten)]
    otlp_exporter_logs_cert_group: LogsCertGroup,

    #[clap(flatten)]
    otlp_exporter_logs_key_group: LogsKeyGroup,

    #[clap(flatten)]
    otlp_exporter_logs_ca_group: LogsCaGroup,

    /// OTLP Exporter TLS SKIP VERIFY - Used as default for all OTLP data types unless more specific flag specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_TLS_SKIP_VERIFY",
        default_value = "false"
    )]
    otlp_exporter_tls_skip_verify: bool,

    /// OTLP Exporter traces TLS SKIP VERIFY - Overrides otlp_exporter_tls_skip_verify for OTLP traces if specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_SKIP_VERIFY")]
    otlp_exporter_traces_tls_skip_verify: Option<bool>,

    /// OTLP Exporter metrics TLS SKIP VERIFY - Overrides otlp_exporter_tls_skip_verify for OTLP metrics if specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_SKIP_VERIFY")]
    otlp_exporter_metrics_tls_skip_verify: Option<bool>,

    /// OTLP Exporter logs TLS SKIP VERIFY - Overrides otlp_exporter_tls_skip_verify for OTLP logs if specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_SKIP_VERIFY")]
    otlp_exporter_logs_tls_skip_verify: Option<bool>,

    /// OTLP Exporter Request Timeout - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_REQUEST_TIMEOUT",
        default_value = "5s"
    )]
    otlp_exporter_request_timeout: humantime::Duration,

    /// OTLP Exporter traces Request Timeout - Overrides otlp_exporter_request_timeout for OTLP traces if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_REQUEST_TIMEOUT")]
    otlp_exporter_traces_request_timeout: Option<humantime::Duration>,

    /// OTLP Exporter metrics Request Timeout - Overrides otlp_exporter_request_timeout for OTLP metrics if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_REQUEST_TIMEOUT")]
    otlp_exporter_metrics_request_timeout: Option<humantime::Duration>,

    /// OTLP Exporter logs Request Timeout - Overrides otlp_exporter_request_timeout for OTLP logs if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_REQUEST_TIMEOUT")]
    otlp_exporter_logs_request_timeout: Option<humantime::Duration>,

    /// OTLP Exporter Retry initial backoff - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_RETRY_INITIAL_BACKOFF",
        default_value = "5s"
    )]
    otlp_exporter_retry_initial_backoff: humantime::Duration,

    /// OTLP Exporter traces Retry initial backoff - Overrides otlp_exporter_retry_initial_backoff for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_INITIAL_BACKOFF")]
    otlp_exporter_traces_retry_initial_backoff: Option<humantime::Duration>,

    /// OTLP Exporter metrics Retry initial backoff  - Overrides otlp_exporter_retry_initial_backoff for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_INITIAL_BACKOFF")]
    otlp_exporter_metrics_retry_initial_backoff: Option<humantime::Duration>,

    /// OTLP Exporter logs Retry initial backoff  - Overrides otlp_exporter_retry_initial_backoff for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_INITIAL_BACKOFF")]
    otlp_exporter_logs_retry_initial_backoff: Option<humantime::Duration>,

    /// OTLP Exporter Retry max backoff - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_RETRY_MAX_BACKOFF",
        default_value = "30s"
    )]
    otlp_exporter_retry_max_backoff: humantime::Duration,

    /// OTLP Exporter traces Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_MAX_BACKOFF")]
    otlp_exporter_traces_retry_max_backoff: Option<humantime::Duration>,

    /// OTLP Exporter metrics Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_MAX_BACKOFF")]
    otlp_exporter_metrics_retry_max_backoff: Option<humantime::Duration>,

    /// OTLP Exporter logs Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_MAX_BACKOFF")]
    otlp_exporter_logs_retry_max_backoff: Option<humantime::Duration>,

    /// OTLP Exporter Retry max elapsed time - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_RETRY_MAX_ELAPSED_TIME",
        default_value = "300s"
    )]
    otlp_exporter_retry_max_elapsed_time: humantime::Duration,

    /// OTLP Exporter traces Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_MAX_ELAPSED_TIME")]
    otlp_exporter_traces_retry_max_elapsed_time: Option<humantime::Duration>,

    /// OTLP Exporter metrics Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_MAX_ELAPSED_TIME")]
    otlp_exporter_metrics_retry_max_elapsed_time: Option<humantime::Duration>,

    /// OTLP Exporter logs Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_MAX_ELAPSED_TIME")]
    otlp_exporter_logs_retry_max_elapsed_time: Option<humantime::Duration>,

    // Batch settings
    /// OTLP Exporter max batch size in number of spans/metrics - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_BATCH_MAX_SIZE",
        default_value = "8192"
    )]
    otlp_exporter_batch_max_size: usize,

    /// OTLP Exporter traces max batch size in number of spans - Overrides otlp_exporter_batch_max_size for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_BATCH_MAX_SIZE")]
    otlp_exporter_traces_batch_max_size: Option<usize>,

    /// OTLP Exporter metrics max batch size in number of metrics - Overrides otlp_exporter_batch_max_size for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_BATCH_MAX_SIZE")]
    otlp_exporter_metrics_batch_max_size: Option<usize>,

    /// OTLP Exporter logs max batch size in number of logs - Overrides otlp_exporter_batch_max_size for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_BATCH_MAX_SIZE")]
    otlp_exporter_logs_batch_max_size: Option<usize>,

    /// OTLP Exporter batch timeout - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_BATCH_TIMEOUT",
        default_value = "200ms"
    )]
    otlp_exporter_batch_timeout: humantime::Duration,

    /// OTLP Exporter traces batch timeout - Overrides otlp_exporter_batch_timeout for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_BATCH_TIMEOUT")]
    otlp_exporter_traces_batch_timeout: Option<humantime::Duration>,

    /// OTLP Exporter metrics batch timeout - Overrides otlp_exporter_batch_timeout for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_BATCH_TIMEOUT")]
    otlp_exporter_metrics_batch_timeout: Option<humantime::Duration>,

    /// OTLP Exporter logs batch timeout - Overrides otlp_exporter_batch_timeout for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_BATCH_TIMEOUT")]
    otlp_exporter_logs_batch_timeout: Option<humantime::Duration>,

    #[cfg(feature = "pprof")]
    #[clap(flatten)]
    profile_group: ProfileGroup,

    /// Datadog Exporter Region
    #[arg(
        value_enum,
        long,
        env = "ROTEL_DATADOG_EXPORTER_REGION",
        default_value = "us1"
    )]
    datadog_exporter_region: DatadogRegion,

    /// Datadog Exporter custom endpoint override
    #[arg(long, env = "ROTEL_DATADOG_EXPORTER_CUSTOM_ENDPOINT")]
    datadog_exporter_custom_endpoint: Option<String>,

    /// Datadog Exporter API key
    #[arg(long, env = "ROTEL_DATADOG_EXPORTER_API_KEY")]
    datadog_exporter_api_key: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum DatadogRegion {
    US1,
    US3,
    US5,
    EU,
    AP1,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum DebugLogParam {
    None,
    Traces,
    Metrics,
    Logs,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum OTLPExporterProtocol {
    Grpc,
    Http,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct CertGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TLS_CERT_FILE", default_value = None)]
    otlp_exporter_tls_cert_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TLS_CERT_PEM", default_value = None)]
    otlp_exporter_tls_cert_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct KeyGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TLS_KEY_FILE", default_value = None)]
    otlp_exporter_tls_key_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TLS_KEY_PEM", default_value = None)]
    otlp_exporter_tls_key_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct CaGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TLS_CA_FILE", default_value = None)]
    otlp_exporter_tls_ca_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TLS_CA_PEM", default_value = None)]
    otlp_exporter_tls_ca_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct TracesCertGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_CERT_FILE", default_value = None)]
    otlp_exporter_traces_tls_cert_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_CERT_PEM", default_value = None)]
    otlp_exporter_traces_tls_cert_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct TracesKeyGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_KEY_FILE", default_value = None)]
    otlp_exporter_traces_tls_key_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_KEY_PEM", default_value = None)]
    otlp_exporter_traces_tls_key_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct TracesCaGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_CA_FILE", default_value = None)]
    otlp_exporter_traces_tls_ca_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_CA_PEM", default_value = None)]
    otlp_exporter_traces_tls_ca_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct MetricsCertGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_CERT_FILE", default_value = None)]
    otlp_exporter_metrics_tls_cert_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_CERT_PEM", default_value = None)]
    otlp_exporter_metrics_tls_cert_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct MetricsKeyGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_KEY_FILE", default_value = None)]
    otlp_exporter_metrics_tls_key_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_KEY_PEM", default_value = None)]
    otlp_exporter_metrics_tls_key_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct MetricsCaGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_CA_FILE", default_value = None)]
    otlp_exporter_metrics_tls_ca_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_CA_PEM", default_value = None)]
    otlp_exporter_metrics_tls_ca_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct LogsCertGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_CERT_FILE", default_value = None)]
    otlp_exporter_logs_tls_cert_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_CERT_PEM", default_value = None)]
    otlp_exporter_logs_tls_cert_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct LogsKeyGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_KEY_FILE", default_value = None)]
    otlp_exporter_logs_tls_key_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_KEY_PEM", default_value = None)]
    otlp_exporter_logs_tls_key_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone)]
#[group(required = false, multiple = false)]
pub struct LogsCaGroup {
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_CA_FILE", default_value = None)]
    otlp_exporter_logs_tls_ca_file: Option<String>,

    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_CA_PEM", default_value = None)]
    otlp_exporter_logs_tls_ca_pem: Option<String>,
}

#[derive(Debug, clap::Args)]
#[group(required = false, multiple = false)]
#[derive(Clone)]
pub struct ProfileGroup {
    #[arg(long, env = "ROTEL_PPROF_FLAME_GRAPH", default_value = "false")]
    pprof_flame_graph: bool,

    #[arg(long, env = "ROTEL_PPROF_CALL_GRAPH", default_value = "false")]
    pprof_call_graph: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum Exporter {
    Otlp,

    Blackhole,

    Datadog,
}

#[derive(Debug, clap::Subcommand)]
pub enum Commands {
    /// Run agent
    Start(Box<AgentRun>),

    /// Return version
    Version,
}

#[derive(Debug, Parser)]
#[command(name = "rotel")]
#[command(bin_name = "rotel")]
#[command(version, about, long_about = None)]
#[command(subcommand_required = true)]
pub struct Arguments {
    #[arg(long, global = true, env = "ROTEL_LOG_LEVEL", default_value = "info")]
    /// Log configuration
    log_level: String,

    #[arg(
        value_enum,
        long,
        global = true,
        env = "ROTEL_LOG_FORMAT",
        default_value = "text"
    )]
    /// Log format
    log_format: LogFormatArg,

    #[arg(long, global = true, env = "ROTEL_ENVIRONMENT", default_value = "dev")]
    /// Environment
    environment: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum LogFormatArg {
    Text,
    Json,
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

/// Parse an endpoint
fn parse_endpoint(s: &str) -> Result<SocketAddr, Box<dyn Error + Send + Sync + 'static>> {
    // Use actual localhost address instead of localhost name
    let s = if s.starts_with("localhost:") {
        s.replace("localhost:", "127.0.0.1:")
    } else {
        s.to_string()
    };
    let sa: SocketAddr = s.parse()?;
    Ok(sa)
}

fn main() -> ExitCode {
    let opt = Arguments::parse();

    match opt.command {
        Some(Commands::Version) => {
            println!("{}", get_version())
        }
        Some(Commands::Start(agent)) => {
            // Attempt to bind ports before we daemonize, so that when the parent process returns
            // the ports are already available for connection.
            let port_map =
                match bind_endpoints(&[agent.otlp_grpc_endpoint, agent.otlp_http_endpoint]) {
                    Ok(ports) => ports,
                    Err(e) => {
                        unsafe {
                            if agent.daemon && check_rotel_active(&agent.pid_file) {
                                // If we are already running, ignore the bind failure
                                return ExitCode::SUCCESS;
                            }
                        }
                        eprintln!("ERROR: {}", e);

                        return ExitCode::from(1);
                    }
                };

            if agent.daemon {
                match daemonize(&agent.pid_file, &agent.log_file) {
                    Ok(Some(exitcode)) => return exitcode,
                    Err(e) => {
                        eprintln!("ERROR: failed to daemonize: {:?}", e);
                        return ExitCode::from(1);
                    }
                    _ => {}
                }
            }

            let _logger = setup_logging(&opt.log_level, &opt.log_format);

            match run_agent(agent, port_map, &opt.environment) {
                Ok(_) => {}
                Err(e) => {
                    error!(error = ?e, "Failed to run agent.");
                    return ExitCode::from(1);
                }
            }
        }
        _ => {
            // it shouldn't be possible to get here since we mark a subcommand as
            // required
            error!("Must specify a command");
            return ExitCode::from(2);
        }
    }

    ExitCode::SUCCESS
}

#[tokio::main]
async fn run_agent(
    agent: Box<AgentRun>,
    mut port_map: HashMap<SocketAddr, Listener>,
    environment: &String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    info!(
        grpc_endpoint = agent.otlp_grpc_endpoint.to_string(),
        http_endpoint = agent.otlp_http_endpoint.to_string(),
        "Starting Rotel.",
    );

    // Initialize the TLS library, we may want to do this conditionally
    if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
        return Err(format!("failed to initialize crypto library: {:?}", e).into());
    }

    let num_cpus = num_cpus::get();

    let mut receivers_task_set = JoinSet::new();
    let mut pipeline_task_set = JoinSet::new();
    let mut exporters_task_set = JoinSet::new();

    let receivers_cancel = CancellationToken::new();
    let pipeline_cancel = CancellationToken::new();
    let exporters_cancel = CancellationToken::new();

    let activation = TelemetryActivation::from_config(&agent);

    // If there are no listeners, suggest the blackhole exporter
    if activation.traces == TelemetryState::NoListeners
        && activation.metrics == TelemetryState::NoListeners
        && activation.logs == TelemetryState::NoListeners
    {
        return Err(
            "no exporter endpoints specified, perhaps you meant to use --exporter blackhole instead"
                .into(),
        );
    }

    // If no active type exists, nothing to do. Exit here before errors later
    if !(activation.traces == TelemetryState::Active
        || activation.metrics == TelemetryState::Active
        || activation.logs == TelemetryState::Active)
    {
        return Err(
            "there are no active telemetry types, exiting because there is nothing to do".into(),
        );
    }

    let (trace_pipeline_in_tx, trace_pipeline_in_rx) =
        bounded::<Vec<ResourceSpans>>(max(4, num_cpus));
    let (trace_pipeline_out_tx, trace_pipeline_out_rx) =
        bounded::<Vec<ResourceSpans>>(SENDING_QUEUE_SIZE);
    let trace_otlp_output = OTLPOutput::new(trace_pipeline_in_tx);

    let (metrics_pipeline_in_tx, metrics_pipeline_in_rx) =
        bounded::<Vec<ResourceMetrics>>(max(4, num_cpus));
    let (metrics_pipeline_out_tx, metrics_pipeline_out_rx) =
        bounded::<Vec<ResourceMetrics>>(SENDING_QUEUE_SIZE);
    let metrics_otlp_output = OTLPOutput::new(metrics_pipeline_in_tx);

    let (logs_pipeline_in_tx, logs_pipeline_in_rx) = bounded::<Vec<ResourceLogs>>(max(4, num_cpus));
    let (logs_pipeline_out_tx, logs_pipeline_out_rx) =
        bounded::<Vec<ResourceLogs>>(SENDING_QUEUE_SIZE);
    let logs_otlp_output = OTLPOutput::new(logs_pipeline_in_tx);

    let mut traces_output = None;
    let mut metrics_output = None;
    let mut logs_output = None;

    match activation.traces {
        TelemetryState::Active => traces_output = Some(trace_otlp_output),
        TelemetryState::Disabled => {
            info!("OTLP Receiver for traces disabled, OTLP receiver will be configured to not accept traces");
        }
        TelemetryState::NoListeners => {
            info!("No exporters are configured for traces, OTLP receiver will be configured to not accept traces");
        }
    }

    match activation.metrics {
        TelemetryState::Active => metrics_output = Some(metrics_otlp_output),
        TelemetryState::Disabled => {
            info!("OTLP Receiver for metrics disabled, OTLP receiver will be configured to not accept metrics");
        }
        TelemetryState::NoListeners => {
            info!("No exporters are configured for metrics, OTLP receiver will be configured to not accept metrics");
        }
    }

    match activation.logs {
        TelemetryState::Active => logs_output = Some(logs_otlp_output),
        TelemetryState::Disabled => {
            info!("OTLP Receiver for logs disabled, OTLP receiver will be configured to not accept logs");
        }
        TelemetryState::NoListeners => {
            info!("No exporters are configured for logs, OTLP receiver will be configured to not accept logs");
        }
    }

    //
    // OTLP GRPC server
    //
    let grpc_srv = OTLPGrpcServer::builder()
        .with_max_recv_msg_size_mib(agent.otlp_grpc_max_recv_msg_size_mib as usize)
        .with_traces_output(traces_output.clone())
        .with_metrics_output(metrics_output.clone())
        .with_logs_output(logs_output.clone())
        .build();

    let grpc_listener = port_map.remove(&agent.otlp_grpc_endpoint).unwrap();
    {
        let receivers_cancel = receivers_cancel.clone();
        receivers_task_set
            .spawn(async move { grpc_srv.serve(grpc_listener, receivers_cancel).await });
    }

    //
    // OTLP HTTP server
    //
    let http_srv = OTLPHttpServer::builder()
        .with_traces_output(traces_output.clone())
        .with_metrics_output(metrics_output.clone())
        .with_logs_output(logs_output.clone())
        .with_traces_path(agent.otlp_receiver_traces_http_path.clone())
        .with_metrics_path(agent.otlp_receiver_metrics_http_path.clone())
        .with_logs_path(agent.otlp_receiver_logs_http_path.clone())
        .build();

    let http_listener = port_map.remove(&agent.otlp_http_endpoint).unwrap();
    {
        let receivers_cancel = receivers_cancel.clone();
        receivers_task_set
            .spawn(async move { http_srv.serve(http_listener, receivers_cancel).await });
    }

    let mut trace_pipeline = topology::generic_pipeline::Pipeline::new(
        trace_pipeline_in_rx.clone(),
        trace_pipeline_out_tx,
        build_traces_batch_config(agent.clone()),
    );

    let mut metrics_pipeline = topology::generic_pipeline::Pipeline::new(
        metrics_pipeline_in_rx.clone(),
        metrics_pipeline_out_tx,
        build_metrics_batch_config(agent.clone()),
    );

    let mut logs_pipeline = topology::generic_pipeline::Pipeline::new(
        logs_pipeline_in_rx.clone(),
        logs_pipeline_out_tx,
        build_logs_batch_config(agent.clone()),
    );

    let token = exporters_cancel.clone();
    match agent.exporter {
        Exporter::Blackhole => {
            let mut exp = BlackholeExporter::new(
                trace_pipeline_out_rx.clone(),
                metrics_pipeline_out_rx.clone(),
            );

            exporters_task_set.spawn(async move {
                exp.start(token).await;
                Ok(())
            });
        }
        Exporter::Otlp => {
            let endpoint = agent.otlp_exporter_endpoint.as_ref();
            if activation.traces == TelemetryState::Active {
                let traces_config = build_traces_config(agent.clone(), endpoint);
                let mut traces = otlp::exporter::build_traces_exporter(
                    traces_config,
                    trace_pipeline_out_rx.clone(),
                )?;
                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    let res = traces.start(token).await;
                    if let Err(e) = res {
                        error!(
                            exporter_type = "otlp_traces",
                            error = e,
                            "OTLPExporter exporter returned from run loop with error."
                        );
                    }

                    Ok(())
                });
            }
            if activation.metrics == TelemetryState::Active {
                let metrics_config = build_metrics_config(agent.clone(), endpoint);
                let mut metrics = otlp::exporter::build_metrics_exporter(
                    metrics_config,
                    metrics_pipeline_out_rx.clone(),
                )?;
                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    let res = metrics.start(token).await;
                    if let Err(e) = res {
                        error!(
                            exporter_type = "otlp_metrics",
                            error = e,
                            "OTLPExporter returned from run loop with error."
                        );
                    }

                    Ok(())
                });
            }
            if activation.logs == TelemetryState::Active {
                let logs_config = build_logs_config(agent.clone(), endpoint);
                let mut logs =
                    otlp::exporter::build_logs_exporter(logs_config, logs_pipeline_out_rx.clone())?;
                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    let res = logs.start(token).await;
                    if let Err(e) = res {
                        error!(
                            exporter_type = "otlp_logs",
                            error = e,
                            "OTLPExporter returned from run loop with error."
                        );
                    }

                    Ok(())
                });
            }
        }

        Exporter::Datadog => {
            if agent.datadog_exporter_api_key.is_none() {
                // todo: is there a way to make this config required with the exporter mode?
                return Err("must specify Datadog exporter API key".into());
            }
            let api_key = agent.datadog_exporter_api_key.unwrap();

            let hostname = get_hostname();

            let mut builder = DatadogTraceExporter::builder(
                agent.datadog_exporter_region.into(),
                agent.datadog_exporter_custom_endpoint.clone(),
                api_key,
            )
            .with_environment(environment.clone());

            if let Some(hostname) = hostname {
                builder = builder.with_hostname(hostname);
            }

            let mut exp = builder.build(trace_pipeline_out_rx)?;

            exporters_task_set.spawn(async move {
                let res = exp.start(token).await;
                if let Err(e) = res {
                    error!(
                        error = e,
                        "Datadog exporter returned from run loop with error."
                    );
                }

                Ok(())
            });
        }
    }

    if traces_output.is_some() {
        let log_traces = agent.debug_log.contains(&DebugLogParam::Traces);
        let dbg_log = DebugLogger::new(log_traces);

        let pipeline_cancel = pipeline_cancel.clone();
        pipeline_task_set
            .spawn(async move { trace_pipeline.start(dbg_log, pipeline_cancel).await });
    }
    if metrics_output.is_some() {
        let log_metrics = agent.debug_log.contains(&DebugLogParam::Metrics);
        let dbg_log = DebugLogger::new(log_metrics);

        let pipeline_cancel = pipeline_cancel.clone();
        pipeline_task_set
            .spawn(async move { metrics_pipeline.start(dbg_log, pipeline_cancel).await });
    }
    if logs_output.is_some() {
        let log_logs = agent.debug_log.contains(&DebugLogParam::Logs);
        let dbg_log = DebugLogger::new(log_logs);

        let pipeline_cancel = pipeline_cancel.clone();
        pipeline_task_set.spawn(async move { logs_pipeline.start(dbg_log, pipeline_cancel).await });
    }

    let internal_metrics_exporter = telemetry::internal_exporter::InternalOTLPMetricsExporter::new(
        metrics_output.clone(),
        Temporality::Cumulative,
    );

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(internal_metrics_exporter)
        .with_resource(Resource::builder().with_service_name("rotel").build())
        .build();
    global::set_meter_provider(meter_provider.clone());

    #[cfg(feature = "pprof")]
    let guard = if agent.profile_group.pprof_flame_graph || agent.profile_group.pprof_call_graph {
        pprof::pprof_guard()
    } else {
        None
    };

    let mut result = Ok(());
    select! {
        _ = signal_wait() => {
            info!("Shutdown signal received.");

            #[cfg(feature = "pprof")]
            if agent.profile_group.pprof_flame_graph || agent.profile_group.pprof_call_graph {
                pprof::pprof_finish(guard, agent.profile_group.pprof_flame_graph, agent.profile_group.pprof_call_graph);
            }
        },
        e = wait_for_any_task(&mut receivers_task_set) => {
            match e {
                Ok(()) => warn!("Unexpected early exit of receiver."),
                Err(e) => result = Err(e),
            }
        },
        e = wait_for_any_task(&mut pipeline_task_set) => {
            match e {
                Ok(()) => warn!("Unexpected early exit of pipeline."),
                Err(e) => result = Err(e),
            }
        },
        e = wait_for_any_task(&mut exporters_task_set) => {
            match e {
                Ok(()) => warn!("Unexpected early exit of task."),
                Err(e) => result = Err(e),
            }
        }
    }
    result?;

    // Step one, cancel the receivers and wait for their termination.
    receivers_cancel.cancel();

    // Wait up until one second for receivers to finish
    let res = wait_for_tasks_with_timeout(&mut receivers_task_set, Duration::from_secs(1)).await;
    if let Err(e) = res {
        return Err(format!("timed out waiting for receiver exit: {}", e).into());
    }

    // Drop the outputs (alternatively move them into receivers?), causing downstream
    // components to exit
    drop(traces_output);
    drop(metrics_output);
    drop(logs_output);

    // Set a maximum duration for exporters to exit, this way if the pipelines exit quickly,
    // the entire wall time is left for exporters to finish flushing (which may require longer if
    // endpoints are slow).
    let receivers_hard_stop = Instant::now() + Duration::from_secs(3);

    // Wait 500ms for the pipelines to finish. They should exit when the pipes are dropped.
    let res = wait_for_tasks_with_timeout(&mut pipeline_task_set, Duration::from_millis(500)).await;
    if res.is_err() {
        warn!("Pipelines did not exit on channel close, cancelling.");

        // force cancel
        pipeline_cancel.cancel();

        // try again
        let res =
            wait_for_tasks_with_timeout(&mut pipeline_task_set, Duration::from_millis(500)).await;
        if let Err(e) = res {
            return Err(format!("timed out waiting for pipline to exit: {}", e).into());
        }
    }

    // pipeline outputs are already moved, so should be closed

    // Wait for the exporters using the same process
    let res =
        wait_for_tasks_with_timeout(&mut exporters_task_set, Duration::from_millis(500)).await;
    if res.is_err() {
        warn!("Exporters did not exit on channel close, cancelling.");

        // force cancel
        exporters_cancel.cancel();

        let res = wait_for_tasks_with_deadline(&mut exporters_task_set, receivers_hard_stop).await;
        if let Err(e) = res {
            return Err(format!("timed out waiting for exporters to exit: {}", e).into());
        }
    }

    Ok(())
}

async fn wait_for_any_task(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let r = tasks.join_next().await;

    match r {
        None => Ok(()), // should not happen
        Some(res) => res?,
    }
}

async fn wait_for_tasks_with_timeout(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    wait_for_tasks_with_deadline(tasks, Instant::now() + timeout).await
}

async fn wait_for_tasks_with_deadline(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    stop_at: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut result = Ok(());
    loop {
        match timeout_at(stop_at, tasks.join_next()).await {
            Err(_) => {
                result = Err("timed out waiting for tasks to complete".into());
                break;
            }
            Ok(None) => break,
            Ok(Some(v)) => {
                match v {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => result = Err(e),
                    e => {
                        error!("Failed to join with task: {:?}", e)
                    } // Ignore?
                }
            }
        }
    }

    result
}

async fn signal_wait() {
    let mut sig_term = sig(SignalKind::terminate());
    let mut sig_int = sig(SignalKind::interrupt());

    select! {
        _ = sig_term.recv() => {},
        _ = sig_int.recv() => {},
    }
}

fn sig(kind: SignalKind) -> Signal {
    signal(kind).unwrap()
}

type LoggerGuard = tracing_appender::non_blocking::WorkerGuard;

fn setup_logging(log_level: &str, log_format: &LogFormatArg) -> std::io::Result<LoggerGuard> {
    LogTracer::init().expect("Unable to setup log tracer!");

    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stdout());

    if *log_format == LogFormatArg::Json {
        let app_name = format!("{}-{}", env!("CARGO_PKG_NAME"), get_version());
        let bunyan_formatting_layer = BunyanFormattingLayer::new(app_name, non_blocking_writer);

        let subscriber = Registry::default()
            .with(EnvFilter::new(log_level))
            .with(JsonStorageLayer)
            .with(bunyan_formatting_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    } else {
        // Create a formatting layer that writes to the file
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(non_blocking_writer)
            .with_target(false)
            .with_level(true)
            .compact();

        let subscriber = Registry::default()
            .with(EnvFilter::new(log_level))
            .with(file_layer);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    }
    Ok(guard)
}

fn build_traces_batch_config(agent: Box<AgentRun>) -> BatchConfig {
    BatchConfig {
        max_size: agent
            .otlp_exporter_traces_batch_max_size
            .unwrap_or(agent.otlp_exporter_batch_max_size),
        timeout: agent
            .otlp_exporter_traces_batch_timeout
            .unwrap_or(agent.otlp_exporter_batch_timeout)
            .into(),
    }
}

fn build_metrics_batch_config(agent: Box<AgentRun>) -> BatchConfig {
    BatchConfig {
        max_size: agent
            .otlp_exporter_metrics_batch_max_size
            .unwrap_or(agent.otlp_exporter_batch_max_size),
        timeout: agent
            .otlp_exporter_metrics_batch_timeout
            .unwrap_or(agent.otlp_exporter_batch_timeout)
            .into(),
    }
}

fn build_logs_batch_config(agent: Box<AgentRun>) -> BatchConfig {
    BatchConfig {
        max_size: agent
            .otlp_exporter_logs_batch_max_size
            .unwrap_or(agent.otlp_exporter_batch_max_size),
        timeout: agent
            .otlp_exporter_logs_batch_timeout
            .unwrap_or(agent.otlp_exporter_batch_timeout)
            .into(),
    }
}

fn build_traces_config(
    agent: Box<AgentRun>,
    endpoint: Option<&String>,
) -> OTLPExporterTracesConfig {
    let mut traces_config_builder = otlp::trace_config_builder(
        agent
            .otlp_exporter_traces_endpoint
            .map(|e| Endpoint::Full(e))
            .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone())), // This is only safe to unwrap endpoint here because we ensure that at least one of them is Some in the caller.
        agent
            .otlp_exporter_traces_protocol
            .unwrap_or(agent.otlp_exporter_protocol)
            .into(),
    )
    .with_tls_skip_verify(
        agent
            .otlp_exporter_traces_tls_skip_verify
            .unwrap_or(agent.otlp_exporter_tls_skip_verify),
    )
    .with_headers(
        agent
            .otlp_exporter_traces_custom_headers
            .as_ref()
            .unwrap_or(&agent.otlp_exporter_custom_headers),
    )
    .with_request_timeout(
        agent
            .otlp_exporter_traces_request_timeout
            .unwrap_or(agent.otlp_exporter_request_timeout)
            .into(),
    )
    .with_max_elapsed_time(
        agent
            .otlp_exporter_traces_retry_max_elapsed_time
            .unwrap_or(agent.otlp_exporter_retry_max_elapsed_time)
            .into(),
    )
    .with_initial_backoff(
        agent
            .otlp_exporter_traces_retry_initial_backoff
            .unwrap_or(agent.otlp_exporter_retry_initial_backoff)
            .into(),
    )
    .with_max_backoff(
        agent
            .otlp_exporter_traces_retry_max_backoff
            .unwrap_or(agent.otlp_exporter_retry_max_backoff)
            .into(),
    )
    .with_compression_encoding(
        agent
            .otlp_exporter_traces_compression
            .unwrap_or(agent.otlp_exporter_compression)
            .into(),
    );

    let traces_tls_cert_file = agent
        .otlp_exporter_traces_cert_group
        .otlp_exporter_traces_tls_cert_file
        .or(agent.otlp_exporter_cert_group.otlp_exporter_tls_cert_file);
    let traces_tls_cert_pem = agent
        .otlp_exporter_traces_cert_group
        .otlp_exporter_traces_tls_cert_pem
        .or(agent.otlp_exporter_cert_group.otlp_exporter_tls_cert_pem);

    let traces_tls_key_file = agent
        .otlp_exporter_traces_key_group
        .otlp_exporter_traces_tls_key_file
        .or(agent.otlp_exporter_key_group.otlp_exporter_tls_key_file);
    let traces_tls_key_pem = agent
        .otlp_exporter_traces_key_group
        .otlp_exporter_traces_tls_key_pem
        .or(agent.otlp_exporter_key_group.otlp_exporter_tls_key_pem);

    let traces_tls_ca_file = agent
        .otlp_exporter_traces_ca_group
        .otlp_exporter_traces_tls_ca_file
        .or(agent.otlp_exporter_ca_group.otlp_exporter_tls_ca_file);

    let traces_tls_ca_pem = agent
        .otlp_exporter_traces_ca_group
        .otlp_exporter_traces_tls_ca_pem
        .or(agent.otlp_exporter_ca_group.otlp_exporter_tls_ca_pem);

    if traces_tls_cert_file.is_some() {
        traces_config_builder =
            traces_config_builder.with_cert_file(traces_tls_cert_file.unwrap().as_str());
    } else if traces_tls_cert_pem.is_some() {
        traces_config_builder =
            traces_config_builder.with_cert_pem(traces_tls_cert_pem.unwrap().as_str());
    }

    if traces_tls_key_file.is_some() {
        traces_config_builder =
            traces_config_builder.with_key_file(traces_tls_key_file.unwrap().as_str());
    } else if traces_tls_key_pem.is_some() {
        traces_config_builder =
            traces_config_builder.with_key_pem(traces_tls_key_pem.unwrap().as_str());
    }

    if traces_tls_ca_file.is_some() {
        traces_config_builder =
            traces_config_builder.with_ca_file(traces_tls_ca_file.unwrap().as_str())
    } else if traces_tls_ca_pem.is_some() {
        traces_config_builder =
            traces_config_builder.with_ca_pem(traces_tls_ca_pem.unwrap().as_str())
    }

    traces_config_builder
}

fn build_metrics_config(
    agent: Box<AgentRun>,
    endpoint: Option<&String>,
) -> OTLPExporterMetricsConfig {
    let mut metrics_config_builder = otlp::metrics_config_builder(
        agent
            .otlp_exporter_metrics_endpoint
            .map(|e| Endpoint::Full(e))
            .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone())), // This is only safe to unwrap endpoint here because we ensure that at least one of them is Some in the caller.
        agent
            .otlp_exporter_metrics_protocol
            .unwrap_or(agent.otlp_exporter_protocol)
            .into(),
    )
    .with_tls_skip_verify(
        agent
            .otlp_exporter_metrics_tls_skip_verify
            .unwrap_or(agent.otlp_exporter_tls_skip_verify),
    )
    .with_headers(
        agent
            .otlp_exporter_metrics_custom_headers
            .as_ref()
            .unwrap_or(&agent.otlp_exporter_custom_headers),
    )
    .with_request_timeout(
        agent
            .otlp_exporter_metrics_request_timeout
            .unwrap_or(agent.otlp_exporter_request_timeout)
            .into(),
    )
    .with_max_elapsed_time(
        agent
            .otlp_exporter_metrics_retry_max_elapsed_time
            .unwrap_or(agent.otlp_exporter_retry_max_elapsed_time)
            .into(),
    )
    .with_initial_backoff(
        agent
            .otlp_exporter_metrics_retry_initial_backoff
            .unwrap_or(agent.otlp_exporter_retry_initial_backoff)
            .into(),
    )
    .with_max_backoff(
        agent
            .otlp_exporter_metrics_retry_max_backoff
            .unwrap_or(agent.otlp_exporter_retry_max_backoff)
            .into(),
    )
    .with_compression_encoding(
        agent
            .otlp_exporter_metrics_compression
            .unwrap_or(agent.otlp_exporter_compression)
            .into(),
    );

    let metrics_tls_cert_file = agent
        .otlp_exporter_metrics_cert_group
        .otlp_exporter_metrics_tls_cert_file
        .or(agent.otlp_exporter_cert_group.otlp_exporter_tls_cert_file);
    let metrics_tls_cert_pem = agent
        .otlp_exporter_metrics_cert_group
        .otlp_exporter_metrics_tls_cert_pem
        .or(agent.otlp_exporter_cert_group.otlp_exporter_tls_cert_pem);

    let metrics_tls_key_file = agent
        .otlp_exporter_metrics_key_group
        .otlp_exporter_metrics_tls_key_file
        .or(agent.otlp_exporter_key_group.otlp_exporter_tls_key_file);
    let metrics_tls_key_pem = agent
        .otlp_exporter_metrics_key_group
        .otlp_exporter_metrics_tls_key_pem
        .or(agent.otlp_exporter_key_group.otlp_exporter_tls_key_pem);

    let metrics_tls_ca_file = agent
        .otlp_exporter_metrics_ca_group
        .otlp_exporter_metrics_tls_ca_file
        .or(agent.otlp_exporter_ca_group.otlp_exporter_tls_ca_file);

    let metrics_tls_ca_pem = agent
        .otlp_exporter_metrics_ca_group
        .otlp_exporter_metrics_tls_ca_pem
        .or(agent.otlp_exporter_ca_group.otlp_exporter_tls_ca_pem);

    if metrics_tls_cert_file.is_some() {
        metrics_config_builder =
            metrics_config_builder.with_cert_file(metrics_tls_cert_file.unwrap().as_str());
    } else if metrics_tls_cert_pem.is_some() {
        metrics_config_builder =
            metrics_config_builder.with_cert_pem(metrics_tls_cert_pem.unwrap().as_str());
    }

    if metrics_tls_key_file.is_some() {
        metrics_config_builder =
            metrics_config_builder.with_key_file(metrics_tls_key_file.unwrap().as_str());
    } else if metrics_tls_key_pem.is_some() {
        metrics_config_builder =
            metrics_config_builder.with_key_pem(metrics_tls_key_pem.unwrap().as_str());
    }

    if metrics_tls_ca_file.is_some() {
        metrics_config_builder =
            metrics_config_builder.with_ca_file(metrics_tls_ca_file.unwrap().as_str())
    } else if metrics_tls_ca_pem.is_some() {
        metrics_config_builder =
            metrics_config_builder.with_ca_pem(metrics_tls_ca_pem.unwrap().as_str())
    }

    metrics_config_builder
}

fn build_logs_config(agent: Box<AgentRun>, endpoint: Option<&String>) -> OTLPExporterLogsConfig {
    let mut logs_config_builder = otlp::logs_config_builder(
        agent
            .otlp_exporter_logs_endpoint
            .map(|e| Endpoint::Full(e))
            .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone())), // This is only safe to unwrap endpoint here because we ensure that at least one of them is Some in the caller.
        agent
            .otlp_exporter_logs_protocol
            .unwrap_or(agent.otlp_exporter_protocol)
            .into(),
    )
    .with_tls_skip_verify(
        agent
            .otlp_exporter_logs_tls_skip_verify
            .unwrap_or(agent.otlp_exporter_tls_skip_verify),
    )
    .with_headers(
        agent
            .otlp_exporter_logs_custom_headers
            .as_ref()
            .unwrap_or(&agent.otlp_exporter_custom_headers),
    )
    .with_request_timeout(
        agent
            .otlp_exporter_logs_request_timeout
            .unwrap_or(agent.otlp_exporter_request_timeout)
            .into(),
    )
    .with_max_elapsed_time(
        agent
            .otlp_exporter_logs_retry_max_elapsed_time
            .unwrap_or(agent.otlp_exporter_retry_max_elapsed_time)
            .into(),
    )
    .with_initial_backoff(
        agent
            .otlp_exporter_logs_retry_initial_backoff
            .unwrap_or(agent.otlp_exporter_retry_initial_backoff)
            .into(),
    )
    .with_max_backoff(
        agent
            .otlp_exporter_logs_retry_max_backoff
            .unwrap_or(agent.otlp_exporter_retry_max_backoff)
            .into(),
    )
    .with_compression_encoding(
        agent
            .otlp_exporter_logs_compression
            .unwrap_or(agent.otlp_exporter_compression)
            .into(),
    );

    let logs_tls_cert_file = agent
        .otlp_exporter_logs_cert_group
        .otlp_exporter_logs_tls_cert_file
        .or(agent.otlp_exporter_cert_group.otlp_exporter_tls_cert_file);
    let logs_tls_cert_pem = agent
        .otlp_exporter_logs_cert_group
        .otlp_exporter_logs_tls_cert_pem
        .or(agent.otlp_exporter_cert_group.otlp_exporter_tls_cert_pem);

    let logs_tls_key_file = agent
        .otlp_exporter_logs_key_group
        .otlp_exporter_logs_tls_key_file
        .or(agent.otlp_exporter_key_group.otlp_exporter_tls_key_file);
    let logs_tls_key_pem = agent
        .otlp_exporter_logs_key_group
        .otlp_exporter_logs_tls_key_pem
        .or(agent.otlp_exporter_key_group.otlp_exporter_tls_key_pem);

    let logs_tls_ca_file = agent
        .otlp_exporter_logs_ca_group
        .otlp_exporter_logs_tls_ca_file
        .or(agent.otlp_exporter_ca_group.otlp_exporter_tls_ca_file);

    let logs_tls_ca_pem = agent
        .otlp_exporter_logs_ca_group
        .otlp_exporter_logs_tls_ca_pem
        .or(agent.otlp_exporter_ca_group.otlp_exporter_tls_ca_pem);

    if logs_tls_cert_file.is_some() {
        logs_config_builder =
            logs_config_builder.with_cert_file(logs_tls_cert_file.unwrap().as_str());
    } else if logs_tls_cert_pem.is_some() {
        logs_config_builder =
            logs_config_builder.with_cert_pem(logs_tls_cert_pem.unwrap().as_str());
    }

    if logs_tls_key_file.is_some() {
        logs_config_builder =
            logs_config_builder.with_key_file(logs_tls_key_file.unwrap().as_str());
    } else if logs_tls_key_pem.is_some() {
        logs_config_builder = logs_config_builder.with_key_pem(logs_tls_key_pem.unwrap().as_str());
    }

    if logs_tls_ca_file.is_some() {
        logs_config_builder = logs_config_builder.with_ca_file(logs_tls_ca_file.unwrap().as_str())
    } else if logs_tls_ca_pem.is_some() {
        logs_config_builder = logs_config_builder.with_ca_pem(logs_tls_ca_pem.unwrap().as_str())
    }

    logs_config_builder
}

fn daemonize(pid_file: &String, log_file: &String) -> Result<Option<ExitCode>, Box<dyn Error>> {
    // Do not using tracing logging functions in here, it is not setup until after we daemonize
    let stdout_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(log_file)
        .map_err(|e| format!("failed to open log file: {}: {}", log_file, e))?;
    let stderr_file = stdout_file.try_clone()?;

    let daemonize = daemonize::Daemonize::new()
        .pid_file(pid_file)
        .working_directory(WORKDING_DIR)
        .stdout(stdout_file)
        .stderr(stderr_file);

    match daemonize.start() {
        Ok(_) => Ok(None),
        Err(e) => match e.kind {
            daemonize::ErrorKind::LockPidfile(_) => {
                println!(
                    "Detected existing agent running, if not remove: {}",
                    pid_file
                );
                Ok(Some(ExitCode::SUCCESS))
            }
            _ => Err(e.into()),
        },
    }
}

fn get_hostname() -> Option<String> {
    match gethostname().into_string() {
        Ok(s) => Some(s),
        Err(e) => {
            error!(error = ?e, "Unable to lookup hostname");
            None
        }
    }
}

fn get_version() -> String {
    // Set during CI
    let version_build = option_env!("BUILD_SHORT_SHA").unwrap_or("dev");

    format!("{}-{}", env!("CARGO_PKG_VERSION"), version_build)
}

fn bind_endpoints(
    endpoints: &[SocketAddr],
) -> Result<HashMap<SocketAddr, Listener>, Box<dyn Error + Send + Sync>> {
    endpoints
        .iter()
        .map(|endpoint| match Listener::listen_std(*endpoint) {
            Ok(l) => Ok((*endpoint, l)),
            Err(e) => Err(e),
        })
        .collect()
}

impl From<OTLPExporterProtocol> for Protocol {
    fn from(value: OTLPExporterProtocol) -> Protocol {
        match value {
            OTLPExporterProtocol::Grpc => Protocol::Grpc,
            OTLPExporterProtocol::Http => Protocol::Http,
        }
    }
}

impl From<DatadogRegion> for Region {
    fn from(value: DatadogRegion) -> Self {
        match value {
            DatadogRegion::US1 => Region::US1,
            DatadogRegion::US3 => Region::US3,
            DatadogRegion::US5 => Region::US5,
            DatadogRegion::EU => Region::EU,
            DatadogRegion::AP1 => Region::AP1,
        }
    }
}

#[derive(Default)]
pub struct TelemetryActivation {
    traces: TelemetryState,
    metrics: TelemetryState,
    logs: TelemetryState,
}

#[derive(Default, PartialEq)]
pub enum TelemetryState {
    #[default]
    Active,
    Disabled,
    NoListeners,
}

impl TelemetryActivation {
    fn from_config(config: &AgentRun) -> Self {
        let mut activation = match config.exporter {
            Exporter::Otlp => {
                let has_global_endpoint = config.otlp_exporter_endpoint.is_some();
                let mut activation = TelemetryActivation::default();
                if !has_global_endpoint && config.otlp_exporter_traces_endpoint.is_none() {
                    activation.traces = TelemetryState::NoListeners
                }
                if !has_global_endpoint && config.otlp_exporter_metrics_endpoint.is_none() {
                    activation.metrics = TelemetryState::NoListeners
                }
                if !has_global_endpoint && config.otlp_exporter_logs_endpoint.is_none() {
                    activation.logs = TelemetryState::NoListeners
                }
                activation
            }
            Exporter::Blackhole => TelemetryActivation::default(),
            Exporter::Datadog => {
                // Only supports traces for now
                TelemetryActivation {
                    traces: TelemetryState::Active,
                    metrics: TelemetryState::NoListeners,
                    logs: TelemetryState::NoListeners,
                }
            }
        };

        if config.otlp_receiver_traces_disabled {
            activation.traces = TelemetryState::Disabled
        }
        if config.otlp_receiver_metrics_disabled {
            activation.metrics = TelemetryState::Disabled
        }
        if config.otlp_receiver_logs_disabled {
            activation.logs = TelemetryState::Disabled
        }

        activation
    }
}

// Check the lock status of the PID file to see if another version of Rotel is running.
// TODO: We should likely move this to a healthcheck on a known status port rather then
// use something more OS dependent.
unsafe fn check_rotel_active(pid_path: &String) -> bool {
    fn string_to_cstring(path: &String) -> Result<CString, Box<dyn Error>> {
        CString::new(path.clone()).map_err(|e| format!("path contains null: {e}").into())
    }
    let path_c = match string_to_cstring(pid_path) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("PID path string is invalid: {e}");
            exit(1);
        }
    };

    let ret = unwrap_errno(libc::open(path_c.as_ptr(), libc::O_RDONLY, 0o666));
    if ret.0 < 0 {
        return false;
    }

    let ret = unwrap_errno(libc::flock(ret.0, libc::LOCK_EX | libc::LOCK_NB));

    // Close the original file descriptor
    libc::close(ret.0);

    if ret.0 != 0 {
        // Unknown error from flock
        if ret.1 != 11 {
            eprintln!("Unknown error from pid file check: {}", ret.1)
        }
        // Treat this as if we are running
        return true;
    }

    false
}

type LibcRet = libc::c_int;
type Errno = libc::c_int;
fn unwrap_errno(ret: LibcRet) -> (LibcRet, Errno) {
    if ret >= 0 {
        return (ret, 0);
    }

    let errno = std::io::Error::last_os_error()
        .raw_os_error()
        .expect("errno");
    (ret, errno)
}

#[cfg(test)]
mod test {
    use crate::parse_endpoint;
    use tokio_test::assert_ok;

    #[test]
    fn endpoint_parse() {
        let sa = parse_endpoint("localhost:4317");
        assert_ok!(sa);
        let sa = sa.unwrap();
        assert!(sa.is_ipv4());
        assert_eq!("127.0.0.1", sa.ip().to_string());
        assert_eq!(4317, sa.port());

        let sa = parse_endpoint("[::1]:4317");
        assert_ok!(sa);
        let sa = sa.unwrap();
        assert!(sa.is_ipv6());
        assert_eq!("::1", sa.ip().to_string());

        let sa = parse_endpoint("0.0.0.0:1234");
        assert_ok!(sa);
        let sa = sa.unwrap();
        assert_eq!("0.0.0.0", sa.ip().to_string());
    }
}
