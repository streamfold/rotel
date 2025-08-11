use crate::exporters::otlp::Authenticator;
use crate::init::batch::BatchArgs;
use crate::init::clickhouse_exporter::ClickhouseExporterArgs;
use crate::init::datadog_exporter::DatadogExporterArgs;
#[cfg(feature = "rdkafka")]
use crate::init::kafka_exporter::KafkaExporterArgs;
use crate::init::otlp_exporter::OTLPExporterArgs;
use crate::init::parse;
use crate::init::xray_exporter::XRayExporterArgs;
use crate::topology::debug::DebugVerbosity;
use clap::{Args, ValueEnum};
use serde::Deserialize;
use std::net::SocketAddr;

use super::awsemf_exporter::AwsEmfExporterArgs;

#[derive(Debug, Args, Clone)]
pub struct AgentRun {
    /// Daemonize
    #[arg(long, env = "ROTEL_DAEMON", default_value = "false")]
    pub daemon: bool,

    /// PID file
    #[arg(long, env = "ROTEL_PID_FILE", default_value = "/tmp/rotel-agent.pid")]
    pub pid_file: String,

    /// Log file
    #[arg(long, env = "ROTEL_LOG_FILE", default_value = "/tmp/rotel-agent.log")]
    pub log_file: String,

    /// Debug log
    #[arg(value_enum, long, env = "ROTEL_DEBUG_LOG", default_value = "none")]
    pub debug_log: Vec<DebugLogParam>,

    /// Debug log verbosity
    #[arg(
        value_enum,
        long,
        env = "ROTEL_DEBUG_LOG_VERBOSITY",
        default_value = "basic"
    )]
    pub debug_log_verbosity: DebugLogVerbosity,

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

    #[arg(long, env = "ROTEL_OTLP_WITH_TRACE_PROCESSOR", action = clap::ArgAction::Append, value_delimiter = ',')]
    pub otlp_with_trace_processor: Vec<String>,

    #[arg(long, env = "ROTEL_OTLP_WITH_LOGS_PROCESSOR", action = clap::ArgAction::Append, value_delimiter = ',')]
    pub otlp_with_logs_processor: Vec<String>,

    #[arg(long, env = "ROTEL_OTLP_WITH_METRICS_PROCESSOR", action = clap::ArgAction::Append, value_delimiter = ',')]
    pub otlp_with_metrics_processor: Vec<String>,

    /// Comma-separated, key=value pairs of resource attributes to set
    #[arg(long, env = "ROTEL_OTEL_RESOURCE_ATTRIBUTES", value_parser = parse::parse_key_val::<String, String>, value_delimiter = ',')]
    pub otel_resource_attributes: Vec<(String, String)>,

    /// Enable reporting of internal telemetry
    #[arg(long, env = "ROTEL_ENABLE_INTERNAL_TELEMETRY", default_value = "false")]
    pub enable_internal_telemetry: bool,

    #[command(flatten)]
    pub batch: BatchArgs,

    /// Single exporter (type)
    #[arg(value_enum, long, env = "ROTEL_EXPORTER")]
    pub exporter: Option<Exporter>,

    /// Multiple exporters (name:type,...)
    #[arg(value_enum, long, env = "ROTEL_EXPORTERS")]
    pub exporters: Option<String>,

    /// Traces exporters
    #[arg(long, env = "ROTEL_EXPORTERS_TRACES")]
    pub exporters_traces: Option<String>,

    /// Metrics exporters
    #[arg(long, env = "ROTEL_EXPORTERS_METRICS")]
    pub exporters_metrics: Option<String>,

    /// Logs exporters
    #[arg(long, env = "ROTEL_EXPORTERS_LOGS")]
    pub exporters_logs: Option<String>,

    #[command(flatten)]
    pub otlp_exporter: OTLPExporterArgs,

    #[command(flatten)]
    pub datadog_exporter: DatadogExporterArgs,

    #[command(flatten)]
    pub clickhouse_exporter: ClickhouseExporterArgs,

    #[command(flatten)]
    pub aws_xray_exporter: XRayExporterArgs,

    #[command(flatten)]
    pub aws_emf_exporter: AwsEmfExporterArgs,

    #[command(flatten)]
    #[cfg(feature = "rdkafka")]
    pub kafka_exporter: KafkaExporterArgs,

    #[cfg(feature = "pprof")]
    #[clap(flatten)]
    pub profile_group: ProfileGroup,
}

impl Default for AgentRun {
    fn default() -> Self {
        AgentRun {
            daemon: false,
            pid_file: "/tmp/rotel-agent.pid".to_string(),
            log_file: "/tmp/rotel-agent.log".to_string(),
            debug_log: vec![DebugLogParam::None],
            debug_log_verbosity: DebugLogVerbosity::Basic,
            otlp_grpc_endpoint: "127.0.0.1:4317".parse().unwrap(),
            otlp_http_endpoint: "127.0.0.1:4318".parse().unwrap(),
            otlp_grpc_max_recv_msg_size_mib: 4,
            otlp_receiver_traces_disabled: false,
            otlp_receiver_metrics_disabled: false,
            otlp_receiver_logs_disabled: false,
            otlp_receiver_traces_http_path: "/v1/traces".to_string(),
            otlp_receiver_metrics_http_path: "/v1/metrics".to_string(),
            otlp_receiver_logs_http_path: "/v1/logs".to_string(),
            otlp_with_trace_processor: Vec::new(),
            otlp_with_logs_processor: Vec::new(),
            otlp_with_metrics_processor: Vec::new(),
            otel_resource_attributes: Vec::new(),
            enable_internal_telemetry: false,
            batch: BatchArgs::default(),
            exporter: None,
            exporters: None,
            exporters_traces: None,
            exporters_metrics: None,
            exporters_logs: None,
            otlp_exporter: OTLPExporterArgs::default(),
            datadog_exporter: DatadogExporterArgs::default(),
            clickhouse_exporter: ClickhouseExporterArgs::default(),
            aws_xray_exporter: XRayExporterArgs::default(),
            aws_emf_exporter: AwsEmfExporterArgs::default(),
            #[cfg(feature = "rdkafka")]
            kafka_exporter: KafkaExporterArgs::default(),
            #[cfg(feature = "pprof")]
            profile_group: ProfileGroup {
                pprof_flame_graph: false,
                pprof_call_graph: false,
            },
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, ValueEnum)]
pub enum DebugLogParam {
    None,
    Traces,
    Metrics,
    Logs,
}

#[derive(Copy, Clone, PartialEq, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum OTLPExporterProtocol {
    Grpc,
    Http,
}

#[derive(Copy, Clone, PartialEq, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum OTLPExporterAuthenticator {
    Sigv4auth,
}

impl From<OTLPExporterAuthenticator> for Authenticator {
    fn from(value: OTLPExporterAuthenticator) -> Self {
        match value {
            OTLPExporterAuthenticator::Sigv4auth => Authenticator::Sigv4auth,
        }
    }
}

#[derive(Debug, clap::Args)]
#[group(required = false, multiple = false)]
#[derive(Clone)]
pub struct ProfileGroup {
    #[arg(long, env = "ROTEL_PPROF_FLAME_GRAPH", default_value = "false")]
    pub(crate) pprof_flame_graph: bool,

    #[arg(long, env = "ROTEL_PPROF_CALL_GRAPH", default_value = "false")]
    pub(crate) pprof_call_graph: bool,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum Exporter {
    Otlp,

    Blackhole,

    Datadog,

    Clickhouse,

    AwsXray,
    
    AwsEmf,

    #[cfg(feature = "rdkafka")]
    Kafka,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
pub enum DebugLogVerbosity {
    Basic,
    Detailed,
}

impl From<DebugLogVerbosity> for DebugVerbosity {
    fn from(value: DebugLogVerbosity) -> Self {
        match value {
            DebugLogVerbosity::Basic => DebugVerbosity::Basic,
            DebugLogVerbosity::Detailed => DebugVerbosity::Detailed,
        }
    }
}
