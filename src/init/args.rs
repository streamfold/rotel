use crate::exporters::otlp::Authenticator;
use crate::init::batch::BatchArgs;
use crate::init::clickhouse_exporter::ClickhouseExporterArgs;
use crate::init::datadog_exporter::DatadogExporterArgs;
#[cfg(feature = "file_exporter")]
use crate::init::file_exporter::FileExporterArgs;
#[cfg(feature = "rdkafka")]
use crate::init::kafka_exporter::KafkaExporterArgs;
use crate::init::kafka_receiver::KafkaReceiverArgs;
use crate::init::otlp_exporter::OTLPExporterArgs;
use crate::init::otlp_receiver::OTLPReceiverArgs;
use crate::init::parse;
use crate::init::xray_exporter::XRayExporterArgs;
use crate::topology::debug::DebugVerbosity;
use clap::{Args, ValueEnum};
use serde::Deserialize;
#[cfg(feature = "prometheus")]
use std::net::SocketAddr;
use std::str::FromStr;

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

    /// Prometheus endpoint
    #[arg(long, env = "ROTEL_PROMETHEUS_ENDPOINT", default_value = "localhost:9090", value_parser = parse::parse_endpoint)]
    #[cfg(feature = "prometheus")]
    pub prometheus_endpoint: SocketAddr,

    #[command(flatten)]
    pub otlp_receiver: OTLPReceiverArgs,

    #[command(flatten)]
    #[cfg(feature = "rdkafka")]
    pub kafka_receiver: KafkaReceiverArgs,

    /// Single receiver (type)
    #[arg(value_enum, long, env = "ROTEL_RECEIVER")]
    pub receiver: Option<Receiver>,

    /// Multiple receivers (name:type,...)
    #[arg(value_enum, long, env = "ROTEL_RECEIVERS")]
    pub receivers: Option<String>,

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

    /// Internal metrics exporters
    #[arg(long, env = "ROTEL_EXPORTERS_INTERNAL_METRICS")]
    pub exporters_internal_metrics: Option<String>,

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

    #[command(flatten)]
    #[cfg(feature = "file_exporter")]
    pub file_exporter: FileExporterArgs,

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
            receiver: None,
            receivers: None,
            otlp_receiver: OTLPReceiverArgs::default(),
            #[cfg(feature = "rdkafka")]
            kafka_receiver: KafkaReceiverArgs::default(),
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
            exporters_internal_metrics: None,
            otlp_exporter: OTLPExporterArgs::default(),
            datadog_exporter: DatadogExporterArgs::default(),
            clickhouse_exporter: ClickhouseExporterArgs::default(),
            aws_xray_exporter: XRayExporterArgs::default(),
            aws_emf_exporter: AwsEmfExporterArgs::default(),
            #[cfg(feature = "prometheus")]
            prometheus_endpoint: "127.0.0.1:9090".parse().unwrap(),
            #[cfg(feature = "rdkafka")]
            kafka_exporter: KafkaExporterArgs::default(),
            #[cfg(feature = "file_exporter")]
            file_exporter: FileExporterArgs::default(),
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

    #[clap(name = "awsxray")]
    AwsXray,

    #[clap(name = "awsemf")]
    AwsEmf,

    #[cfg(feature = "rdkafka")]
    Kafka,

    #[cfg(feature = "file_exporter")]
    File,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash, ValueEnum)]
pub enum Receiver {
    Otlp,
    #[cfg(feature = "rdkafka")]
    Kafka,
}

impl FromStr for Receiver {
    type Err = &'static str; // Define an error type
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "otlp" => Ok(Receiver::Otlp),
            "kafka" => Ok(Receiver::Kafka),
            _ => Err("Unknown receiver"),
        }
    }
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
