use crate::exporters::otlp::Authenticator;
use crate::init::batch::BatchArgs;
use crate::init::clickhouse_exporter::ClickhouseExporterArgs;
use crate::init::datadog_exporter::DatadogExporterArgs;
use crate::init::kafka_exporter::KafkaExporterArgs;
use crate::init::otlp_exporter::OTLPExporterArgs;
use crate::init::xray_exporter::XRayExporterArgs;
use crate::topology::debug::DebugVerbosity;
use clap::{Args, ValueEnum};
use std::error::Error;
use std::net::SocketAddr;
use tower::BoxError;

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
    #[arg(long, env = "ROTEL_OTLP_GRPC_ENDPOINT", default_value = "localhost:4317", value_parser = parse_endpoint
    )]
    pub otlp_grpc_endpoint: SocketAddr,

    /// OTLP HTTP endpoint
    #[arg(long, env = "ROTEL_OTLP_HTTP_ENDPOINT", default_value = "localhost:4318", value_parser = parse_endpoint
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

    #[arg(long, env = "ROTEL_OTLP_WITH_TRACE_PROCESSOR", action = clap::ArgAction::Append)]
    pub otlp_with_trace_processor: Vec<String>,

    #[arg(long, env = "ROTEL_OTLP_WITH_LOGS_PROCESSOR", action = clap::ArgAction::Append)]
    pub otlp_with_logs_processor: Vec<String>,

    #[arg(long, env = "ROTEL_OTLP_WITH_METRICS_PROCESSOR", action = clap::ArgAction::Append)]
    pub otlp_with_metrics_processor: Vec<String>,

    /// Comma-separated, key=value pairs of resource attributes to set
    #[arg(long, env = "ROTEL_OTEL_RESOURCE_ATTRIBUTES", value_parser = parse_key_val::<String, String>, value_delimiter = ',')]
    pub otel_resource_attributes: Vec<(String, String)>,

    /// Enable reporting of internal telemetry
    #[arg(long, env = "ROTEL_ENABLE_INTERNAL_TELEMETRY", default_value = "false")]
    pub enable_internal_telemetry: bool,

    #[command(flatten)]
    pub batch: BatchArgs,

    /// Exporter
    #[arg(value_enum, long, env = "ROTEL_EXPORTER", default_value = "otlp")]
    pub exporter: Exporter,

    #[command(flatten)]
    pub otlp_exporter: OTLPExporterArgs,

    #[command(flatten)]
    pub datadog_exporter: DatadogExporterArgs,

    #[command(flatten)]
    pub clickhouse_exporter: ClickhouseExporterArgs,

    #[command(flatten)]
    pub aws_xray_exporter: XRayExporterArgs,

    #[command(flatten)]
    pub kafka_exporter: KafkaExporterArgs,

    #[cfg(feature = "pprof")]
    #[clap(flatten)]
    pub profile_group: ProfileGroup,
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

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, ValueEnum)]
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

/// Parse a single key-value pair
pub fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
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
pub fn parse_endpoint(s: &str) -> Result<SocketAddr, Box<dyn Error + Send + Sync + 'static>> {
    // Use actual localhost address instead of localhost name
    let s = if s.starts_with("localhost:") {
        s.replace("localhost:", "127.0.0.1:")
    } else {
        s.to_string()
    };
    let sa: SocketAddr = s.parse()?;
    Ok(sa)
}

#[cfg(test)]
mod test {
    use crate::init::args::parse_endpoint;
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

pub(crate) fn parse_bool_value(val: &String) -> Result<bool, BoxError> {
    match val.to_lowercase().as_str() {
        "0" | "false" => Ok(false),
        "1" | "true" => Ok(true),
        _ => Err(format!("Unable to parse bool value: {}", val).into()),
    }
}
