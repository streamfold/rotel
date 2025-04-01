use crate::init::datadog_exporter::DatadogExporterArgs;
use crate::init::otlp_exporter::OTLPExporterArgs;
use clap::{Args, ValueEnum};
use std::error::Error;
use std::net::SocketAddr;

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

    /// Exporter
    #[arg(value_enum, long, env = "ROTEL_EXPORTER", default_value = "otlp")]
    pub exporter: Exporter,

    #[command(flatten)]
    pub otlp_exporter: OTLPExporterArgs,

    #[command(flatten)]
    pub datadog_exporter: DatadogExporterArgs,

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
