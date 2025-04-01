use crate::exporters::otlp;
use crate::exporters::otlp::config::{
    OTLPExporterLogsConfig, OTLPExporterMetricsConfig, OTLPExporterTracesConfig,
};
use crate::exporters::otlp::{CompressionEncoding, Endpoint, Protocol};
use crate::init::args;
use crate::init::args::OTLPExporterProtocol;
use crate::topology::batch::BatchConfig;

#[derive(Debug, Clone, clap::Args)]
pub struct OTLPExporterArgs {
    /// OTLP Exporter Endpoint - Used as default for all OTLP data types unless more specific flag specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_ENDPOINT")]
    pub otlp_exporter_endpoint: Option<String>,

    /// OTLP Exporter Traces Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_ENDPOINT")]
    pub otlp_exporter_traces_endpoint: Option<String>,

    /// OTLP Exporter Metrics Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_ENDPOINT")]
    pub otlp_exporter_metrics_endpoint: Option<String>,

    /// OTLP Exporter Logs Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_ENDPOINT")]
    pub otlp_exporter_logs_endpoint: Option<String>,

    /// OTLP Exporter Protocol - Used as default for all OTLP data types unless more specific flag specified
    #[arg(
        value_enum,
        long,
        env = "ROTEL_OTLP_EXPORTER_PROTOCOL",
        default_value = "grpc"
    )]
    pub otlp_exporter_protocol: OTLPExporterProtocol,

    /// OTLP Exporter Traces Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_TRACES_PROTOCOL")]
    pub otlp_exporter_traces_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Metrics Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_METRICS_PROTOCOL")]
    pub otlp_exporter_metrics_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Logs Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_LOGS_PROTOCOL")]
    pub otlp_exporter_logs_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Headers - Used as default for all OTLP data types unless more specific flag specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_CUSTOM_HEADERS", value_parser = args::parse_key_val::<String, String>, value_delimiter = ','
    )]
    pub otlp_exporter_custom_headers: Vec<(String, String)>,

    /// OTLP Exporter Traces Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_CUSTOM_HEADERS", value_parser = args::parse_key_val::<String, String>, value_delimiter = ','
    )]
    pub otlp_exporter_traces_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Metrics Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_CUSTOM_HEADERS", value_parser = args::parse_key_val::<String, String>, value_delimiter = ','
    )]
    pub otlp_exporter_metrics_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Logs Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_CUSTOM_HEADERS", value_parser = args::parse_key_val::<String, String>, value_delimiter = ',')]
    pub otlp_exporter_logs_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Compression - Used as default for all OTLP data types unless more specific flag specified
    #[arg(
        value_enum,
        long,
        env = "ROTEL_OTLP_EXPORTER_COMPRESSION",
        default_value = "gzip"
    )]
    pub otlp_exporter_compression: CompressionEncoding,

    /// OTLP Exporter Traces Compression - Overrides otlp_exporter_compression if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_TRACES_COMPRESSION")]
    pub otlp_exporter_traces_compression: Option<CompressionEncoding>,

    /// OTLP Exporter Metrics Compression - Overrides otlp_exporter_compression if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_METRICS_COMPRESSION")]
    pub otlp_exporter_metrics_compression: Option<CompressionEncoding>,

    /// OTLP Exporter Logs Compression - Overrides otlp_exporter_compression if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_LOGS_COMPRESSION")]
    pub otlp_exporter_logs_compression: Option<CompressionEncoding>,

    #[clap(flatten)]
    pub otlp_exporter_cert_group: CertGroup,

    #[clap(flatten)]
    pub otlp_exporter_key_group: KeyGroup,

    #[clap(flatten)]
    pub otlp_exporter_ca_group: CaGroup,

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
    pub otlp_exporter_tls_skip_verify: bool,

    /// OTLP Exporter traces TLS SKIP VERIFY - Overrides otlp_exporter_tls_skip_verify for OTLP traces if specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_TLS_SKIP_VERIFY")]
    pub otlp_exporter_traces_tls_skip_verify: Option<bool>,

    /// OTLP Exporter metrics TLS SKIP VERIFY - Overrides otlp_exporter_tls_skip_verify for OTLP metrics if specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_TLS_SKIP_VERIFY")]
    pub otlp_exporter_metrics_tls_skip_verify: Option<bool>,

    /// OTLP Exporter logs TLS SKIP VERIFY - Overrides otlp_exporter_tls_skip_verify for OTLP logs if specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_TLS_SKIP_VERIFY")]
    pub otlp_exporter_logs_tls_skip_verify: Option<bool>,

    /// OTLP Exporter Request Timeout - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_REQUEST_TIMEOUT",
        default_value = "5s"
    )]
    pub otlp_exporter_request_timeout: humantime::Duration,

    /// OTLP Exporter traces Request Timeout - Overrides otlp_exporter_request_timeout for OTLP traces if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_REQUEST_TIMEOUT")]
    pub otlp_exporter_traces_request_timeout: Option<humantime::Duration>,

    /// OTLP Exporter metrics Request Timeout - Overrides otlp_exporter_request_timeout for OTLP metrics if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_REQUEST_TIMEOUT")]
    pub otlp_exporter_metrics_request_timeout: Option<humantime::Duration>,

    /// OTLP Exporter logs Request Timeout - Overrides otlp_exporter_request_timeout for OTLP logs if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_REQUEST_TIMEOUT")]
    pub otlp_exporter_logs_request_timeout: Option<humantime::Duration>,

    /// OTLP Exporter Retry initial backoff - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_RETRY_INITIAL_BACKOFF",
        default_value = "5s"
    )]
    pub otlp_exporter_retry_initial_backoff: humantime::Duration,

    /// OTLP Exporter traces Retry initial backoff - Overrides otlp_exporter_retry_initial_backoff for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_INITIAL_BACKOFF")]
    pub otlp_exporter_traces_retry_initial_backoff: Option<humantime::Duration>,

    /// OTLP Exporter metrics Retry initial backoff  - Overrides otlp_exporter_retry_initial_backoff for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_INITIAL_BACKOFF")]
    pub otlp_exporter_metrics_retry_initial_backoff: Option<humantime::Duration>,

    /// OTLP Exporter logs Retry initial backoff  - Overrides otlp_exporter_retry_initial_backoff for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_INITIAL_BACKOFF")]
    pub otlp_exporter_logs_retry_initial_backoff: Option<humantime::Duration>,

    /// OTLP Exporter Retry max backoff - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_RETRY_MAX_BACKOFF",
        default_value = "30s"
    )]
    pub otlp_exporter_retry_max_backoff: humantime::Duration,

    /// OTLP Exporter traces Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_MAX_BACKOFF")]
    pub otlp_exporter_traces_retry_max_backoff: Option<humantime::Duration>,

    /// OTLP Exporter metrics Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_MAX_BACKOFF")]
    pub otlp_exporter_metrics_retry_max_backoff: Option<humantime::Duration>,

    /// OTLP Exporter logs Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_MAX_BACKOFF")]
    pub otlp_exporter_logs_retry_max_backoff: Option<humantime::Duration>,

    /// OTLP Exporter Retry max elapsed time - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_RETRY_MAX_ELAPSED_TIME",
        default_value = "300s"
    )]
    pub otlp_exporter_retry_max_elapsed_time: humantime::Duration,

    /// OTLP Exporter traces Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_MAX_ELAPSED_TIME")]
    pub otlp_exporter_traces_retry_max_elapsed_time: Option<humantime::Duration>,

    /// OTLP Exporter metrics Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_MAX_ELAPSED_TIME")]
    pub otlp_exporter_metrics_retry_max_elapsed_time: Option<humantime::Duration>,

    /// OTLP Exporter logs Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_MAX_ELAPSED_TIME")]
    pub otlp_exporter_logs_retry_max_elapsed_time: Option<humantime::Duration>,

    // Batch settings
    /// OTLP Exporter max batch size in number of spans/metrics - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_BATCH_MAX_SIZE",
        default_value = "8192"
    )]
    pub otlp_exporter_batch_max_size: usize,

    /// OTLP Exporter traces max batch size in number of spans - Overrides otlp_exporter_batch_max_size for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_BATCH_MAX_SIZE")]
    pub otlp_exporter_traces_batch_max_size: Option<usize>,

    /// OTLP Exporter metrics max batch size in number of metrics - Overrides otlp_exporter_batch_max_size for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_BATCH_MAX_SIZE")]
    pub otlp_exporter_metrics_batch_max_size: Option<usize>,

    /// OTLP Exporter logs max batch size in number of logs - Overrides otlp_exporter_batch_max_size for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_BATCH_MAX_SIZE")]
    pub otlp_exporter_logs_batch_max_size: Option<usize>,

    /// OTLP Exporter batch timeout - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long,
        env = "ROTEL_OTLP_EXPORTER_BATCH_TIMEOUT",
        default_value = "200ms"
    )]
    pub otlp_exporter_batch_timeout: humantime::Duration,

    /// OTLP Exporter traces batch timeout - Overrides otlp_exporter_batch_timeout for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_BATCH_TIMEOUT")]
    pub otlp_exporter_traces_batch_timeout: Option<humantime::Duration>,

    /// OTLP Exporter metrics batch timeout - Overrides otlp_exporter_batch_timeout for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_BATCH_TIMEOUT")]
    pub otlp_exporter_metrics_batch_timeout: Option<humantime::Duration>,

    /// OTLP Exporter logs batch timeout - Overrides otlp_exporter_batch_timeout for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_BATCH_TIMEOUT")]
    pub otlp_exporter_logs_batch_timeout: Option<humantime::Duration>,
}

impl From<OTLPExporterProtocol> for Protocol {
    fn from(value: OTLPExporterProtocol) -> Protocol {
        match value {
            OTLPExporterProtocol::Grpc => Protocol::Grpc,
            OTLPExporterProtocol::Http => Protocol::Http,
        }
    }
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

// todo: add these as impl functions of the exporter args?
pub fn build_traces_batch_config(agent: OTLPExporterArgs) -> BatchConfig {
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

pub fn build_metrics_batch_config(agent: OTLPExporterArgs) -> BatchConfig {
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

pub fn build_logs_batch_config(agent: OTLPExporterArgs) -> BatchConfig {
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

pub fn build_traces_config(
    agent: OTLPExporterArgs,
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

pub fn build_metrics_config(
    agent: OTLPExporterArgs,
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

pub fn build_logs_config(
    agent: OTLPExporterArgs,
    endpoint: Option<&String>,
) -> OTLPExporterLogsConfig {
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
