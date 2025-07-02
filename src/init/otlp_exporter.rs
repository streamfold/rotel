use crate::exporters::otlp;
use crate::exporters::otlp::config::OTLPExporterConfig;
use crate::exporters::otlp::{CompressionEncoding, Endpoint, Protocol};
use crate::init::args::{OTLPExporterAuthenticator, OTLPExporterProtocol};
use crate::init::parse;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, clap::Args, Clone, Deserialize)]
#[serde(default)]
pub struct OTLPExporterBaseArgs {
    /// OTLP Exporter Endpoint - Used as default for all OTLP data types unless more specific flag specified
    #[arg(long("otlp-exporter-endpoint"), env = "ROTEL_OTLP_EXPORTER_ENDPOINT")]
    pub endpoint: Option<String>,

    //
    // These are broken out in the base definition because the HTTP
    // exporter may have a different endpoint per telemetry type.
    //
    /// OTLP Exporter Traces Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(
        long("otlp-exporter-traces-endpoint"),
        env = "ROTEL_OTLP_EXPORTER_TRACES_ENDPOINT"
    )]
    pub traces_endpoint: Option<String>,

    /// OTLP Exporter Metrics Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(
        long("otlp-exporter-metrics-endpoint"),
        env = "ROTEL_OTLP_EXPORTER_METRICS_ENDPOINT"
    )]
    pub metrics_endpoint: Option<String>,

    /// OTLP Exporter Logs Endpoint - Overrides otlp_exporter_endpoint if specified
    #[arg(
        long("otlp-exporter-logs-endpoint"),
        env = "ROTEL_OTLP_EXPORTER_LOGS_ENDPOINT"
    )]
    pub logs_endpoint: Option<String>,

    /// OTLP Exporter Protocol - Used as default for all OTLP data types unless more specific flag specified
    #[arg(
        value_enum,
        long("otlp-exporter-protocol"),
        env = "ROTEL_OTLP_EXPORTER_PROTOCOL",
        default_value = "grpc"
    )]
    pub protocol: OTLPExporterProtocol,

    /// OTLP Exporter authenticator
    #[arg(
        value_enum,
        long("otlp-exporter-authenticator"),
        env = "ROTEL_OTLP_EXPORTER_AUTHENTICATOR"
    )]
    pub authenticator: Option<OTLPExporterAuthenticator>,

    /// OTLP Exporter Headers - Used as default for all OTLP data types unless more specific flag specified
    #[arg(long("otlp-exporter-custom-headers"), env = "ROTEL_OTLP_EXPORTER_CUSTOM_HEADERS", value_parser = parse::parse_key_val::<String, String>, value_delimiter = ','
    )]
    #[serde(deserialize_with = "parse::deserialize_key_value_pairs")]
    pub custom_headers: Vec<(String, String)>,

    /// OTLP Exporter Compression - Used as default for all OTLP data types unless more specific flag specified
    #[arg(
        value_enum,
        long("otlp-exporter-compression"),
        env = "ROTEL_OTLP_EXPORTER_COMPRESSION",
        default_value = "gzip"
    )]
    pub compression: CompressionEncoding,

    #[clap(flatten)]
    #[serde(flatten)]
    pub cert_group: CertGroup,

    #[clap(flatten)]
    #[serde(flatten)]
    pub key_group: KeyGroup,

    #[clap(flatten)]
    #[serde(flatten)]
    pub ca_group: CaGroup,

    /// OTLP Exporter TLS SKIP VERIFY - Used as default for all OTLP data types unless more specific flag specified
    /// THIS SHOULD ONLY BE USED IN SITUATIONS WHERE YOU ABSOLUTELY NEED TO BYPASS SSL CERTIFICATE VERIFICATION FOR TESTING PURPOSES OR WHEN CONNECTING TO A SERVER WITH A SELF-SIGNED CERTIFICATE THAT YOU FULLY TRUST!!!
    #[arg(
        long("otlp-exporter-tls-skip-verify"),
        env = "ROTEL_OTLP_EXPORTER_TLS_SKIP_VERIFY",
        default_value = "false"
    )]
    pub tls_skip_verify: bool,

    /// OTLP Exporter Request Timeout - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long("otlp-exporter-request-timeout"),
        env = "ROTEL_OTLP_EXPORTER_REQUEST_TIMEOUT",
        default_value = "5s",
        value_parser = humantime::parse_duration,
    )]
    pub request_timeout: std::time::Duration,

    /// OTLP Exporter Retry initial backoff - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long("otlp-exporter-retry-initial-backoff"),
        env = "ROTEL_OTLP_EXPORTER_RETRY_INITIAL_BACKOFF",
        default_value = "5s",
        value_parser = humantime::parse_duration,
    )]
    pub retry_initial_backoff: std::time::Duration,

    /// OTLP Exporter Retry max backoff - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long("otlp-exporter-retry-max-backoff"),
        env = "ROTEL_OTLP_EXPORTER_RETRY_MAX_BACKOFF",
        default_value = "30s",
        value_parser = humantime::parse_duration,
    )]
    pub retry_max_backoff: std::time::Duration,

    /// OTLP Exporter Retry max elapsed time - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(
        long("otlp-exporter-retry-max-elapsed-time"),
        env = "ROTEL_OTLP_EXPORTER_RETRY_MAX_ELAPSED_TIME",
        default_value = "300s",
        value_parser = humantime::parse_duration,
    )]
    pub retry_max_elapsed_time: std::time::Duration,
}

impl Default for OTLPExporterBaseArgs {
    fn default() -> Self {
        Self {
            endpoint: None,
            traces_endpoint: None,
            metrics_endpoint: None,
            logs_endpoint: None,
            protocol: OTLPExporterProtocol::Grpc,
            authenticator: None,
            custom_headers: vec![],
            compression: CompressionEncoding::Gzip,
            cert_group: CertGroup {
                tls_cert_file: None,
                tls_cert_pem: None,
            },
            key_group: KeyGroup {
                tls_key_file: None,
                tls_key_pem: None,
            },
            ca_group: CaGroup {
                tls_ca_file: None,
                tls_ca_pem: None,
            },
            tls_skip_verify: false,
            request_timeout: Duration::from_secs(5),
            retry_initial_backoff: Duration::from_secs(5),
            retry_max_backoff: Duration::from_secs(30),
            retry_max_elapsed_time: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct OTLPExporterArgs {
    #[clap(flatten)]
    pub base: OTLPExporterBaseArgs,

    /// OTLP Exporter Traces Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_TRACES_PROTOCOL")]
    pub otlp_exporter_traces_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Metrics Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_METRICS_PROTOCOL")]
    pub otlp_exporter_metrics_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Logs Protocol - Overrides otlp_exporter_protocol if specified
    #[arg(value_enum, long, env = "ROTEL_OTLP_EXPORTER_LOGS_PROTOCOL")]
    pub otlp_exporter_logs_protocol: Option<OTLPExporterProtocol>,

    /// OTLP Exporter Traces Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_CUSTOM_HEADERS", value_parser = parse::parse_key_val::<String, String>, value_delimiter = ','
    )]
    pub otlp_exporter_traces_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Metrics Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_CUSTOM_HEADERS", value_parser = parse::parse_key_val::<String, String>, value_delimiter = ','
    )]
    pub otlp_exporter_metrics_custom_headers: Option<Vec<(String, String)>>,

    /// OTLP Exporter Logs Headers - Overrides otlp_exporter_custom_headers if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_CUSTOM_HEADERS", value_parser = parse::parse_key_val::<String, String>, value_delimiter = ',')]
    pub otlp_exporter_logs_custom_headers: Option<Vec<(String, String)>>,

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

    /// OTLP Exporter traces Request Timeout - Overrides otlp_exporter_request_timeout for OTLP traces if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_REQUEST_TIMEOUT",
        value_parser = humantime::parse_duration)]
    pub otlp_exporter_traces_request_timeout: Option<std::time::Duration>,

    /// OTLP Exporter metrics Request Timeout - Overrides otlp_exporter_request_timeout for OTLP metrics if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_REQUEST_TIMEOUT",
        value_parser = humantime::parse_duration)]
    pub otlp_exporter_metrics_request_timeout: Option<std::time::Duration>,

    /// OTLP Exporter logs Request Timeout - Overrides otlp_exporter_request_timeout for OTLP logs if specified
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_REQUEST_TIMEOUT",
        value_parser = humantime::parse_duration)]
    pub otlp_exporter_logs_request_timeout: Option<std::time::Duration>,

    /// OTLP Exporter traces Retry initial backoff - Overrides otlp_exporter_retry_initial_backoff for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_INITIAL_BACKOFF",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_traces_retry_initial_backoff: Option<std::time::Duration>,

    /// OTLP Exporter metrics Retry initial backoff  - Overrides otlp_exporter_retry_initial_backoff for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_INITIAL_BACKOFF",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_metrics_retry_initial_backoff: Option<std::time::Duration>,

    /// OTLP Exporter logs Retry initial backoff  - Overrides otlp_exporter_retry_initial_backoff for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_INITIAL_BACKOFF",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_logs_retry_initial_backoff: Option<std::time::Duration>,

    /// OTLP Exporter traces Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_MAX_BACKOFF",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_traces_retry_max_backoff: Option<std::time::Duration>,

    /// OTLP Exporter metrics Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_MAX_BACKOFF",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_metrics_retry_max_backoff: Option<std::time::Duration>,

    /// OTLP Exporter logs Retry max backoff - Overrides otlp_exporter_retry_max_backoff for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_MAX_BACKOFF",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_logs_retry_max_backoff: Option<std::time::Duration>,

    /// OTLP Exporter traces Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP traces if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_TRACES_RETRY_MAX_ELAPSED_TIME",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_traces_retry_max_elapsed_time: Option<std::time::Duration>,

    /// OTLP Exporter metrics Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_METRICS_RETRY_MAX_ELAPSED_TIME",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_metrics_retry_max_elapsed_time: Option<std::time::Duration>,

    /// OTLP Exporter logs Retry max elapsed time - Overrides otlp_exporter_retry_max_elapsed_time for OTLP logs if specified.
    #[arg(long, env = "ROTEL_OTLP_EXPORTER_LOGS_RETRY_MAX_ELAPSED_TIME",
        value_parser = humantime::parse_duration,)]
    pub otlp_exporter_logs_retry_max_elapsed_time: Option<std::time::Duration>,
}

impl Default for OTLPExporterArgs {
    fn default() -> Self {
        Self {
            base: OTLPExporterBaseArgs::default(),
            otlp_exporter_traces_protocol: None,
            otlp_exporter_metrics_protocol: None,
            otlp_exporter_logs_protocol: None,
            otlp_exporter_traces_custom_headers: None,
            otlp_exporter_metrics_custom_headers: None,
            otlp_exporter_logs_custom_headers: None,
            otlp_exporter_traces_compression: None,
            otlp_exporter_metrics_compression: None,
            otlp_exporter_logs_compression: None,
            otlp_exporter_traces_cert_group: TracesCertGroup {
                otlp_exporter_traces_tls_cert_file: None,
                otlp_exporter_traces_tls_cert_pem: None,
            },
            otlp_exporter_traces_key_group: TracesKeyGroup {
                otlp_exporter_traces_tls_key_file: None,
                otlp_exporter_traces_tls_key_pem: None,
            },
            otlp_exporter_traces_ca_group: TracesCaGroup {
                otlp_exporter_traces_tls_ca_file: None,
                otlp_exporter_traces_tls_ca_pem: None,
            },
            otlp_exporter_metrics_cert_group: MetricsCertGroup {
                otlp_exporter_metrics_tls_cert_file: None,
                otlp_exporter_metrics_tls_cert_pem: None,
            },
            otlp_exporter_metrics_key_group: MetricsKeyGroup {
                otlp_exporter_metrics_tls_key_file: None,
                otlp_exporter_metrics_tls_key_pem: None,
            },
            otlp_exporter_metrics_ca_group: MetricsCaGroup {
                otlp_exporter_metrics_tls_ca_file: None,
                otlp_exporter_metrics_tls_ca_pem: None,
            },
            otlp_exporter_logs_cert_group: LogsCertGroup {
                otlp_exporter_logs_tls_cert_file: None,
                otlp_exporter_logs_tls_cert_pem: None,
            },
            otlp_exporter_logs_key_group: LogsKeyGroup {
                otlp_exporter_logs_tls_key_file: None,
                otlp_exporter_logs_tls_key_pem: None,
            },
            otlp_exporter_logs_ca_group: LogsCaGroup {
                otlp_exporter_logs_tls_ca_file: None,
                otlp_exporter_logs_tls_ca_pem: None,
            },
            otlp_exporter_traces_tls_skip_verify: None,
            otlp_exporter_metrics_tls_skip_verify: None,
            otlp_exporter_logs_tls_skip_verify: None,
            otlp_exporter_traces_request_timeout: None,
            otlp_exporter_metrics_request_timeout: None,
            otlp_exporter_logs_request_timeout: None,
            otlp_exporter_traces_retry_initial_backoff: None,
            otlp_exporter_metrics_retry_initial_backoff: None,
            otlp_exporter_logs_retry_initial_backoff: None,
            otlp_exporter_traces_retry_max_backoff: None,
            otlp_exporter_metrics_retry_max_backoff: None,
            otlp_exporter_logs_retry_max_backoff: None,
            otlp_exporter_traces_retry_max_elapsed_time: None,
            otlp_exporter_metrics_retry_max_elapsed_time: None,
            otlp_exporter_logs_retry_max_elapsed_time: None,
        }
    }
}

impl From<OTLPExporterProtocol> for Protocol {
    fn from(value: OTLPExporterProtocol) -> Protocol {
        match value {
            OTLPExporterProtocol::Grpc => Protocol::Grpc,
            OTLPExporterProtocol::Http => Protocol::Http,
        }
    }
}

#[derive(Debug, clap::Args, Clone, Deserialize)]
#[group(required = false, multiple = false)]
pub struct CertGroup {
    #[arg(long("otlp-exporter-tls-cert-file"), env = "ROTEL_OTLP_EXPORTER_TLS_CERT_FILE", default_value = None)]
    tls_cert_file: Option<String>,

    #[arg(long("otlp-exporter-tls-cert-pem"), env = "ROTEL_OTLP_EXPORTER_TLS_CERT_PEM", default_value = None)]
    tls_cert_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone, Deserialize)]
#[group(required = false, multiple = false)]
pub struct KeyGroup {
    #[arg(long("otlp-exporter-tls-key-file"), env = "ROTEL_OTLP_EXPORTER_TLS_KEY_FILE", default_value = None)]
    tls_key_file: Option<String>,

    #[arg(long("otlp-exporter-tls-key-pem"), env = "ROTEL_OTLP_EXPORTER_TLS_KEY_PEM", default_value = None)]
    tls_key_pem: Option<String>,
}

#[derive(Debug, clap::Args, Clone, Deserialize)]
#[group(required = false, multiple = false)]
pub struct CaGroup {
    #[arg(long("otlp-exporter-tls-ca-file"), env = "ROTEL_OTLP_EXPORTER_TLS_CA_FILE", default_value = None)]
    tls_ca_file: Option<String>,

    #[arg(long("otlp-exporter-tls-ca-pem"), env = "ROTEL_OTLP_EXPORTER_TLS_CA_PEM", default_value = None)]
    tls_ca_pem: Option<String>,
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

pub fn build_traces_config(agent: OTLPExporterArgs) -> OTLPExporterBaseArgs {
    let mut config = agent.base;

    if let Some(protocol) = agent.otlp_exporter_traces_protocol {
        config.protocol = protocol
    }
    if let Some(headers) = agent.otlp_exporter_traces_custom_headers {
        config.custom_headers = headers
    }
    if let Some(compression) = agent.otlp_exporter_traces_compression {
        config.compression = compression
    }
    if let Some(tls_skip_verify) = agent.otlp_exporter_traces_tls_skip_verify {
        config.tls_skip_verify = tls_skip_verify
    }
    if let Some(request_time) = agent.otlp_exporter_traces_request_timeout {
        config.request_timeout = request_time
    }
    if let Some(max_elapsed_time) = agent.otlp_exporter_traces_retry_max_elapsed_time {
        config.retry_max_elapsed_time = max_elapsed_time
    }
    if let Some(initial_backoff) = agent.otlp_exporter_traces_retry_initial_backoff {
        config.retry_initial_backoff = initial_backoff
    }
    if let Some(max_backoff) = agent.otlp_exporter_traces_retry_max_backoff {
        config.retry_max_backoff = max_backoff
    }
    if let Some(tls_cert_file) = agent
        .otlp_exporter_traces_cert_group
        .otlp_exporter_traces_tls_cert_file
    {
        config.cert_group.tls_cert_file = Some(tls_cert_file)
    }
    if let Some(tls_cert_pem) = agent
        .otlp_exporter_traces_cert_group
        .otlp_exporter_traces_tls_cert_pem
    {
        config.cert_group.tls_cert_pem = Some(tls_cert_pem)
    }
    if let Some(tls_key_file) = agent
        .otlp_exporter_traces_key_group
        .otlp_exporter_traces_tls_key_file
    {
        config.key_group.tls_key_file = Some(tls_key_file)
    }
    if let Some(tls_key_pem) = agent
        .otlp_exporter_traces_key_group
        .otlp_exporter_traces_tls_key_pem
    {
        config.key_group.tls_key_pem = Some(tls_key_pem)
    }
    if let Some(tls_ca_file) = agent
        .otlp_exporter_traces_ca_group
        .otlp_exporter_traces_tls_ca_file
    {
        config.ca_group.tls_ca_file = Some(tls_ca_file)
    }
    if let Some(tls_ca_pem) = agent
        .otlp_exporter_traces_ca_group
        .otlp_exporter_traces_tls_ca_pem
    {
        config.ca_group.tls_ca_pem = Some(tls_ca_pem)
    }

    config
}

pub fn build_metrics_config(agent: OTLPExporterArgs) -> OTLPExporterBaseArgs {
    let mut config = agent.base;

    if let Some(protocol) = agent.otlp_exporter_metrics_protocol {
        config.protocol = protocol
    }
    if let Some(headers) = agent.otlp_exporter_metrics_custom_headers {
        config.custom_headers = headers
    }
    if let Some(compression) = agent.otlp_exporter_metrics_compression {
        config.compression = compression
    }
    if let Some(tls_skip_verify) = agent.otlp_exporter_metrics_tls_skip_verify {
        config.tls_skip_verify = tls_skip_verify
    }
    if let Some(request_time) = agent.otlp_exporter_metrics_request_timeout {
        config.request_timeout = request_time
    }
    if let Some(max_elapsed_time) = agent.otlp_exporter_metrics_retry_max_elapsed_time {
        config.retry_max_elapsed_time = max_elapsed_time
    }
    if let Some(initial_backoff) = agent.otlp_exporter_metrics_retry_initial_backoff {
        config.retry_initial_backoff = initial_backoff
    }
    if let Some(max_backoff) = agent.otlp_exporter_metrics_retry_max_backoff {
        config.retry_max_backoff = max_backoff
    }
    if let Some(tls_cert_file) = agent
        .otlp_exporter_metrics_cert_group
        .otlp_exporter_metrics_tls_cert_file
    {
        config.cert_group.tls_cert_file = Some(tls_cert_file)
    }
    if let Some(tls_cert_pem) = agent
        .otlp_exporter_metrics_cert_group
        .otlp_exporter_metrics_tls_cert_pem
    {
        config.cert_group.tls_cert_pem = Some(tls_cert_pem)
    }
    if let Some(tls_key_file) = agent
        .otlp_exporter_metrics_key_group
        .otlp_exporter_metrics_tls_key_file
    {
        config.key_group.tls_key_file = Some(tls_key_file)
    }
    if let Some(tls_key_pem) = agent
        .otlp_exporter_metrics_key_group
        .otlp_exporter_metrics_tls_key_pem
    {
        config.key_group.tls_key_pem = Some(tls_key_pem)
    }
    if let Some(tls_ca_file) = agent
        .otlp_exporter_metrics_ca_group
        .otlp_exporter_metrics_tls_ca_file
    {
        config.ca_group.tls_ca_file = Some(tls_ca_file)
    }
    if let Some(tls_ca_pem) = agent
        .otlp_exporter_metrics_ca_group
        .otlp_exporter_metrics_tls_ca_pem
    {
        config.ca_group.tls_ca_pem = Some(tls_ca_pem)
    }

    config
}

pub fn build_logs_config(agent: OTLPExporterArgs) -> OTLPExporterBaseArgs {
    let mut config = agent.base;

    if let Some(protocol) = agent.otlp_exporter_logs_protocol {
        config.protocol = protocol
    }
    if let Some(headers) = agent.otlp_exporter_logs_custom_headers {
        config.custom_headers = headers
    }
    if let Some(compression) = agent.otlp_exporter_logs_compression {
        config.compression = compression
    }
    if let Some(tls_skip_verify) = agent.otlp_exporter_logs_tls_skip_verify {
        config.tls_skip_verify = tls_skip_verify
    }
    if let Some(request_time) = agent.otlp_exporter_logs_request_timeout {
        config.request_timeout = request_time
    }
    if let Some(max_elapsed_time) = agent.otlp_exporter_logs_retry_max_elapsed_time {
        config.retry_max_elapsed_time = max_elapsed_time
    }
    if let Some(initial_backoff) = agent.otlp_exporter_logs_retry_initial_backoff {
        config.retry_initial_backoff = initial_backoff
    }
    if let Some(max_backoff) = agent.otlp_exporter_logs_retry_max_backoff {
        config.retry_max_backoff = max_backoff
    }
    if let Some(tls_cert_file) = agent
        .otlp_exporter_logs_cert_group
        .otlp_exporter_logs_tls_cert_file
    {
        config.cert_group.tls_cert_file = Some(tls_cert_file)
    }
    if let Some(tls_cert_pem) = agent
        .otlp_exporter_logs_cert_group
        .otlp_exporter_logs_tls_cert_pem
    {
        config.cert_group.tls_cert_pem = Some(tls_cert_pem)
    }
    if let Some(tls_key_file) = agent
        .otlp_exporter_logs_key_group
        .otlp_exporter_logs_tls_key_file
    {
        config.key_group.tls_key_file = Some(tls_key_file)
    }
    if let Some(tls_key_pem) = agent
        .otlp_exporter_logs_key_group
        .otlp_exporter_logs_tls_key_pem
    {
        config.key_group.tls_key_pem = Some(tls_key_pem)
    }
    if let Some(tls_ca_file) = agent
        .otlp_exporter_logs_ca_group
        .otlp_exporter_logs_tls_ca_file
    {
        config.ca_group.tls_ca_file = Some(tls_ca_file)
    }
    if let Some(tls_ca_pem) = agent
        .otlp_exporter_logs_ca_group
        .otlp_exporter_logs_tls_ca_pem
    {
        config.ca_group.tls_ca_pem = Some(tls_ca_pem)
    }

    config
}

impl OTLPExporterBaseArgs {
    pub fn into_exporter_config(self, type_name: &str, endpoint: Endpoint) -> OTLPExporterConfig {
        let mut builder = otlp::config_builder(type_name, endpoint, self.protocol.into())
            .with_authenticator(self.authenticator.map(|a| a.into()))
            .with_tls_skip_verify(self.tls_skip_verify)
            .with_headers(self.custom_headers.as_slice())
            .with_request_timeout(self.request_timeout.into())
            .with_max_elapsed_time(self.retry_max_elapsed_time.into())
            .with_initial_backoff(self.retry_initial_backoff.into())
            .with_max_backoff(self.retry_max_backoff.into())
            .with_compression_encoding(self.compression.into());

        if let Some(tls_cert_file) = self.cert_group.tls_cert_file {
            builder = builder.with_cert_file(tls_cert_file.as_str());
        } else if let Some(tls_cert_pem) = self.cert_group.tls_cert_pem {
            builder = builder.with_cert_pem(tls_cert_pem.as_str());
        }

        if let Some(tls_key_file) = self.key_group.tls_key_file {
            builder = builder.with_cert_file(tls_key_file.as_str());
        } else if let Some(tls_key_pem) = self.key_group.tls_key_pem {
            builder = builder.with_cert_pem(tls_key_pem.as_str());
        }
        if let Some(tls_ca_file) = self.ca_group.tls_ca_file {
            builder = builder.with_cert_file(tls_ca_file.as_str());
        } else if let Some(tls_ca_pem) = self.ca_group.tls_ca_pem {
            builder = builder.with_cert_pem(tls_ca_pem.as_str());
        }

        builder
    }
}
