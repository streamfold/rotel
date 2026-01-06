use std::time::Duration;

use clap::Args;
use serde::Deserialize;

/// Macro to generate exporter-specific retry argument structs with prefixed argument names.
///
/// This macro helps avoid clap argument name conflicts when multiple exporters need retry
/// configuration. Each exporter gets its own retry args struct with unique CLI flags and
/// environment variable names.
///
/// # Parameters
/// - `$struct_name`: Name of the struct to generate (e.g., `ClickhouseRetryArgs`)
/// - `$long_prefix`: Prefix for id and long options (e.g., `"clickhouse-exporter"`)
/// - `$env_prefix`: Prefix for environment variables (e.g., `"ROTEL_CLICKHOUSE_EXPORTER"`)
/// - `$doc_prefix`: Prefix for documentation (e.g., `"Clickhouse Exporter"`)
#[macro_export]
macro_rules! define_exporter_retry_args {
    (
        $struct_name:ident,
        $long_prefix:literal,
        $env_prefix:literal,
        $doc_prefix:literal
    ) => {
        #[derive(Default, Debug, Clone, clap::Args, serde::Deserialize)]
        #[serde(default)]
        pub struct $struct_name {
            #[doc = concat!($doc_prefix, " Retry initial backoff")]
            #[arg(
                                        id = concat!($long_prefix, "-retry-initial-backoff"),
                                        long = concat!($long_prefix, "-retry-initial-backoff"),
                                        env = concat!($env_prefix, "_RETRY_INITIAL_BACKOFF"),
                                        value_parser = humantime::parse_duration,
                                    )]
            pub initial_backoff: Option<std::time::Duration>,

            #[doc = concat!($doc_prefix, " Retry max backoff")]
            #[arg(
                                        id = concat!($long_prefix, "-retry-max-backoff"),
                                        long = concat!($long_prefix, "-retry-max-backoff"),
                                        env = concat!($env_prefix, "_RETRY_MAX_BACKOFF"),
                                        value_parser = humantime::parse_duration,
                                    )]
            pub max_backoff: Option<std::time::Duration>,

            #[doc = concat!($doc_prefix, " Retry max elapsed time")]
            #[arg(
                                        id = concat!($long_prefix, "-retry-max-elapsed-time"),
                                        long = concat!($long_prefix, "-retry-max-elapsed-time"),
                                        env = concat!($env_prefix, "_RETRY_MAX_ELAPSED_TIME"),
                                        value_parser = humantime::parse_duration,
                                    )]
            pub max_elapsed_time: Option<std::time::Duration>,
        }
    };
}

/// Global retry arguments that all exporters fall back to
#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct GlobalExporterRetryArgs {
    /// Exporter Retry initial backoff - Default for all exporters unless overridden by exporter.
    #[arg(
        id("exporter-retry-initial-backoff"),
        long("exporter-retry-initial-backoff"),
        env = "ROTEL_EXPORTER_RETRY_INITIAL_BACKOFF",
        default_value = "5s",
        value_parser = humantime::parse_duration,
    )]
    pub initial_backoff: std::time::Duration,

    /// Exporter Retry max backoff - Default for all exporters unless overridden by exporter.
    #[arg(
        id("exporter-retry-max-backoff"),
        long("exporter-retry-max-backoff"),
        env = "ROTEL_EXPORTER_RETRY_MAX_BACKOFF",
        default_value = "30s",
        value_parser = humantime::parse_duration,
    )]
    pub max_backoff: std::time::Duration,

    /// Exporter Retry max elapsed time - Default for all exporters unless overridden by exporter.
    #[arg(
        id("exporter-retry-max-elapsed-time"),
        long("exporter-retry-max-elapsed-time"),
        env = "ROTEL_EXPORTER_RETRY_MAX_ELAPSED_TIME",
        default_value = "300s",
        value_parser = humantime::parse_duration,
    )]
    pub max_elapsed_time: std::time::Duration,
}

impl Default for GlobalExporterRetryArgs {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(30),
            max_elapsed_time: Duration::from_secs(300),
        }
    }
}
