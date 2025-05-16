use crate::topology::batch::BatchConfig;
use clap::Args;

// Batch settings
#[derive(Debug, Clone, Args)]
pub struct BatchArgs {
    /// Batch size in number of spans/metrics - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(long, env = "ROTEL_BATCH_MAX_SIZE", default_value = "8192")]
    pub batch_max_size: usize,

    /// OTLP batch timeout - Used as default for all OTLP data types unless more specific flag specified.
    #[arg(long, env = "ROTEL_BATCH_TIMEOUT", default_value = "200ms")]
    pub batch_timeout: humantime::Duration,

    /// OTLP traces max batch size in number of spans - Overrides batch_max_size for OTLP traces if specified.
    #[arg(long, env = "ROTEL_TRACES_BATCH_MAX_SIZE")]
    pub traces_batch_max_size: Option<usize>,

    /// OTLP metrics max batch size in number of metrics - Overrides batch_max_size for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_METRICS_BATCH_MAX_SIZE")]
    pub metrics_batch_max_size: Option<usize>,

    /// OTLP logs max batch size in number of logs - Overrides batch_max_size for OTLP logs if specified.
    #[arg(long, env = "ROTEL_LOGS_BATCH_MAX_SIZE")]
    pub logs_batch_max_size: Option<usize>,

    /// OTLP traces batch timeout - Overrides batch_timeout for OTLP traces if specified.
    #[arg(long, env = "ROTEL_TRACES_BATCH_TIMEOUT")]
    pub traces_batch_timeout: Option<humantime::Duration>,

    /// OTLP metrics batch timeout - Overrides batch_timeout for OTLP metrics if specified.
    #[arg(long, env = "ROTEL_METRICS_BATCH_TIMEOUT")]
    pub metrics_batch_timeout: Option<humantime::Duration>,

    /// OTLP logs batch timeout - Overrides batch_timeout for OTLP logs if specified.
    #[arg(long, env = "ROTEL_LOGS_BATCH_TIMEOUT")]
    pub logs_batch_timeout: Option<humantime::Duration>,

    /// Disable batching, incoming messages are immediately exported (not recommended)
    #[arg(long, env = "ROTEL_DISABLE_BATCHING", default_value = "false")]
    pub disable_batching: bool,
}

// todo: add these as impl functions of the exporter args?
pub fn build_traces_batch_config(config: BatchArgs) -> BatchConfig {
    BatchConfig {
        max_size: config
            .traces_batch_max_size
            .unwrap_or(config.batch_max_size),
        timeout: config
            .traces_batch_timeout
            .unwrap_or(config.batch_timeout)
            .into(),
        disabled: config.disable_batching,
    }
}

pub fn build_metrics_batch_config(config: BatchArgs) -> BatchConfig {
    BatchConfig {
        max_size: config
            .metrics_batch_max_size
            .unwrap_or(config.batch_max_size),
        timeout: config
            .metrics_batch_timeout
            .unwrap_or(config.batch_timeout)
            .into(),
        disabled: config.disable_batching,
    }
}

pub fn build_logs_batch_config(config: BatchArgs) -> BatchConfig {
    BatchConfig {
        max_size: config.logs_batch_max_size.unwrap_or(config.batch_max_size),
        timeout: config
            .logs_batch_timeout
            .unwrap_or(config.batch_timeout)
            .into(),
        disabled: config.disable_batching,
    }
}
