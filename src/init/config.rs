use crate::exporters::awsemf::AwsEmfExporterConfigBuilder;
use crate::exporters::clickhouse::ClickhouseExporterConfigBuilder;
use crate::exporters::datadog::DatadogExporterConfigBuilder;
#[cfg(feature = "rdkafka")]
use crate::exporters::kafka::KafkaExporterConfig;
use crate::exporters::otlp::Endpoint;
use crate::exporters::otlp::config::OTLPExporterConfig;
use crate::exporters::xray::XRayExporterConfigBuilder;
use crate::init::args::{AgentRun, Exporter, Receiver};
use crate::init::awsemf_exporter::AwsEmfExporterArgs;
use crate::init::clickhouse_exporter::ClickhouseExporterArgs;
use crate::init::datadog_exporter::DatadogExporterArgs;
#[cfg(feature = "file_exporter")]
use crate::init::file_exporter::FileExporterArgs;
#[cfg(feature = "rdkafka")]
use crate::init::kafka_exporter::KafkaExporterArgs;
use crate::init::otlp_exporter::{
    OTLPExporterBaseArgs, build_logs_config, build_metrics_config, build_traces_config,
};
use crate::init::parse::parse_bool_value;
use crate::init::xray_exporter::XRayExporterArgs;
#[cfg(feature = "rdkafka")]
use crate::receivers::kafka::config::KafkaReceiverConfig;
use crate::receivers::otlp::OTLPReceiverConfig;
use figment::{Figment, providers::Env};
use gethostname::gethostname;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use tower::BoxError;
use tracing::error;

struct ExporterMap {
    exporters: HashMap<String, ExporterArgs>,
}

impl Debug for ExporterMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExporterMap{{")?;
        for (name, args) in &self.exporters {
            write!(f, "{}={:?},", name, args)?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

impl FromStr for ExporterMap {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let exporters: HashMap<String, ExporterArgs> = s
            .split(",")
            .map(|exporter| {
                let sp: Vec<&str> = exporter.split(":").collect();
                if sp.len() > 2 {
                    return Err(format!("invalid exporter config: {}", exporter).into());
                }

                let (name, exporter_type) = if sp.len() == 1 {
                    (sp[0], sp[0])
                } else {
                    (sp[0], sp[1])
                };

                let args = match args_from_env_prefix(exporter_type, name) {
                    Ok(args) => args,
                    Err(e) => return Err(e),
                };

                Ok((name.to_string(), args))
            })
            .collect::<Result<HashMap<String, ExporterArgs>, BoxError>>()?;

        Ok(ExporterMap { exporters })
    }
}

impl ExporterMap {
    fn get(&self, name: &String) -> Option<&ExporterArgs> {
        self.exporters.get(name)
    }
}

#[derive(Default)]
pub(crate) struct ExporterConfigs {
    pub(crate) metrics: Vec<ExporterConfig>,
    pub(crate) logs: Vec<ExporterConfig>,
    pub(crate) traces: Vec<ExporterConfig>,
    pub(crate) internal_metrics: Vec<ExporterConfig>,
}

impl ExporterConfigs {
    /// Set all exporters to use indefinite retry (for Kafka offset tracking)
    #[allow(dead_code)] // Allowing for feature flagging Kafka support
    pub(crate) fn set_indefinite_retry(&mut self) {
        for exporter in self
            .traces
            .iter_mut()
            .chain(self.metrics.iter_mut())
            .chain(self.logs.iter_mut())
            .chain(self.internal_metrics.iter_mut())
        {
            match exporter {
                ExporterConfig::Otlp(config) => {
                    config.retry_config.indefinite_retry = true;
                }
                ExporterConfig::Datadog(config) => {
                    config.set_indefinite_retry();
                }
                ExporterConfig::Xray(config) => {
                    config.set_indefinite_retry();
                }
                ExporterConfig::Awsemf(config) => {
                    config.set_indefinite_retry();
                }
                ExporterConfig::Clickhouse(config) => {
                    config.set_indefinite_retry();
                }
                // Kafka is already maxint reties and we don't need to worry about this for file.
                _ => {}
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum ExporterArgs {
    Blackhole,
    Otlp(OTLPExporterBaseArgs),
    Datadog(DatadogExporterArgs),
    Clickhouse(ClickhouseExporterArgs),
    Xray(XRayExporterArgs),
    Awsemf(AwsEmfExporterArgs),
    #[cfg(feature = "rdkafka")]
    Kafka(KafkaExporterArgs),
    #[cfg(feature = "file_exporter")]
    File(FileExporterArgs),
}

#[derive(PartialEq)]
enum PipelineType {
    Metrics,
    Logs,
    Traces,
    InternalMetrics,
}

impl Display for PipelineType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineType::Metrics => write!(f, "metrics"),
            PipelineType::Logs => write!(f, "logs"),
            PipelineType::Traces => write!(f, "traces"),
            PipelineType::InternalMetrics => write!(f, "internal_metrics"),
        }
    }
}

trait TryIntoConfig {
    fn try_into_config(
        &self,
        pipeline_type: PipelineType,
        environment: &str,
    ) -> Result<ExporterConfig, BoxError>;
}

pub(crate) enum ExporterConfig {
    Blackhole,
    Otlp(OTLPExporterConfig),
    Datadog(DatadogExporterConfigBuilder),
    Clickhouse(ClickhouseExporterConfigBuilder),
    Xray(XRayExporterConfigBuilder),
    Awsemf(AwsEmfExporterConfigBuilder),
    #[cfg(feature = "rdkafka")]
    Kafka(KafkaExporterConfig),
    #[cfg(feature = "file_exporter")]
    File(crate::exporters::file::config::FileExporterConfig),
}

impl ExporterConfig {
    pub fn name(&self) -> &'static str {
        match self {
            ExporterConfig::Blackhole => "blackhole",
            ExporterConfig::Otlp(_) => "otlp",
            ExporterConfig::Datadog(_) => "datadog",
            ExporterConfig::Clickhouse(_) => "clickhouse",
            ExporterConfig::Xray(_) => "awsxray",
            ExporterConfig::Awsemf(_) => "awsemf",
            #[cfg(feature = "rdkafka")]
            ExporterConfig::Kafka(_) => "kafka",
        }
    }
}

#[derive(Debug)]
pub(crate) enum ReceiverConfig {
    Otlp(OTLPReceiverConfig),
    #[cfg(feature = "rdkafka")]
    Kafka(KafkaReceiverConfig),
}

impl TryIntoConfig for ExporterArgs {
    fn try_into_config(
        &self,
        pipeline_type: PipelineType,
        environment: &str,
    ) -> Result<ExporterConfig, BoxError> {
        match self {
            ExporterArgs::Blackhole => Ok(ExporterConfig::Blackhole),
            ExporterArgs::Otlp(otlp) => {
                let otlp = otlp.clone();

                let endpoint = otlp.endpoint.as_ref();
                match pipeline_type {
                    PipelineType::Metrics | PipelineType::InternalMetrics => {
                        if endpoint.is_none() && otlp.metrics_endpoint.is_none() {
                            return Err("must specify an endpoint for OTLP metrics".into());
                        }
                        let endpoint = otlp
                            .metrics_endpoint
                            .as_ref()
                            .map(|e| Endpoint::Full(e.clone()))
                            .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));

                        if pipeline_type == PipelineType::Metrics {
                            Ok(ExporterConfig::Otlp(
                                otlp.into_exporter_config("metrics", endpoint),
                            ))
                        } else {
                            Ok(ExporterConfig::Otlp(
                                otlp.into_exporter_config("internal_metrics", endpoint),
                            ))
                        }
                    }
                    PipelineType::Logs => {
                        if endpoint.is_none() && otlp.logs_endpoint.is_none() {
                            return Err("must specify an endpoint for OTLP logs".into());
                        }
                        let endpoint = otlp
                            .logs_endpoint
                            .as_ref()
                            .map(|e| Endpoint::Full(e.clone()))
                            .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));

                        Ok(ExporterConfig::Otlp(
                            otlp.into_exporter_config("logs", endpoint),
                        ))
                    }
                    PipelineType::Traces => {
                        if endpoint.is_none() && otlp.traces_endpoint.is_none() {
                            return Err("must specify an endpoint for OTLP traces".into());
                        }
                        let endpoint = otlp
                            .traces_endpoint
                            .as_ref()
                            .map(|e| Endpoint::Full(e.clone()))
                            .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));

                        Ok(ExporterConfig::Otlp(
                            otlp.into_exporter_config("traces", endpoint),
                        ))
                    }
                }
            }
            ExporterArgs::Datadog(dd) => {
                if pipeline_type != PipelineType::Traces {
                    return Err(format!(
                        "Datadog exporter not supported for pipeline type {}",
                        pipeline_type
                    )
                    .into());
                }

                if dd.api_key.is_none() {
                    // todo: is there a way to make this dd.g required with the exporter mode?
                    return Err("must specify Datadog exporter API key".into());
                }
                let api_key = dd.api_key.as_ref().unwrap();

                let hostname = get_hostname();

                let mut builder = DatadogExporterConfigBuilder::new(
                    dd.region.into(),
                    dd.custom_endpoint.clone(),
                    api_key.clone(),
                )
                .with_environment(environment.to_string());

                if let Some(hostname) = hostname {
                    builder = builder.with_hostname(hostname);
                }

                Ok(ExporterConfig::Datadog(builder))
            }
            ExporterArgs::Clickhouse(ch) => {
                if ch.endpoint.is_none() {
                    return Err("must specify a Clickhouse exporter endpoint".into());
                }

                let async_insert = parse_bool_value(&ch.async_insert)?;

                let mut cfg_builder = ClickhouseExporterConfigBuilder::new(
                    ch.endpoint.as_ref().unwrap().clone(),
                    ch.database.clone(),
                    ch.table_prefix.clone(),
                )
                .with_compression(ch.compression)
                .with_async_insert(async_insert)
                .with_json(ch.enable_json)
                .with_json_underscore(ch.json_underscore)
                .with_request_timeout(ch.request_timeout);

                if let Some(user) = &ch.user {
                    cfg_builder = cfg_builder.with_user(user.clone());
                }

                if let Some(password) = &ch.password {
                    cfg_builder = cfg_builder.with_password(password.clone());
                }

                Ok(ExporterConfig::Clickhouse(cfg_builder))
            }
            ExporterArgs::Xray(xray) => {
                if pipeline_type != PipelineType::Traces {
                    return Err(format!(
                        "XRay exporter not supported for pipeline type {}",
                        pipeline_type
                    )
                    .into());
                }

                let builder =
                    XRayExporterConfigBuilder::new(xray.region, xray.custom_endpoint.clone());

                Ok(ExporterConfig::Xray(builder))
            }
            #[cfg(feature = "file_exporter")]
            ExporterArgs::File(file) => {
                let config = crate::exporters::file::config::FileExporterConfig::new(
                    file.file_format,
                    file.output_dir.clone(),
                    file.flush_interval,
                    file.parquet_compression,
                );
                Ok(ExporterConfig::File(config))
            }
            ExporterArgs::Awsemf(awsemf) => {
                if pipeline_type != PipelineType::Metrics
                    && pipeline_type != PipelineType::InternalMetrics
                {
                    return Err(format!(
                        "AWS EMF exporter only supports metrics, not {}",
                        pipeline_type
                    )
                    .into());
                }

                let mut builder = AwsEmfExporterConfigBuilder::new()
                    .with_region(awsemf.region)
                    .with_log_group_name(awsemf.log_group_name.clone())
                    .with_log_stream_name(awsemf.log_stream_name.clone())
                    .with_include_dimensions(awsemf.include_dimensions.clone())
                    .with_exclude_dimensions(awsemf.exclude_dimensions.clone())
                    .with_retain_initial_value_of_delta_metric(
                        awsemf.retain_initial_value_of_delta_metric,
                    );

                if let Some(log_retention) = &awsemf.log_retention {
                    builder = builder.with_log_retention(*log_retention);
                }

                if let Some(namespace) = &awsemf.namespace {
                    builder = builder.with_namespace(namespace.clone());
                }

                if let Some(custom_endpoint) = &awsemf.custom_endpoint {
                    builder = builder.with_custom_endpoint(custom_endpoint.clone());
                }

                Ok(ExporterConfig::Awsemf(builder))
            }
            #[cfg(feature = "rdkafka")]
            ExporterArgs::Kafka(k) => {
                if k.brokers.is_empty() {
                    return Err("must specify a Kafka broker address".into());
                }
                Ok(ExporterConfig::Kafka(k.build_config()))
            }
        }
    }
}

pub(crate) fn get_receivers_config(
    config: &AgentRun,
) -> Result<HashMap<Receiver, ReceiverConfig>, BoxError> {
    if config.receivers.is_none() && config.receiver.is_none() {
        let mut map = HashMap::new();
        map.insert(Receiver::Otlp, get_receiver_config(config, Receiver::Otlp));
        return Ok(map);
    }
    if config.receivers.is_some() && config.receiver.is_some() {
        return Err("Can not use --receiver and --receivers together".into());
    }

    if let Some(receiver) = config.receiver {
        let mut map = HashMap::new();
        map.insert(receiver, get_receiver_config(config, receiver));
        return Ok(map);
    }

    let mut receivers: HashMap<Receiver, ReceiverConfig> = HashMap::new();
    if let Some(recs) = &config.receivers {
        let rec: Vec<&str> = recs.split(",").collect();
        for receiver in rec {
            let receiver: Receiver = receiver.parse()?;
            receivers.insert(receiver, get_receiver_config(config, receiver));
        }
    }
    Ok(receivers)
}

pub(crate) fn get_exporters_config(
    config: &AgentRun,
    environment: &str,
) -> Result<ExporterConfigs, BoxError> {
    // Default to OTLP exporter
    if config.exporters.is_none() && config.exporter.is_none() {
        return get_single_exporter_config(config, Exporter::Otlp, environment);
    }

    if config.exporters.is_some() && config.exporter.is_some() {
        return Err("Can not use --exporter and --exporters together".into());
    }

    if let Some(exporter) = config.exporter {
        return get_single_exporter_config(config, exporter, environment);
    }

    get_multi_exporter_config(
        config,
        config.exporters.as_ref().unwrap().clone(),
        environment,
    )
}

fn get_multi_exporter_config(
    config: &AgentRun,
    exporters: String,
    environment: &str,
) -> Result<ExporterConfigs, BoxError> {
    let exporter_map = exporters.parse::<ExporterMap>()?;

    let mut cfg = ExporterConfigs::default();

    if let Some(traces_exps) = &config.exporters_traces {
        let sp: Vec<&str> = traces_exps.split(",").collect();

        cfg.traces = sp
            .into_iter()
            .map(|exp| match exporter_map.get(&exp.to_string()) {
                Some(args) => match args.try_into_config(PipelineType::Traces, environment) {
                    Ok(config) => Ok(config),
                    Err(err) => Err(format!("Exporter[{}]: {}", exp, err).into()),
                },
                None => Err(format!("Can not find exporter {} for traces exporters", exp).into()),
            })
            .collect::<Result<Vec<ExporterConfig>, BoxError>>()?;
    }

    if let Some(metrics_exps) = &config.exporters_metrics {
        let sp: Vec<&str> = metrics_exps.split(",").collect();

        cfg.metrics = sp
            .into_iter()
            .map(|exp| match exporter_map.get(&exp.to_string()) {
                Some(args) => match args.try_into_config(PipelineType::Metrics, environment) {
                    Ok(config) => Ok(config),
                    Err(err) => Err(format!("Exporter[{}]: {}", exp, err).into()),
                },
                None => Err(format!("Can not find exporter {} for metrics exporters", exp).into()),
            })
            .collect::<Result<Vec<ExporterConfig>, BoxError>>()?;
    }

    if let Some(logs_exps) = &config.exporters_logs {
        let sp: Vec<&str> = logs_exps.split(",").collect();

        cfg.logs = sp
            .into_iter()
            .map(|exp| match exporter_map.get(&exp.to_string()) {
                Some(args) => match args.try_into_config(PipelineType::Logs, environment) {
                    Ok(config) => Ok(config),
                    Err(err) => Err(format!("Exporter[{}]: {}", exp, err).into()),
                },
                None => Err(format!("Can not find exporter {} for logs exporters", exp).into()),
            })
            .collect::<Result<Vec<ExporterConfig>, BoxError>>()?;
    }

    if let Some(internal_metrics_exps) = &config.exporters_internal_metrics {
        let sp: Vec<&str> = internal_metrics_exps.split(",").collect();

        cfg.internal_metrics = sp
            .into_iter()
            .map(|exp| match exporter_map.get(&exp.to_string()) {
                Some(args) => {
                    match args.try_into_config(PipelineType::InternalMetrics, environment) {
                        Ok(config) => Ok(config),
                        Err(err) => Err(format!("Exporter[{}]: {}", exp, err).into()),
                    }
                }
                None => Err(format!(
                    "Can not find exporter {} for internal metrics exporters",
                    exp
                )
                .into()),
            })
            .collect::<Result<Vec<ExporterConfig>, BoxError>>()?;
    }

    // Internal metrics do not count here, we need at least one actual pipeline
    if cfg.traces.is_empty() && cfg.metrics.is_empty() && cfg.logs.is_empty() {
        return Err(
            "No telemetry pipeline exporters, did you set --exporters-{traces,metrics,logs}?"
                .into(),
        );
    }

    Ok(cfg)
}

fn args_from_env_prefix(exporter_type: &str, prefix: &str) -> Result<ExporterArgs, BoxError> {
    let figment = Figment::new().merge(Env::prefixed(
        format!("ROTEL_EXPORTER_{}_", prefix.to_uppercase()).as_str(),
    ));
    match exporter_type {
        "blackhole" => Ok(ExporterArgs::Blackhole),
        "otlp" => {
            let args: OTLPExporterBaseArgs = match figment.extract() {
                Ok(args) => args,
                Err(e) => return Err(format!("failed to parse OTLP config: {}", e).into()),
            };

            Ok(ExporterArgs::Otlp(args))
        }
        "datadog" => {
            let args: DatadogExporterArgs = match figment.extract() {
                Ok(args) => args,
                Err(e) => return Err(format!("failed to parse Datadog config: {}", e).into()),
            };

            Ok(ExporterArgs::Datadog(args))
        }
        "clickhouse" => {
            let args: ClickhouseExporterArgs = match figment.extract() {
                Ok(args) => args,
                Err(e) => {
                    return Err(format!("failed to parse Clickhouse config: {}", e).into());
                }
            };

            Ok(ExporterArgs::Clickhouse(args))
        }
        "awsxray" => {
            let args: XRayExporterArgs = match figment.extract() {
                Ok(args) => args,
                Err(e) => return Err(format!("failed to parse X-Ray config: {}", e).into()),
            };

            Ok(ExporterArgs::Xray(args))
        }
        "awsemf" => {
            let args: AwsEmfExporterArgs = match figment.extract() {
                Ok(args) => args,
                Err(e) => return Err(format!("failed to parse AWS EMF config: {}", e).into()),
            };

            Ok(ExporterArgs::Awsemf(args))
        }
        #[cfg(feature = "rdkafka")]
        "kafka" => {
            let args: KafkaExporterArgs = match figment.extract() {
                Ok(args) => args,
                Err(e) => return Err(format!("failed to parse Kafka config {}", e).into()),
            };
            Ok(ExporterArgs::Kafka(args))
        }
        _ => Err(format!("unknown exporter type: {}", exporter_type).into()),
    }
}

// Function is currently small but expect it will grow over time so splitting out rather than inlining for now.
fn get_receiver_config(config: &AgentRun, receiver: Receiver) -> ReceiverConfig {
    match receiver {
        Receiver::Otlp => ReceiverConfig::Otlp(OTLPReceiverConfig::from(&config.otlp_receiver)),
        #[cfg(feature = "rdkafka")]
        Receiver::Kafka => ReceiverConfig::Kafka(config.kafka_receiver.build_config()),
    }
}

fn get_single_exporter_config(
    config: &AgentRun,
    exporter: Exporter,
    environment: &str,
) -> Result<ExporterConfigs, BoxError> {
    let mut cfg = ExporterConfigs::default();

    // We convert these into ExporterArgs so that we can use the `try_into_config` method
    // above to DRY this out.
    match exporter {
        Exporter::Otlp => {
            // Because the single exporter configuration has custom overrides per type, we
            // must build new args here that'll override with the custom type variations.
            let args = ExporterArgs::Otlp(build_traces_config(config.otlp_exporter.clone()));
            cfg.traces
                .push(args.try_into_config(PipelineType::Traces, environment)?);

            let args = ExporterArgs::Otlp(build_metrics_config(config.otlp_exporter.clone()));
            cfg.metrics
                .push(args.try_into_config(PipelineType::Metrics, environment)?);

            let args = ExporterArgs::Otlp(build_logs_config(config.otlp_exporter.clone()));
            cfg.logs
                .push(args.try_into_config(PipelineType::Logs, environment)?);

            let args = ExporterArgs::Otlp(build_metrics_config(config.otlp_exporter.clone()));
            cfg.internal_metrics
                .push(args.try_into_config(PipelineType::InternalMetrics, environment)?);
        }
        Exporter::Blackhole => {
            cfg.traces.push(ExporterConfig::Blackhole {});
            cfg.metrics.push(ExporterConfig::Blackhole {});
            cfg.logs.push(ExporterConfig::Blackhole {});
        }
        Exporter::Datadog => {
            let args = ExporterArgs::Datadog(config.datadog_exporter.clone());
            cfg.traces
                .push(args.try_into_config(PipelineType::Traces, environment)?);
        }
        Exporter::Clickhouse => {
            let args = ExporterArgs::Clickhouse(config.clickhouse_exporter.clone());
            cfg.logs
                .push(args.try_into_config(PipelineType::Logs, environment)?);
            cfg.traces
                .push(args.try_into_config(PipelineType::Traces, environment)?);
            cfg.metrics
                .push(args.try_into_config(PipelineType::Metrics, environment)?);
        }
        Exporter::AwsXray => {
            let args = ExporterArgs::Xray(config.aws_xray_exporter.clone());
            cfg.traces
                .push(args.try_into_config(PipelineType::Traces, environment)?);
        }
        Exporter::AwsEmf => {
            let args = ExporterArgs::Awsemf(config.aws_emf_exporter.clone());
            cfg.metrics
                .push(args.try_into_config(PipelineType::Metrics, environment)?);
        }

        #[cfg(feature = "rdkafka")]
        Exporter::Kafka => {
            let kafka_config = config.kafka_exporter.build_config();
            cfg.traces.push(ExporterConfig::Kafka(kafka_config.clone()));
            cfg.metrics
                .push(ExporterConfig::Kafka(kafka_config.clone()));
            cfg.logs.push(ExporterConfig::Kafka(kafka_config));
        }
        #[cfg(feature = "file_exporter")]
        Exporter::File => {
            let args = ExporterArgs::File(config.file_exporter.clone());
            cfg.logs
                .push(args.try_into_config(PipelineType::Logs, environment)?);
            cfg.traces
                .push(args.try_into_config(PipelineType::Traces, environment)?);
            cfg.metrics
                .push(args.try_into_config(PipelineType::Metrics, environment)?);
        }
    }

    Ok(cfg)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::args::AgentRun;
    use std::collections::HashMap;
    use std::env;

    // Helper struct to manage environment variables during tests
    struct EnvManager {
        original_vars: HashMap<String, Option<String>>,
    }

    impl EnvManager {
        fn new() -> Self {
            Self {
                original_vars: HashMap::new(),
            }
        }

        fn set_var(&mut self, key: &str, value: &str) {
            // Save original value if not already saved
            if !self.original_vars.contains_key(key) {
                self.original_vars
                    .insert(key.to_string(), env::var(key).ok());
            }
            unsafe { env::set_var(key, value) };
        }

        fn remove_var(&mut self, key: &str) {
            // Save original value if not already saved
            if !self.original_vars.contains_key(key) {
                self.original_vars
                    .insert(key.to_string(), env::var(key).ok());
            }
            unsafe { env::remove_var(key) };
        }
    }

    impl Drop for EnvManager {
        fn drop(&mut self) {
            // Restore all environment variables
            for (key, original_value) in &self.original_vars {
                match original_value {
                    Some(value) => unsafe { env::set_var(key, value) },
                    None => unsafe { env::remove_var(key) },
                }
            }
        }
    }

    #[test]
    fn test_get_multi_exporter_config_blackhole_traces_only() {
        let config = AgentRun {
            exporters_traces: Some("test_blackhole".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(
            &config,
            "test_blackhole:blackhole".to_string(),
            "production",
        );

        assert!(result.is_ok());
        let exporters = result.unwrap();
        assert_eq!(1, exporters.traces.len());
        assert!(exporters.metrics.is_empty());
        assert!(exporters.logs.is_empty());

        match exporters.traces[0] {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter"),
        }
    }

    #[test]
    fn test_get_multi_exporter_config_blackhole_all_pipelines() {
        let config = AgentRun {
            exporters_traces: Some("test_blackhole".to_string()),
            exporters_metrics: Some("test_blackhole".to_string()),
            exporters_logs: Some("test_blackhole".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(
            &config,
            "test_blackhole:blackhole".to_string(),
            "production",
        );

        assert!(result.is_ok());
        let exporters = result.unwrap();
        assert_eq!(1, exporters.traces.len());
        assert_eq!(1, exporters.metrics.len());
        assert_eq!(1, exporters.logs.len());

        match exporters.traces[0] {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter for traces"),
        }
        match exporters.metrics[0] {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter for metrics"),
        }
        match exporters.logs[0] {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter for logs"),
        }
    }

    #[test]
    fn test_get_multi_exporter_config_otlp_with_endpoint() {
        let mut env_manager = EnvManager::new();
        env_manager.set_var("ROTEL_EXPORTER_TEST_ENDPOINT", "http://localhost:4317");

        let config = AgentRun {
            exporters_traces: Some("test".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(&config, "test:otlp".to_string(), "production");

        assert!(result.is_ok());
        let exporters = result.unwrap();
        assert_eq!(1, exporters.traces.len());
        assert!(exporters.metrics.is_empty());
        assert!(exporters.logs.is_empty());

        match &exporters.traces[0] {
            ExporterConfig::Otlp(otlp) => assert_eq!(
                Endpoint::Base("http://localhost:4317".to_string()),
                otlp.endpoint
            ),
            _ => panic!("Expected OTLP exporter"),
        }
    }

    #[test]
    fn test_get_multi_exporter_config_datadog_with_api_key() {
        let mut env_manager = EnvManager::new();
        env_manager.set_var("ROTEL_EXPORTER_DD_API_KEY", "test-api-key");
        env_manager.set_var("ROTEL_EXPORTER_DD_REGION", "us1");

        let config = AgentRun {
            exporters_traces: Some("dd".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(&config, "dd:datadog".to_string(), "production");

        assert!(result.is_ok());
        let exporters = result.unwrap();
        assert_eq!(1, exporters.traces.len());

        match exporters.traces[0] {
            ExporterConfig::Datadog(_) => {}
            _ => panic!("Expected Datadog exporter"),
        }
    }

    #[test]
    fn test_get_multi_exporter_config_datadog_missing_api_key() {
        let mut env_manager = EnvManager::new();
        env_manager.remove_var("ROTEL_EXPORTER_DD_API_KEY");

        let config = AgentRun {
            exporters_traces: Some("dd".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(&config, "dd:datadog".to_string(), "production");

        match result {
            Ok(_) => panic!("should have failed"),
            Err(err) => {
                assert!(
                    err.to_string()
                        .contains("must specify Datadog exporter API key")
                );
            }
        };
    }

    #[test]
    fn test_get_multi_exporter_config_datadog_metrics_not_supported() {
        let mut env_manager = EnvManager::new();
        env_manager.set_var("ROTEL_EXPORTER_DD_API_KEY", "test-api-key");

        let config = AgentRun {
            exporters_metrics: Some("dd".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(&config, "dd:datadog".to_string(), "production");
        match result {
            Ok(_) => panic!("should have failed"),
            Err(err) => {
                assert!(
                    err.to_string()
                        .contains("Datadog exporter not supported for pipeline type metrics")
                );
            }
        };
    }

    #[test]
    fn test_get_multi_exporter_config_multiple_exporters_error() {
        let mut env_manager = EnvManager::new();
        env_manager.set_var("ROTEL_EXPORTER_DD_API_KEY", "test-api-key");
        env_manager.set_var("ROTEL_EXPORTER_DD_REGION", "us1");

        let config = AgentRun {
            exporters_traces: Some("bh,dd".to_string()),
            ..AgentRun::default()
        };

        let result =
            get_multi_exporter_config(&config, "dd:datadog,bh:blackhole".to_string(), "production");

        assert!(result.is_ok());
        let exporters = result.unwrap();

        assert_eq!(2, exporters.traces.len());
        match exporters.traces[0] {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter"),
        }
        match exporters.traces[1] {
            ExporterConfig::Datadog(_) => {}
            _ => panic!("Expected Datadog exporter"),
        }
    }

    #[test]
    fn test_get_multi_exporter_config_exporter_not_found() {
        let config = AgentRun {
            exporters_traces: Some("nonexistent".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(&config, "exp1:blackhole".to_string(), "production");

        match result {
            Ok(_) => panic!("should have failed"),
            Err(err) => {
                assert!(
                    err.to_string()
                        .contains("Can not find exporter nonexistent for traces exporters")
                );
            }
        };
    }

    #[test]
    fn test_get_multi_exporter_config_no_exporters_configured() {
        let config = AgentRun::default();

        let result = get_multi_exporter_config(&config, "exp1:blackhole".to_string(), "production");
        match result {
            Ok(_) => panic!("should have failed"),
            Err(err) => {
                assert!(err.to_string().contains("No telemetry pipeline exporters"));
            }
        };
    }

    #[test]
    fn test_get_multi_exporter_config_mixed_exporters() {
        let mut env_manager = EnvManager::new();
        env_manager.set_var("ROTEL_EXPORTER_DD_API_KEY", "test-api-key");
        env_manager.set_var("ROTEL_EXPORTER_CH_ENDPOINT", "http://localhost:8123");

        let config = AgentRun {
            exporters_traces: Some("dd".to_string()),
            exporters_metrics: Some("ch".to_string()),
            exporters_logs: Some("bh".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(
            &config,
            "dd:datadog,ch:clickhouse,bh:blackhole".to_string(),
            "production",
        );

        assert!(result.is_ok());
        let exporters = result.unwrap();
        assert_eq!(1, exporters.traces.len());
        assert_eq!(1, exporters.metrics.len());
        assert_eq!(1, exporters.logs.len());

        match exporters.traces[0] {
            ExporterConfig::Datadog(_) => {}
            _ => panic!("Expected Datadog exporter for traces"),
        }
        match exporters.metrics[0] {
            ExporterConfig::Clickhouse(_) => {}
            _ => panic!("Expected Clickhouse exporter for metrics"),
        }
        match exporters.logs[0] {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter for logs"),
        }
    }

    #[test]
    fn test_args_from_env_prefix_otlp() {
        let mut env_manager = EnvManager::new();
        env_manager.set_var("ROTEL_EXPORTER_TEST_ENDPOINT", "http://localhost:4317");

        let result = args_from_env_prefix("otlp", "test");

        assert!(result.is_ok());
        match result.unwrap() {
            ExporterArgs::Otlp(_) => {}
            _ => panic!("Expected OTLP args"),
        }
    }

    #[test]
    fn test_args_from_env_prefix_awsxray() {
        let mut env_manager = EnvManager::new();
        env_manager.set_var("ROTEL_EXPORTER_TEST_REGION", "us-west-1");

        let result = args_from_env_prefix("awsxray", "test");

        assert!(result.is_ok());
        match result.unwrap() {
            ExporterArgs::Xray(_) => {}
            _ => panic!("Expected Xray args"),
        }
    }

    #[test]
    fn test_exporter_map_from_str_invalid_format() {
        let result = "exp1:type1:extra".parse::<ExporterMap>();

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("invalid exporter config"));
    }

    #[test]
    fn test_exporter_map_from_str_unknown_type() {
        let result = "exp1:unknown".parse::<ExporterMap>();

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("unknown exporter type: unknown"));
    }

    #[test]
    fn test_get_multi_exporter_config_figment_parse_error() {
        let mut env_manager = EnvManager::new();
        // Set an invalid value that would cause figment parsing to fail
        env_manager.set_var("ROTEL_EXPORTER_TEST_REQUEST_TIMEOUT", "not_a_number");

        let config = AgentRun {
            exporters_traces: Some("test".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(&config, "test:otlp".to_string(), "production");
        match result {
            Ok(_) => panic!("should have failed"),
            Err(err) => {
                assert!(err.to_string().contains("failed to parse OTLP config"));
            }
        };
    }
}
