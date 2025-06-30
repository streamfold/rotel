use crate::exporters::clickhouse::ClickhouseExporterConfigBuilder;
use crate::exporters::datadog::DatadogExporterConfigBuilder;
use crate::exporters::otlp::Endpoint;
use crate::exporters::otlp::config::OTLPExporterConfig;
use crate::exporters::xray::XRayExporterConfigBuilder;
use crate::init::args::{AgentRun, Exporter};
use crate::init::clickhouse_exporter::ClickhouseExporterArgs;
use crate::init::datadog_exporter::DatadogExporterArgs;
use crate::init::otlp_exporter::{
    OTLPExporterBaseArgs, build_logs_config, build_metrics_config, build_traces_config,
};
use crate::init::parse::parse_bool_value;
use crate::init::xray_exporter::XRayExporterArgs;
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

pub(crate) struct ExporterConfigs {
    pub(crate) metrics: Option<ExporterConfig>,
    pub(crate) logs: Option<ExporterConfig>,
    pub(crate) traces: Option<ExporterConfig>,
}

#[derive(Debug)]
pub(crate) enum ExporterArgs {
    Blackhole,
    Otlp(OTLPExporterBaseArgs),
    Datadog(DatadogExporterArgs),
    Clickhouse(ClickhouseExporterArgs),
    Xray(XRayExporterArgs),
}

#[derive(PartialEq)]
enum PipelineType {
    Metrics,
    Logs,
    Traces,
}

impl Display for PipelineType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineType::Metrics => write!(f, "metrics"),
            PipelineType::Logs => write!(f, "logs"),
            PipelineType::Traces => write!(f, "traces"),
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
                    PipelineType::Metrics => {
                        if endpoint.is_none() && otlp.metrics_endpoint.is_none() {
                            return Err("must specify an endpoint for OTLP metrics".into());
                        }
                        let endpoint = otlp
                            .metrics_endpoint
                            .as_ref()
                            .map(|e| Endpoint::Full(e.clone()))
                            .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));

                        Ok(ExporterConfig::Otlp(
                            otlp.into_exporter_config("otlp_metrics", endpoint),
                        ))
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
                            otlp.into_exporter_config("otlp_logs", endpoint),
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
                            otlp.into_exporter_config("otlp_traces", endpoint),
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
                .with_json_underscore(ch.json_underscore);

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
        }
    }
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
        return Err("Can not use --exporter and --exporters".into());
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

    let mut cfg = ExporterConfigs {
        metrics: None,
        logs: None,
        traces: None,
    };

    if let Some(traces_exps) = &config.exporters_traces {
        let sp: Vec<&str> = traces_exps.split(",").collect();
        if sp.len() != 1 {
            return Err(format!(
                "Only one exporter supported for ROTEL_EXPORTERS_TRACES: {}",
                traces_exps
            )
            .into());
        }

        let args = match exporter_map.get(&sp[0].to_string()) {
            Some(args) => args,
            None => {
                return Err(format!("Can not find exporter {} for traces exporters", sp[0]).into());
            }
        };

        cfg.traces = Some(
            args.try_into_config(PipelineType::Traces, environment)
                .map_err(|err| format!("Exporter[{}]: {}", sp[0], err))?,
        );
    }

    if let Some(metrics_exps) = &config.exporters_metrics {
        let sp: Vec<&str> = metrics_exps.split(",").collect();
        if sp.len() != 1 {
            return Err(format!(
                "Only one exporter supported for ROTEL_EXPORTERS_METRICS: {}",
                metrics_exps
            )
            .into());
        }

        let args = match exporter_map.get(&sp[0].to_string()) {
            Some(args) => args,
            None => {
                return Err(
                    format!("Can not find exporter {} for metrics exporters", sp[0]).into(),
                );
            }
        };

        cfg.metrics = Some(
            args.try_into_config(PipelineType::Metrics, environment)
                .map_err(|err| format!("Exporter[{}]: {}", sp[0], err))?,
        );
    }

    if let Some(logs_exps) = &config.exporters_logs {
        let sp: Vec<&str> = logs_exps.split(",").collect();
        if sp.len() != 1 {
            return Err(format!(
                "Only one exporter supported for ROTEL_EXPORTERS_LOGS: {}",
                logs_exps
            )
            .into());
        }

        let args = match exporter_map.get(&sp[0].to_string()) {
            Some(args) => args,
            None => {
                return Err(format!("Can not find exporter {} for logs exporters", sp[0]).into());
            }
        };

        cfg.logs = Some(
            args.try_into_config(PipelineType::Logs, environment)
                .map_err(|err| format!("Exporter[{}]: {}", sp[0], err))?,
        );
    }

    if cfg.traces.is_none() && cfg.metrics.is_none() && cfg.logs.is_none() {
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
        "xray" => {
            let args: XRayExporterArgs = match figment.extract() {
                Ok(args) => args,
                Err(e) => return Err(format!("failed to parse X-Ray config: {}", e).into()),
            };

            Ok(ExporterArgs::Xray(args))
        }
        _ => Err(format!("unknown exporter type: {}", exporter_type).into()),
    }
}

fn get_single_exporter_config(
    config: &AgentRun,
    exporter: Exporter,
    environment: &str,
) -> Result<ExporterConfigs, BoxError> {
    let mut cfg = ExporterConfigs {
        metrics: None,
        logs: None,
        traces: None,
    };

    match exporter {
        Exporter::Otlp => {
            let endpoint = config.otlp_exporter.base.endpoint.as_ref();

            if endpoint.is_some() || config.otlp_exporter.base.traces_endpoint.is_some() {
                cfg.traces = Some(ExporterConfig::Otlp({
                    let endpoint = config
                        .otlp_exporter
                        .base
                        .traces_endpoint
                        .as_ref()
                        .map(|e| Endpoint::Full(e.clone()))
                        .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));
                    let traces_config = build_traces_config(config.otlp_exporter.clone());
                    traces_config.into_exporter_config("otlp_traces", endpoint)
                }));
            }
            if endpoint.is_some() || config.otlp_exporter.base.metrics_endpoint.is_some() {
                cfg.metrics = Some(ExporterConfig::Otlp({
                    let endpoint = config
                        .otlp_exporter
                        .base
                        .metrics_endpoint
                        .as_ref()
                        .map(|e| Endpoint::Full(e.clone()))
                        .unwrap_or_else(|| Endpoint::Base(endpoint.clone().unwrap().clone()));

                    let metrics_config = build_metrics_config(config.otlp_exporter.clone());
                    metrics_config
                        .clone()
                        .into_exporter_config("otlp_metrics", endpoint.clone())
                }));
            }
            if endpoint.is_some() || config.otlp_exporter.base.logs_endpoint.is_some() {
                cfg.logs = Some(ExporterConfig::Otlp({
                    let endpoint = config
                        .otlp_exporter
                        .base
                        .logs_endpoint
                        .as_ref()
                        .map(|e| Endpoint::Full(e.clone()))
                        .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));

                    let logs_config = build_logs_config(config.otlp_exporter.clone());
                    logs_config.into_exporter_config("otlp_logs", endpoint)
                }));
            }
        }
        Exporter::Blackhole => {
            cfg.traces = Some(ExporterConfig::Blackhole {});
            cfg.metrics = Some(ExporterConfig::Blackhole {});
            cfg.logs = Some(ExporterConfig::Blackhole {});
        }
        Exporter::Datadog => {
            if config.datadog_exporter.api_key.is_none() {
                // todo: is there a way to make this config required with the exporter mode?
                return Err("must specify Datadog exporter API key".into());
            }
            let api_key = config.datadog_exporter.api_key.as_ref().unwrap();

            let hostname = get_hostname();

            let mut builder = DatadogExporterConfigBuilder::new(
                config.datadog_exporter.region.into(),
                config.datadog_exporter.custom_endpoint.clone(),
                api_key.clone(),
            )
            .with_environment(environment.to_string());

            if let Some(hostname) = hostname {
                builder = builder.with_hostname(hostname);
            }

            cfg.traces = Some(ExporterConfig::Datadog(builder))
        }
        Exporter::Clickhouse => {
            if config.clickhouse_exporter.endpoint.is_none() {
                return Err("must specify a Clickhouse exporter endpoint".into());
            }

            let async_insert = parse_bool_value(&config.clickhouse_exporter.async_insert)?;

            let mut cfg_builder = ClickhouseExporterConfigBuilder::new(
                config
                    .clickhouse_exporter
                    .endpoint
                    .as_ref()
                    .unwrap()
                    .clone(),
                config.clickhouse_exporter.database.clone(),
                config.clickhouse_exporter.table_prefix.clone(),
            )
            .with_compression(config.clickhouse_exporter.compression)
            .with_async_insert(async_insert)
            .with_json(config.clickhouse_exporter.enable_json)
            .with_json_underscore(config.clickhouse_exporter.json_underscore);

            if let Some(user) = &config.clickhouse_exporter.user {
                cfg_builder = cfg_builder.with_user(user.clone());
            }

            if let Some(password) = &config.clickhouse_exporter.password {
                cfg_builder = cfg_builder.with_password(password.clone());
            }

            cfg.traces = Some(ExporterConfig::Clickhouse(cfg_builder.clone()));
            cfg.metrics = Some(ExporterConfig::Clickhouse(cfg_builder.clone()));
            cfg.logs = Some(ExporterConfig::Clickhouse(cfg_builder));
        }
        Exporter::AwsXray => {
            let builder = XRayExporterConfigBuilder::new(
                config.aws_xray_exporter.region,
                config.aws_xray_exporter.custom_endpoint.clone(),
            );

            cfg.traces = Some(ExporterConfig::Xray(builder))
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
        assert!(exporters.traces.is_some());
        assert!(exporters.metrics.is_none());
        assert!(exporters.logs.is_none());

        match exporters.traces.unwrap() {
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
        assert!(exporters.traces.is_some());
        assert!(exporters.metrics.is_some());
        assert!(exporters.logs.is_some());

        match exporters.traces.unwrap() {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter for traces"),
        }
        match exporters.metrics.unwrap() {
            ExporterConfig::Blackhole => {}
            _ => panic!("Expected Blackhole exporter for metrics"),
        }
        match exporters.logs.unwrap() {
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
        assert!(exporters.traces.is_some());
        assert!(exporters.metrics.is_none());
        assert!(exporters.logs.is_none());

        match exporters.traces.unwrap() {
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
        assert!(exporters.traces.is_some());

        match exporters.traces.unwrap() {
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
        let config = AgentRun {
            exporters_traces: Some("exp1,exp2".to_string()),
            ..AgentRun::default()
        };

        let result = get_multi_exporter_config(
            &config,
            "exp1:blackhole,exp2:blackhole".to_string(),
            "production",
        );

        match result {
            Ok(_) => panic!("should have failed"),
            Err(err) => {
                assert!(
                    err.to_string()
                        .contains("Only one exporter supported for ROTEL_EXPORTERS_TRACES")
                );
            }
        };
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
        assert!(exporters.traces.is_some());
        assert!(exporters.metrics.is_some());
        assert!(exporters.logs.is_some());

        match exporters.traces.unwrap() {
            ExporterConfig::Datadog(_) => {}
            _ => panic!("Expected Datadog exporter for traces"),
        }
        match exporters.metrics.unwrap() {
            ExporterConfig::Clickhouse(_) => {}
            _ => panic!("Expected Clickhouse exporter for metrics"),
        }
        match exporters.logs.unwrap() {
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

        let result = args_from_env_prefix("xray", "test");

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
