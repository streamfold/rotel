use crate::exporters::clickhouse::ClickhouseExporterConfigBuilder;
use crate::exporters::datadog::DatadogExporterConfigBuilder;
use crate::exporters::otlp::Endpoint;
use crate::exporters::otlp::config::OTLPExporterConfig;
use crate::exporters::xray::XRayExporterConfigBuilder;
use crate::init::args::{AgentRun, Exporter, parse_bool_value};
use crate::init::otlp_exporter::{build_logs_config, build_metrics_config, build_traces_config};
use gethostname::gethostname;
use tower::BoxError;
use tracing::error;

pub(crate) struct ExporterConfigs {
    pub(crate) metrics: Option<ExporterConfig>,
    pub(crate) logs: Option<ExporterConfig>,
    pub(crate) traces: Option<ExporterConfig>,
}

pub(crate) enum ExporterConfig {
    Blackhole,
    Otlp(OTLPExporterConfig),
    Datadog(DatadogExporterConfigBuilder),
    Clickhouse(ClickhouseExporterConfigBuilder),
    Xray(XRayExporterConfigBuilder),
}

pub(crate) fn get_exporters_config(
    config: &AgentRun,
    environment: &str,
) -> Result<ExporterConfigs, BoxError> {
    let mut cfg = ExporterConfigs {
        metrics: None,
        logs: None,
        traces: None,
    };

    match config.exporter {
        Exporter::Otlp => {
            let endpoint = config.otlp_exporter.base.endpoint.as_ref();
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
