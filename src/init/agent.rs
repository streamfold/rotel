use crate::aws_api::config::AwsConfig;
use crate::bounded_channel::{BoundedReceiver, bounded};
use crate::crypto::init_crypto_provider;
use crate::exporters::blackhole::BlackholeExporter;
use crate::exporters::clickhouse::ClickhouseExporterConfigBuilder;
use crate::exporters::datadog::{DatadogTraceExporterBuilder, Region};
use crate::exporters::kafka::KafkaExporter;
use crate::exporters::otlp;
use crate::exporters::otlp::Endpoint;
use crate::exporters::xray::XRayTraceExporterBuilder;
use crate::init::activation::{TelemetryActivation, TelemetryState};
use crate::init::args::{AgentRun, DebugLogParam, Exporter, parse_bool_value};
use crate::init::batch::{
    build_logs_batch_config, build_metrics_batch_config, build_traces_batch_config,
};
use crate::init::datadog_exporter::DatadogRegion;
use crate::init::otlp_exporter::{build_logs_config, build_metrics_config, build_traces_config};
#[cfg(feature = "pprof")]
use crate::init::pprof;
use crate::init::wait;
use crate::listener::Listener;
use crate::receivers::otlp_grpc::OTLPGrpcServer;
use crate::receivers::otlp_http::OTLPHttpServer;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::debug::DebugLogger;
use crate::topology::flush_control::FlushSubscriber;
use crate::{telemetry, topology};
use gethostname::gethostname;
use opentelemetry::global;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, Temporality};
use std::cmp::max;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::log::warn;
use tracing::{debug, error, info};

pub struct Agent {
    config: Box<AgentRun>,
    port_map: HashMap<SocketAddr, Listener>,
    sending_queue_size: usize,
    environment: String,
    logs_rx: Option<BoundedReceiver<ResourceLogs>>,
    pipeline_flush_sub: Option<FlushSubscriber>,
    exporters_flush_sub: Option<FlushSubscriber>,
}

impl Agent {
    pub fn new(
        config: Box<AgentRun>,
        port_map: HashMap<SocketAddr, Listener>,
        sending_queue_size: usize,
        environment: String,
    ) -> Self {
        Self {
            config,
            port_map,
            sending_queue_size,
            environment,
            logs_rx: None,
            pipeline_flush_sub: None,
            exporters_flush_sub: None,
        }
    }

    pub fn with_logs_rx(mut self, logs_rx: BoundedReceiver<ResourceLogs>) -> Self {
        self.logs_rx = Some(logs_rx);
        self
    }

    pub fn with_pipeline_flush(mut self, pipeline_flush_sub: FlushSubscriber) -> Self {
        self.pipeline_flush_sub = Some(pipeline_flush_sub);
        self
    }

    pub fn with_exporters_flush(mut self, exporters_flush_sub: FlushSubscriber) -> Self {
        self.exporters_flush_sub = Some(exporters_flush_sub);
        self
    }

    pub async fn run(
        mut self,
        agent_cancel: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let config = self.config;

        info!(
            grpc_endpoint = config.otlp_grpc_endpoint.to_string(),
            http_endpoint = config.otlp_http_endpoint.to_string(),
            "Starting Rotel.",
        );

        // Initialize the TLS library, we may want to do this conditionally
        init_crypto_provider()?;

        let num_cpus = num_cpus::get();

        let mut receivers_task_set = JoinSet::new();
        let mut pipeline_task_set = JoinSet::new();
        let mut exporters_task_set = JoinSet::new();

        let receivers_cancel = CancellationToken::new();
        let pipeline_cancel = CancellationToken::new();
        let exporters_cancel = CancellationToken::new();

        let activation = TelemetryActivation::from_config(&config);

        // If there are no listeners, suggest the blackhole exporter
        if activation.traces == TelemetryState::NoListeners
            && activation.metrics == TelemetryState::NoListeners
            && activation.logs == TelemetryState::NoListeners
        {
            return Err(
                "no exporter endpoints specified, perhaps you meant to use --exporter blackhole instead"
                    .into(),
            );
        }

        // If no active type exists, nothing to do. Exit here before errors later
        if !(activation.traces == TelemetryState::Active
            || activation.metrics == TelemetryState::Active
            || activation.logs == TelemetryState::Active)
        {
            return Err(
                "there are no active telemetry types, exiting because there is nothing to do"
                    .into(),
            );
        }

        let (trace_pipeline_in_tx, trace_pipeline_in_rx) =
            bounded::<Vec<ResourceSpans>>(max(4, num_cpus));
        let (trace_pipeline_out_tx, trace_pipeline_out_rx) =
            bounded::<Vec<ResourceSpans>>(self.sending_queue_size);
        let trace_otlp_output = OTLPOutput::new(trace_pipeline_in_tx);

        let (metrics_pipeline_in_tx, metrics_pipeline_in_rx) =
            bounded::<Vec<ResourceMetrics>>(max(4, num_cpus));
        let (metrics_pipeline_out_tx, metrics_pipeline_out_rx) =
            bounded::<Vec<ResourceMetrics>>(self.sending_queue_size);
        let metrics_otlp_output = OTLPOutput::new(metrics_pipeline_in_tx);

        let (logs_pipeline_in_tx, logs_pipeline_in_rx) =
            bounded::<Vec<ResourceLogs>>(max(4, num_cpus));
        let (logs_pipeline_out_tx, logs_pipeline_out_rx) =
            bounded::<Vec<ResourceLogs>>(self.sending_queue_size);
        let logs_otlp_output = OTLPOutput::new(logs_pipeline_in_tx);

        let (internal_metrics_pipeline_in_tx, internal_metrics_pipeline_in_rx) =
            bounded::<Vec<ResourceMetrics>>(max(4, num_cpus));
        let (internal_metrics_pipeline_out_tx, internal_metrics_pipeline_out_rx) =
            bounded::<Vec<ResourceMetrics>>(self.sending_queue_size);
        let internal_metrics_otlp_output = OTLPOutput::new(internal_metrics_pipeline_in_tx);

        let mut traces_output = None;
        let mut metrics_output = None;
        let mut logs_output = None;
        let mut internal_metrics_output = None;

        match activation.traces {
            TelemetryState::Active => traces_output = Some(trace_otlp_output),
            TelemetryState::Disabled => {
                info!(
                    "OTLP Receiver for traces disabled, OTLP receiver will be configured to not accept traces"
                );
            }
            TelemetryState::NoListeners => {
                info!(
                    "No exporters are configured for traces, OTLP receiver will be configured to not accept traces"
                );
            }
        }

        match activation.metrics {
            TelemetryState::Active => {
                metrics_output = Some(metrics_otlp_output);
                internal_metrics_output = Some(internal_metrics_otlp_output);
            }
            TelemetryState::Disabled => {
                info!(
                    "OTLP Receiver for metrics disabled, OTLP receiver will be configured to not accept metrics"
                );
            }
            TelemetryState::NoListeners => {
                info!(
                    "No exporters are configured for metrics, OTLP receiver will be configured to not accept metrics"
                );
            }
        }

        match activation.logs {
            TelemetryState::Active => logs_output = Some(logs_otlp_output),
            TelemetryState::Disabled => {
                info!(
                    "OTLP Receiver for logs disabled, OTLP receiver will be configured to not accept logs"
                );
            }
            TelemetryState::NoListeners => {
                info!(
                    "No exporters are configured for logs, OTLP receiver will be configured to not accept logs"
                );
            }
        }

        if !config.enable_internal_telemetry {
            internal_metrics_output = None;
        }

        let mut pipeline_flush_sub = self.pipeline_flush_sub.take();

        // AWS-XRay only supports a batch size of 50 segments
        let mut trace_batch_config = build_traces_batch_config(config.batch.clone());
        if config.exporter == Exporter::AwsXray && trace_batch_config.max_size > 50 {
            info!(
                "AWS X-Ray only supports a batch size of 50 segments, setting batch max size to 50"
            );
            trace_batch_config.max_size = 50;
        }

        // Internal metrics
        // N.B Internal metrics initialization MUST be done before starting other parts of the agent such as
        // receiver and exporters, so that the global meter provider is set before those components attempt to
        // create instruments such as counters, etc. Be careful when refactoring this code to avoid breaking
        // this dependency.
        //

        let internal_metrics_sdk_exporter =
            telemetry::internal_exporter::InternalOTLPMetricsExporter::new(
                internal_metrics_output.clone(),
                Temporality::Cumulative,
            );

        let periodic_reader = PeriodicReader::builder(internal_metrics_sdk_exporter)
            .with_interval(Duration::from_secs(10))
            .build();

        let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(periodic_reader)
            .with_resource(Resource::builder().with_service_name("rotel").build())
            .build();

        global::set_meter_provider(meter_provider);

        let token = exporters_cancel.clone();
        match config.exporter {
            Exporter::Blackhole => {
                let mut exp = BlackholeExporter::new(
                    trace_pipeline_out_rx,
                    metrics_pipeline_out_rx,
                    logs_pipeline_out_rx,
                );

                exporters_task_set.spawn(async move {
                    exp.start(token).await;
                    Ok(())
                });
            }
            Exporter::Otlp => {
                let endpoint = config.otlp_exporter.base.endpoint.as_ref();
                if activation.traces == TelemetryState::Active {
                    let endpoint = config
                        .otlp_exporter
                        .base
                        .traces_endpoint
                        .as_ref()
                        .map(|e| Endpoint::Full(e.clone()))
                        .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));
                    let traces_config = build_traces_config(config.otlp_exporter.clone());
                    let mut traces = otlp::exporter::build_traces_exporter(
                        traces_config.into_exporter_config("otlp_traces", endpoint),
                        trace_pipeline_out_rx,
                        self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    )?;
                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        let res = traces.start(token).await;
                        if let Err(e) = res {
                            error!(
                                exporter_type = "otlp_traces",
                                error = e,
                                "OTLPExporter exporter returned from run loop with error."
                            );
                        }

                        Ok(())
                    });
                }
                if activation.metrics == TelemetryState::Active {
                    let endpoint = config
                        .otlp_exporter
                        .base
                        .metrics_endpoint
                        .as_ref()
                        .map(|e| Endpoint::Full(e.clone()))
                        .unwrap_or_else(|| Endpoint::Base(endpoint.clone().unwrap().clone()));

                    let metrics_config = build_metrics_config(config.otlp_exporter.clone());
                    let mut metrics = otlp::exporter::build_metrics_exporter(
                        metrics_config
                            .clone()
                            .into_exporter_config("otlp_metrics", endpoint.clone()),
                        metrics_pipeline_out_rx,
                        self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    )?;
                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        let res = metrics.start(token).await;
                        if let Err(e) = res {
                            error!(
                                exporter_type = "otlp_metrics",
                                error = e,
                                "OTLPExporter returned from run loop with error."
                            );
                        }

                        Ok(())
                    });

                    if config.enable_internal_telemetry {
                        let mut internal_metrics = otlp::exporter::build_internal_metrics_exporter(
                            metrics_config.into_exporter_config("otlp_metrics", endpoint),
                            internal_metrics_pipeline_out_rx,
                            self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                        )?;
                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            let res = internal_metrics.start(token).await;
                            if let Err(e) = res {
                                error!(
                                    exporter_type = "internal_otlp_metrics",
                                    error = e,
                                    "OTLPExporter returned from run loop with error."
                                );
                            }

                            Ok(())
                        });
                    }
                }
                if activation.logs == TelemetryState::Active {
                    let endpoint = config
                        .otlp_exporter
                        .base
                        .logs_endpoint
                        .as_ref()
                        .map(|e| Endpoint::Full(e.clone()))
                        .unwrap_or_else(|| Endpoint::Base(endpoint.unwrap().clone()));

                    let logs_config = build_logs_config(config.otlp_exporter.clone());
                    let mut logs = otlp::exporter::build_logs_exporter(
                        logs_config.into_exporter_config("otlp_logs", endpoint),
                        logs_pipeline_out_rx.clone(),
                        self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    )?;
                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        let res = logs.start(token).await;
                        if let Err(e) = res {
                            error!(
                                exporter_type = "otlp_logs",
                                error = e,
                                "OTLPExporter returned from run loop with error."
                            );
                        }

                        Ok(())
                    });
                }
            }

            Exporter::Datadog => {
                if config.datadog_exporter.api_key.is_none() {
                    // todo: is there a way to make this config required with the exporter mode?
                    return Err("must specify Datadog exporter API key".into());
                }
                let api_key = config.datadog_exporter.api_key.unwrap();

                let hostname = get_hostname();

                let mut builder = DatadogTraceExporterBuilder::new(
                    config.datadog_exporter.region.into(),
                    config.datadog_exporter.custom_endpoint.clone(),
                    api_key,
                )
                .with_environment(self.environment.clone());

                if let Some(hostname) = hostname {
                    builder = builder.with_hostname(hostname);
                }

                let exp = builder.build(
                    trace_pipeline_out_rx,
                    self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                )?;

                exporters_task_set.spawn(async move {
                    let res = exp.start(token).await;
                    if let Err(e) = res {
                        error!(
                            error = e,
                            "Datadog exporter returned from run loop with error."
                        );
                    }

                    Ok(())
                });
            }

            Exporter::AwsXray => {
                let builder = XRayTraceExporterBuilder::new(
                    config.aws_xray_exporter.region,
                    config.aws_xray_exporter.custom_endpoint,
                );
                let config = AwsConfig::from_env();
                let exp = builder.build(
                    trace_pipeline_out_rx,
                    self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    "production".to_string(),
                    config,
                )?;

                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    let res = exp.start(token).await;
                    if let Err(e) = res {
                        error!(
                            error = e,
                            "AWS X-Ray exporter returned from run loop with error."
                        );
                    }
                    Ok(())
                });
            }

            Exporter::Clickhouse => {
                if config.clickhouse_exporter.endpoint.is_none() {
                    return Err("must specify a Clickhouse exporter endpoint".into());
                }

                let async_insert = parse_bool_value(config.clickhouse_exporter.async_insert)?;

                let mut cfg_builder = ClickhouseExporterConfigBuilder::new(
                    config.clickhouse_exporter.endpoint.unwrap(),
                    config.clickhouse_exporter.database,
                    config.clickhouse_exporter.table_prefix,
                )
                .with_compression(config.clickhouse_exporter.compression)
                .with_async_insert(async_insert)
                .with_json(config.clickhouse_exporter.enable_json)
                .with_json_underscore(config.clickhouse_exporter.json_underscore);

                if let Some(user) = config.clickhouse_exporter.user {
                    cfg_builder = cfg_builder.with_user(user);
                }

                if let Some(password) = config.clickhouse_exporter.password {
                    cfg_builder = cfg_builder.with_password(password);
                }

                let builder = cfg_builder.build()?;

                // Trace spans
                let exp = builder.build_traces_exporter(
                    trace_pipeline_out_rx,
                    self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                )?;

                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    let res = exp.start(token).await;
                    if let Err(e) = res {
                        error!(
                            error = e,
                            "Clickhouse traces exporter returned from run loop with error."
                        );
                    }

                    Ok(())
                });

                // Log records
                let exp = builder.build_logs_exporter(
                    logs_pipeline_out_rx,
                    self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                )?;

                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    let res = exp.start(token).await;
                    if let Err(e) = res {
                        error!(
                            error = e,
                            "Clickhouse logs exporter returned from run loop with error."
                        );
                    }

                    Ok(())
                });

                // Metrics
                let exp = builder.build_metrics_exporter(
                    metrics_pipeline_out_rx,
                    self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                )?;

                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    let res = exp.start(token).await;
                    if let Err(e) = res {
                        error!(
                            error = e,
                            "Clickhouse metrics exporter returned from run loop with error."
                        );
                    }

                    Ok(())
                });
            }
            
            Exporter::Kafka => {
                let kafka_config = config.kafka_exporter.build_config();
                
                let mut kafka_exporter = KafkaExporter::new(
                    kafka_config,
                    trace_pipeline_out_rx,
                    metrics_pipeline_out_rx,
                    logs_pipeline_out_rx,
                )?;
                
                let token = exporters_cancel.clone();
                exporters_task_set.spawn(async move {
                    kafka_exporter.start(token).await;
                    Ok(())
                });
            }
        }

        if traces_output.is_some() {
            let mut trace_pipeline = topology::generic_pipeline::Pipeline::new(
                trace_pipeline_in_rx.clone(),
                trace_pipeline_out_tx,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                trace_batch_config,
                config.otlp_with_trace_processor.clone(),
                config.otel_resource_attributes.clone(),
            );

            let log_traces = config.debug_log.contains(&DebugLogParam::Traces);
            let dbg_log = DebugLogger::new(
                log_traces
                    .then_some(config.debug_log_verbosity)
                    .map(|v| v.into()),
            );

            let pipeline_cancel = pipeline_cancel.clone();
            pipeline_task_set
                .spawn(async move { trace_pipeline.start(dbg_log, pipeline_cancel).await });
        }

        if metrics_output.is_some() {
            let mut metrics_pipeline = topology::generic_pipeline::Pipeline::new(
                metrics_pipeline_in_rx.clone(),
                metrics_pipeline_out_tx,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_metrics_batch_config(config.batch.clone()),
                config.otlp_with_metrics_processor.clone(),
                config.otel_resource_attributes.clone(),
            );

            let log_metrics = config.debug_log.contains(&DebugLogParam::Metrics);
            let dbg_log = DebugLogger::new(
                log_metrics
                    .then_some(config.debug_log_verbosity)
                    .map(|v| v.into()),
            );

            let pipeline_cancel = pipeline_cancel.clone();
            pipeline_task_set
                .spawn(async move { metrics_pipeline.start(dbg_log, pipeline_cancel).await });
        }

        if logs_output.is_some() {
            let mut logs_pipeline = topology::generic_pipeline::Pipeline::new(
                logs_pipeline_in_rx.clone(),
                logs_pipeline_out_tx,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_logs_batch_config(config.batch.clone()),
                config.otlp_with_logs_processor.clone(),
                config.otel_resource_attributes.clone(),
            );

            let log_logs = config.debug_log.contains(&DebugLogParam::Logs);
            let dbg_log = DebugLogger::new(
                log_logs
                    .then_some(config.debug_log_verbosity)
                    .map(|v| v.into()),
            );

            let pipeline_cancel = pipeline_cancel.clone();
            pipeline_task_set
                .spawn(async move { logs_pipeline.start(dbg_log, pipeline_cancel).await });
        }

        if internal_metrics_output.is_some() {
            let mut internal_metrics_pipeline = topology::generic_pipeline::Pipeline::new(
                internal_metrics_pipeline_in_rx.clone(),
                internal_metrics_pipeline_out_tx,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_metrics_batch_config(config.batch.clone()),
                vec![],
                config.otel_resource_attributes.clone(),
            );

            let log_metrics = config.debug_log.contains(&DebugLogParam::Metrics);
            let dbg_log = DebugLogger::new(
                log_metrics
                    .then_some(config.debug_log_verbosity)
                    .map(|v| v.into()),
            );

            let pipeline_cancel = pipeline_cancel.clone();
            pipeline_task_set.spawn(async move {
                internal_metrics_pipeline
                    .start(dbg_log, pipeline_cancel)
                    .await
            });
        }

        //
        // OTLP GRPC server
        //
        let grpc_srv = OTLPGrpcServer::builder()
            .with_max_recv_msg_size_mib(config.otlp_grpc_max_recv_msg_size_mib as usize)
            .with_traces_output(traces_output.clone())
            .with_metrics_output(metrics_output.clone())
            .with_logs_output(logs_output.clone())
            .build();

        let grpc_listener = self.port_map.remove(&config.otlp_grpc_endpoint).unwrap();
        {
            let receivers_cancel = receivers_cancel.clone();
            receivers_task_set
                .spawn(async move { grpc_srv.serve(grpc_listener, receivers_cancel).await });
        }

        //
        // OTLP HTTP server
        //
        let http_srv = OTLPHttpServer::builder()
            .with_traces_output(traces_output.clone())
            .with_metrics_output(metrics_output.clone())
            .with_logs_output(logs_output.clone())
            .with_traces_path(config.otlp_receiver_traces_http_path.clone())
            .with_metrics_path(config.otlp_receiver_metrics_http_path.clone())
            .with_logs_path(config.otlp_receiver_logs_http_path.clone())
            .build();

        let http_listener = self.port_map.remove(&config.otlp_http_endpoint).unwrap();
        {
            let receivers_cancel = receivers_cancel.clone();
            receivers_task_set
                .spawn(async move { http_srv.serve(http_listener, receivers_cancel).await });
        }

        //
        // Logs input receiver
        //
        if let Some(mut logs_rx) = self.logs_rx {
            let receivers_cancel = receivers_cancel.clone();
            let logs_output = logs_output.clone();

            receivers_task_set.spawn(async move {
                loop {
                    select! {
                        rl = logs_rx.next() => {
                            match rl {
                                None => break,
                                Some(rl) => {
                                    if let Some(out) = &logs_output {
                                        if let Err(e) = out.send(vec![rl]).await {
                                            // todo: is this possibly in a logging loop path?
                                            warn!("Unable to send logs to logs output: {}", e)
                                        }
                                    }
                                }
                            }
                        },
                        _ = receivers_cancel.cancelled() => break,
                    }
                }
                Ok(())
            });
        }

        #[cfg(feature = "pprof")]
        let guard =
            if config.profile_group.pprof_flame_graph || config.profile_group.pprof_call_graph {
                pprof::pprof_guard()
            } else {
                None
            };

        let mut result = Ok(());
        select! {
            _ = agent_cancel.cancelled() => {
                debug!("Agent cancellation signaled.");

                #[cfg(feature = "pprof")]
                if config.profile_group.pprof_flame_graph || config.profile_group.pprof_call_graph {
                    pprof::pprof_finish(guard, config.profile_group.pprof_flame_graph, config.profile_group.pprof_call_graph);
                }
            },
            e = wait::wait_for_any_task(&mut receivers_task_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of receiver."),
                    Err(e) => result = Err(e),
                }
            },
            e = wait::wait_for_any_task(&mut pipeline_task_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of pipeline."),
                    Err(e) => result = Err(e),
                }
            },
            e = wait::wait_for_any_task(&mut exporters_task_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of task."),
                    Err(e) => result = Err(e),
                }
            }
        }
        result?;

        // Step one, cancel the receivers and wait for their termination.
        receivers_cancel.cancel();

        // Wait up until one second for receivers to finish
        let res =
            wait::wait_for_tasks_with_timeout(&mut receivers_task_set, Duration::from_secs(1))
                .await;
        if let Err(e) = res {
            return Err(format!("timed out waiting for receiver exit: {}", e).into());
        }

        // Drop the outputs (alternatively move them into receivers?), causing downstream
        // components to exit
        drop(traces_output);
        drop(metrics_output);
        drop(logs_output);
        drop(internal_metrics_output);

        // Construct a noop meter provider that will allow all pipelines to drop their input channels
        let noop_meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        global::set_meter_provider(noop_meter_provider);

        // Set a maximum duration for exporters to exit, this way if the pipelines exit quickly,
        // the entire wall time is left for exporters to finish flushing (which may require longer if
        // endpoints are slow).
        let receivers_hard_stop = Instant::now() + Duration::from_secs(3);

        // Wait 500ms for the pipelines to finish. They should exit when the pipes are dropped.
        let res =
            wait::wait_for_tasks_with_timeout(&mut pipeline_task_set, Duration::from_millis(500))
                .await;
        if res.is_err() {
            warn!("Pipelines did not exit on channel close, cancelling.");

            // force cancel
            pipeline_cancel.cancel();

            // try again
            let res = wait::wait_for_tasks_with_timeout(
                &mut pipeline_task_set,
                Duration::from_millis(500),
            )
            .await;
            if let Err(e) = res {
                return Err(format!("timed out waiting for pipline to exit: {}", e).into());
            }
        }

        // pipeline outputs are already moved, so should be closed

        // Wait for the exporters using the same process
        let res =
            wait::wait_for_tasks_with_timeout(&mut exporters_task_set, Duration::from_millis(500))
                .await;
        if res.is_err() {
            warn!("Exporters did not exit on channel close, cancelling.");

            // force cancel
            exporters_cancel.cancel();

            let res =
                wait::wait_for_tasks_with_deadline(&mut exporters_task_set, receivers_hard_stop)
                    .await;
            if let Err(e) = res {
                return Err(format!("timed out waiting for exporters to exit: {}", e).into());
            }
        }

        Ok(())
    }
}

impl From<DatadogRegion> for Region {
    fn from(value: DatadogRegion) -> Self {
        match value {
            DatadogRegion::US1 => Region::US1,
            DatadogRegion::US3 => Region::US3,
            DatadogRegion::US5 => Region::US5,
            DatadogRegion::EU => Region::EU,
            DatadogRegion::AP1 => Region::AP1,
        }
    }
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
