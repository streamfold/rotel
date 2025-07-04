use crate::aws_api::config::AwsConfig;
use crate::bounded_channel::{BoundedReceiver, bounded};
use crate::crypto::init_crypto_provider;
use crate::exporters::blackhole::BlackholeExporter;
use crate::exporters::datadog::Region;
#[cfg(feature = "rdkafka")]
use crate::exporters::kafka::{build_logs_exporter, build_metrics_exporter, build_traces_exporter};
use crate::exporters::otlp;
use crate::exporters::otlp::signer::AwsSigv4RequestSigner;
use crate::init::activation::{TelemetryActivation, TelemetryState};
use crate::init::args::{AgentRun, DebugLogParam};
use crate::init::batch::{
    build_logs_batch_config, build_metrics_batch_config, build_traces_batch_config,
};
use crate::init::config::{ExporterConfig, get_exporters_config};
use crate::init::datadog_exporter::DatadogRegion;
#[cfg(feature = "pprof")]
use crate::init::pprof;
use crate::init::wait;
use crate::listener::Listener;
use crate::receivers::otlp_grpc::OTLPGrpcServer;
use crate::receivers::otlp_http::OTLPHttpServer;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::batch::BatchSizer;
use crate::topology::debug::DebugLogger;
use crate::topology::flush_control::FlushSubscriber;
use crate::{telemetry, topology};
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

        let exp_config = get_exporters_config(&config, &self.environment)?;

        let activation = TelemetryActivation::from_config(&config, &exp_config);

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
        if let Some(ExporterConfig::Xray(_)) = exp_config.traces {
            if trace_batch_config.max_size > 50 {
                info!(
                    "AWS X-Ray only supports a batch size of 50 segments, setting batch max size to 50"
                );
                trace_batch_config.max_size = 50;
            }
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

        //
        // Build the exporters now
        //

        //
        // TRACES
        //
        if activation.traces == TelemetryState::Active {
            match exp_config.traces {
                Some(ExporterConfig::Otlp(exp_config)) => {
                    let traces = otlp::exporter::build_traces_exporter(
                        exp_config,
                        trace_pipeline_out_rx,
                        self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    )?;

                    start_otlp_exporter(
                        &mut exporters_task_set,
                        "otlp_traces",
                        traces,
                        exporters_cancel.clone(),
                    );
                }
                Some(ExporterConfig::Clickhouse(cfg_builder)) => {
                    let builder = cfg_builder.build()?;

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
                                exporter_type = "clickhouse_traces",
                                "Clickhouse exporter returned from run loop with error."
                            );
                        }

                        Ok(())
                    });
                }
                Some(ExporterConfig::Datadog(cfg_builder)) => {
                    let builder = cfg_builder.build();

                    let exp = builder.build(
                        trace_pipeline_out_rx,
                        self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    )?;

                    let token = exporters_cancel.clone();
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
                Some(ExporterConfig::Xray(cfg_builder)) => {
                    let config = AwsConfig::from_env();
                    let builder = cfg_builder.build();
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
                Some(ExporterConfig::Blackhole) => {
                    let mut exp = BlackholeExporter::new(trace_pipeline_out_rx);

                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        exp.start(token).await;
                        Ok(())
                    });
                }
                #[cfg(feature = "rdkafka")]
                Some(ExporterConfig::Kafka(kafka_config)) => {
                    let mut traces_exporter =
                        build_traces_exporter(kafka_config, trace_pipeline_out_rx)?;
                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        traces_exporter.start(token).await;
                        Ok(())
                    });
                }
                None => {}
            }
        }

        //
        // METRICS
        //
        if activation.metrics == TelemetryState::Active {
            match exp_config.metrics {
                Some(ExporterConfig::Otlp(exp_config)) => {
                    let metrics = otlp::exporter::build_metrics_exporter(
                        exp_config.clone(),
                        metrics_pipeline_out_rx,
                        self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    )?;

                    start_otlp_exporter(
                        &mut exporters_task_set,
                        "otlp_metrics",
                        metrics,
                        exporters_cancel.clone(),
                    );

                    // TODO: Allow internal metrics pipeline to be configured separately?
                    if config.enable_internal_telemetry {
                        let internal_metrics = otlp::exporter::build_internal_metrics_exporter(
                            exp_config,
                            internal_metrics_pipeline_out_rx,
                            self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                        )?;

                        start_otlp_exporter(
                            &mut exporters_task_set,
                            "otlp_internal_metrics",
                            internal_metrics,
                            exporters_cancel.clone(),
                        );
                    }
                }
                Some(ExporterConfig::Clickhouse(cfg_builder)) => {
                    let builder = cfg_builder.build()?;

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
                                exporter_type = "clickhouse_metrics",
                                "Clickhouse exporter returned from run loop with error."
                            );
                        }

                        Ok(())
                    });
                }
                Some(ExporterConfig::Blackhole) => {
                    let mut exp = BlackholeExporter::new(metrics_pipeline_out_rx);

                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        exp.start(token).await;
                        Ok(())
                    });
                }
                #[cfg(feature = "rdkafka")]
                Some(ExporterConfig::Kafka(kafka_config)) => {
                    let mut metrics_exporter =
                        build_metrics_exporter(kafka_config, metrics_pipeline_out_rx)?;
                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        metrics_exporter.start(token).await;
                        Ok(())
                    });
                }
                _ => {}
            }
        }

        //
        // LOGS
        //
        if activation.logs == TelemetryState::Active {
            match exp_config.logs {
                Some(ExporterConfig::Otlp(exp_config)) => {
                    let logs = otlp::exporter::build_logs_exporter(
                        exp_config,
                        logs_pipeline_out_rx,
                        self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                    )?;

                    start_otlp_exporter(
                        &mut exporters_task_set,
                        "otlp_logs",
                        logs,
                        exporters_cancel.clone(),
                    );
                }
                Some(ExporterConfig::Clickhouse(cfg_builder)) => {
                    let builder = cfg_builder.build()?;

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
                                exporter_type = "clickhouse_logs",
                                "Clickhouse exporter returned from run loop with error."
                            );
                        }

                        Ok(())
                    });
                }
                Some(ExporterConfig::Blackhole) => {
                    let mut exp = BlackholeExporter::new(logs_pipeline_out_rx);

                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        exp.start(token).await;
                        Ok(())
                    });
                }
                #[cfg(feature = "rdkafka")]
                Some(ExporterConfig::Kafka(kafka_config)) => {
                    let mut logs_exporter =
                        build_logs_exporter(kafka_config, logs_pipeline_out_rx)?;
                    let token = exporters_cancel.clone();
                    exporters_task_set.spawn(async move {
                        logs_exporter.start(token).await;
                        Ok(())
                    });
                }
                _ => {}
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

fn start_otlp_exporter<Resource, Request, Response>(
    exporters_task_set: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    telemetry_type: &'static str,
    exporter: otlp::exporter::Exporter<Resource, Request, AwsSigv4RequestSigner, Response>,
    cancel_token: CancellationToken,
) where
    Request: prost::Message + topology::payload::OTLPFrom<Vec<Resource>> + Clone,
    Resource: prost::Message + Clone,
    [Resource]: BatchSizer,
    Response: prost::Message + Default + Clone,
{
    let mut exporter = exporter;

    exporters_task_set.spawn(async move {
        let res = exporter.start(cancel_token).await;
        if let Err(e) = res {
            error!(
                exporter_type = telemetry_type,
                error = e,
                "OTLPExporter exporter returned from run loop with error."
            );
        }

        Ok(())
    });
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
