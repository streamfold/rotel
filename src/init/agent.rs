use crate::bounded_channel::bounded;
use crate::exporters::blackhole::BlackholeExporter;
use crate::exporters::datadog::{DatadogTraceExporter, Region};
use crate::exporters::otlp;
use crate::init::activation::{TelemetryActivation, TelemetryState};
use crate::init::args::{AgentRun, DebugLogParam, Exporter};
use crate::init::datadog_exporter::DatadogRegion;
use crate::init::otlp_exporter::{
    build_logs_batch_config, build_logs_config, build_metrics_batch_config, build_metrics_config,
    build_traces_batch_config, build_traces_config,
};
use crate::listener::Listener;
use crate::receivers::otlp_grpc::OTLPGrpcServer;
use crate::receivers::otlp_http::OTLPHttpServer;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::debug::DebugLogger;
use crate::{telemetry, topology};
use gethostname::gethostname;
use opentelemetry::global;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::Resource;
use std::cmp::max;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinSet;
use tokio::time::{timeout_at, Instant};
use tokio_util::sync::CancellationToken;
use tracing::log::warn;
use tracing::{error, info};

#[cfg(feature = "pprof")]
use crate::init::pprof;

// todo: split out argument processing into some type of AgentBuilder interface
#[derive(Default)]
pub struct Agent {}

impl Agent {
    pub async fn run(
        &self,
        agent: Box<AgentRun>,
        mut port_map: HashMap<SocketAddr, Listener>,
        sending_queue_size: usize,
        environment: &String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            grpc_endpoint = agent.otlp_grpc_endpoint.to_string(),
            http_endpoint = agent.otlp_http_endpoint.to_string(),
            "Starting Rotel.",
        );

        // Initialize the TLS library, we may want to do this conditionally
        if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
            return Err(format!("failed to initialize crypto library: {:?}", e).into());
        }

        let num_cpus = num_cpus::get();

        let mut receivers_task_set = JoinSet::new();
        let mut pipeline_task_set = JoinSet::new();
        let mut exporters_task_set = JoinSet::new();

        let receivers_cancel = CancellationToken::new();
        let pipeline_cancel = CancellationToken::new();
        let exporters_cancel = CancellationToken::new();

        let activation = TelemetryActivation::from_config(&agent);

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
            bounded::<Vec<ResourceSpans>>(sending_queue_size);
        let trace_otlp_output = OTLPOutput::new(trace_pipeline_in_tx);

        let (metrics_pipeline_in_tx, metrics_pipeline_in_rx) =
            bounded::<Vec<ResourceMetrics>>(max(4, num_cpus));
        let (metrics_pipeline_out_tx, metrics_pipeline_out_rx) =
            bounded::<Vec<ResourceMetrics>>(sending_queue_size);
        let metrics_otlp_output = OTLPOutput::new(metrics_pipeline_in_tx);

        let (logs_pipeline_in_tx, logs_pipeline_in_rx) =
            bounded::<Vec<ResourceLogs>>(max(4, num_cpus));
        let (logs_pipeline_out_tx, logs_pipeline_out_rx) =
            bounded::<Vec<ResourceLogs>>(sending_queue_size);
        let logs_otlp_output = OTLPOutput::new(logs_pipeline_in_tx);

        let mut traces_output = None;
        let mut metrics_output = None;
        let mut logs_output = None;

        match activation.traces {
            TelemetryState::Active => traces_output = Some(trace_otlp_output),
            TelemetryState::Disabled => {
                info!("OTLP Receiver for traces disabled, OTLP receiver will be configured to not accept traces");
            }
            TelemetryState::NoListeners => {
                info!("No exporters are configured for traces, OTLP receiver will be configured to not accept traces");
            }
        }

        match activation.metrics {
            TelemetryState::Active => metrics_output = Some(metrics_otlp_output),
            TelemetryState::Disabled => {
                info!("OTLP Receiver for metrics disabled, OTLP receiver will be configured to not accept metrics");
            }
            TelemetryState::NoListeners => {
                info!("No exporters are configured for metrics, OTLP receiver will be configured to not accept metrics");
            }
        }

        match activation.logs {
            TelemetryState::Active => logs_output = Some(logs_otlp_output),
            TelemetryState::Disabled => {
                info!("OTLP Receiver for logs disabled, OTLP receiver will be configured to not accept logs");
            }
            TelemetryState::NoListeners => {
                info!("No exporters are configured for logs, OTLP receiver will be configured to not accept logs");
            }
        }

        //
        // OTLP GRPC server
        //
        let grpc_srv = OTLPGrpcServer::builder()
            .with_max_recv_msg_size_mib(agent.otlp_grpc_max_recv_msg_size_mib as usize)
            .with_traces_output(traces_output.clone())
            .with_metrics_output(metrics_output.clone())
            .with_logs_output(logs_output.clone())
            .build();

        let grpc_listener = port_map.remove(&agent.otlp_grpc_endpoint).unwrap();
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
            .with_traces_path(agent.otlp_receiver_traces_http_path.clone())
            .with_metrics_path(agent.otlp_receiver_metrics_http_path.clone())
            .with_logs_path(agent.otlp_receiver_logs_http_path.clone())
            .build();

        let http_listener = port_map.remove(&agent.otlp_http_endpoint).unwrap();
        {
            let receivers_cancel = receivers_cancel.clone();
            receivers_task_set
                .spawn(async move { http_srv.serve(http_listener, receivers_cancel).await });
        }

        let mut trace_pipeline = topology::generic_pipeline::Pipeline::new(
            trace_pipeline_in_rx.clone(),
            trace_pipeline_out_tx,
            build_traces_batch_config(agent.otlp_exporter.clone()),
        );

        let mut metrics_pipeline = topology::generic_pipeline::Pipeline::new(
            metrics_pipeline_in_rx.clone(),
            metrics_pipeline_out_tx,
            build_metrics_batch_config(agent.otlp_exporter.clone()),
        );

        let mut logs_pipeline = topology::generic_pipeline::Pipeline::new(
            logs_pipeline_in_rx.clone(),
            logs_pipeline_out_tx,
            build_logs_batch_config(agent.otlp_exporter.clone()),
        );

        let token = exporters_cancel.clone();
        match agent.exporter {
            Exporter::Blackhole => {
                let mut exp = BlackholeExporter::new(
                    trace_pipeline_out_rx.clone(),
                    metrics_pipeline_out_rx.clone(),
                );

                exporters_task_set.spawn(async move {
                    exp.start(token).await;
                    Ok(())
                });
            }
            Exporter::Otlp => {
                let endpoint = agent.otlp_exporter.otlp_exporter_endpoint.as_ref();
                if activation.traces == TelemetryState::Active {
                    let traces_config = build_traces_config(agent.otlp_exporter.clone(), endpoint);
                    let mut traces = otlp::exporter::build_traces_exporter(
                        traces_config,
                        trace_pipeline_out_rx.clone(),
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
                    let metrics_config =
                        build_metrics_config(agent.otlp_exporter.clone(), endpoint);
                    let mut metrics = otlp::exporter::build_metrics_exporter(
                        metrics_config,
                        metrics_pipeline_out_rx.clone(),
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
                }
                if activation.logs == TelemetryState::Active {
                    let logs_config = build_logs_config(agent.otlp_exporter.clone(), endpoint);
                    let mut logs = otlp::exporter::build_logs_exporter(
                        logs_config,
                        logs_pipeline_out_rx.clone(),
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
                if agent.datadog_exporter.datadog_exporter_api_key.is_none() {
                    // todo: is there a way to make this config required with the exporter mode?
                    return Err("must specify Datadog exporter API key".into());
                }
                let api_key = agent.datadog_exporter.datadog_exporter_api_key.unwrap();

                let hostname = get_hostname();

                let mut builder = DatadogTraceExporter::builder(
                    agent.datadog_exporter.datadog_exporter_region.into(),
                    agent
                        .datadog_exporter
                        .datadog_exporter_custom_endpoint
                        .clone(),
                    api_key,
                )
                .with_environment(environment.clone());

                if let Some(hostname) = hostname {
                    builder = builder.with_hostname(hostname);
                }

                let mut exp = builder.build(trace_pipeline_out_rx)?;

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
        }

        if traces_output.is_some() {
            let log_traces = agent.debug_log.contains(&DebugLogParam::Traces);
            let dbg_log = DebugLogger::new(log_traces);

            let pipeline_cancel = pipeline_cancel.clone();
            pipeline_task_set
                .spawn(async move { trace_pipeline.start(dbg_log, pipeline_cancel).await });
        }
        if metrics_output.is_some() {
            let log_metrics = agent.debug_log.contains(&DebugLogParam::Metrics);
            let dbg_log = DebugLogger::new(log_metrics);

            let pipeline_cancel = pipeline_cancel.clone();
            pipeline_task_set
                .spawn(async move { metrics_pipeline.start(dbg_log, pipeline_cancel).await });
        }
        if logs_output.is_some() {
            let log_logs = agent.debug_log.contains(&DebugLogParam::Logs);
            let dbg_log = DebugLogger::new(log_logs);

            let pipeline_cancel = pipeline_cancel.clone();
            pipeline_task_set
                .spawn(async move { logs_pipeline.start(dbg_log, pipeline_cancel).await });
        }

        let internal_metrics_exporter =
            telemetry::internal_exporter::InternalOTLPMetricsExporter::new(
                metrics_output.clone(),
                Temporality::Cumulative,
            );

        let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(internal_metrics_exporter)
            .with_resource(Resource::builder().with_service_name("rotel").build())
            .build();
        global::set_meter_provider(meter_provider);

        #[cfg(feature = "pprof")]
        let guard = if agent.profile_group.pprof_flame_graph || agent.profile_group.pprof_call_graph
        {
            pprof::pprof_guard()
        } else {
            None
        };

        let mut result = Ok(());
        select! {
            _ = signal_wait() => {
                info!("Shutdown signal received.");

                #[cfg(feature = "pprof")]
                if agent.profile_group.pprof_flame_graph || agent.profile_group.pprof_call_graph {
                    pprof::pprof_finish(guard, agent.profile_group.pprof_flame_graph, agent.profile_group.pprof_call_graph);
                }
            },
            e = wait_for_any_task(&mut receivers_task_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of receiver."),
                    Err(e) => result = Err(e),
                }
            },
            e = wait_for_any_task(&mut pipeline_task_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of pipeline."),
                    Err(e) => result = Err(e),
                }
            },
            e = wait_for_any_task(&mut exporters_task_set) => {
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
            wait_for_tasks_with_timeout(&mut receivers_task_set, Duration::from_secs(1)).await;
        if let Err(e) = res {
            return Err(format!("timed out waiting for receiver exit: {}", e).into());
        }

        // Drop the outputs (alternatively move them into receivers?), causing downstream
        // components to exit
        drop(traces_output);
        drop(metrics_output);
        drop(logs_output);

        // Set a maximum duration for exporters to exit, this way if the pipelines exit quickly,
        // the entire wall time is left for exporters to finish flushing (which may require longer if
        // endpoints are slow).
        let receivers_hard_stop = Instant::now() + Duration::from_secs(3);

        // Wait 500ms for the pipelines to finish. They should exit when the pipes are dropped.
        let res =
            wait_for_tasks_with_timeout(&mut pipeline_task_set, Duration::from_millis(500)).await;
        if res.is_err() {
            warn!("Pipelines did not exit on channel close, cancelling.");

            // force cancel
            pipeline_cancel.cancel();

            // try again
            let res =
                wait_for_tasks_with_timeout(&mut pipeline_task_set, Duration::from_millis(500))
                    .await;
            if let Err(e) = res {
                return Err(format!("timed out waiting for pipline to exit: {}", e).into());
            }
        }

        // pipeline outputs are already moved, so should be closed

        // Wait for the exporters using the same process
        let res =
            wait_for_tasks_with_timeout(&mut exporters_task_set, Duration::from_millis(500)).await;
        if res.is_err() {
            warn!("Exporters did not exit on channel close, cancelling.");

            // force cancel
            exporters_cancel.cancel();

            let res =
                wait_for_tasks_with_deadline(&mut exporters_task_set, receivers_hard_stop).await;
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

async fn wait_for_any_task(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let r = tasks.join_next().await;

    match r {
        None => Ok(()), // should not happen
        Some(res) => res?,
    }
}

async fn wait_for_tasks_with_timeout(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    wait_for_tasks_with_deadline(tasks, Instant::now() + timeout).await
}

async fn wait_for_tasks_with_deadline(
    tasks: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    stop_at: Instant,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut result = Ok(());
    loop {
        match timeout_at(stop_at, tasks.join_next()).await {
            Err(_) => {
                result = Err("timed out waiting for tasks to complete".into());
                break;
            }
            Ok(None) => break,
            Ok(Some(v)) => {
                match v {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => result = Err(e),
                    e => {
                        error!("Failed to join with task: {:?}", e)
                    } // Ignore?
                }
            }
        }
    }

    result
}

async fn signal_wait() {
    let mut sig_term = sig(SignalKind::terminate());
    let mut sig_int = sig(SignalKind::interrupt());

    select! {
        _ = sig_term.recv() => {},
        _ = sig_int.recv() => {},
    }
}

fn sig(kind: SignalKind) -> tokio::signal::unix::Signal {
    signal(kind).unwrap()
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
