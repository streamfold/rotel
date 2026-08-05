use crate::aws_api::creds::AwsCredsProvider;
use crate::bounded_channel::{BoundedReceiver, BoundedSender, bounded};
use crate::crypto::init_crypto_provider;
use crate::exporters::blackhole::BlackholeExporter;
use crate::exporters::datadog::Region;
#[cfg(feature = "rdkafka")]
use crate::exporters::kafka::{build_logs_exporter, build_metrics_exporter, build_traces_exporter};
use crate::exporters::otlp::{self, Authenticator};
use crate::init::activation::{TelemetryActivation, TelemetryState};
use crate::init::args::{AgentRun, DebugLogParam, Receiver};
use crate::init::batch::{
    build_logs_batch_config, build_metrics_batch_config, build_traces_batch_config,
};
#[cfg(feature = "rdkafka")]
use crate::init::config::validate_receiver_targets;
use crate::init::config::{
    ExportGroupConfig, ExporterConfig, ReceiverConfig, get_exporters_config, get_receivers_config,
};
use crate::init::datadog_exporter::DatadogRegion;
#[cfg(feature = "pprof")]
use crate::init::pprof;
use crate::init::wait;
use crate::listener::Listener;
#[cfg(feature = "file_receiver")]
use crate::receivers::file::offset_committer::FileOffsetCommitter;
#[cfg(feature = "file_receiver")]
use crate::receivers::file::receiver::FileReceiver;
#[cfg(feature = "fluent_receiver")]
use crate::receivers::fluent::receiver::FluentReceiver;
#[cfg(feature = "rdkafka")]
use crate::receivers::kafka::offset_ack_committer::KafkaOffsetCommitter;
#[cfg(feature = "rdkafka")]
use crate::receivers::kafka::receiver::KafkaReceiver;
#[cfg(all(target_os = "linux", feature = "kmsg_receiver"))]
use crate::receivers::kmsg::receiver::KmsgReceiver;
use crate::receivers::otlp::otlp_grpc::OTLPGrpcServer;
use crate::receivers::otlp::otlp_http::OTLPHttpServer;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::batch::BatchSizer;
use crate::topology::debug::DebugLogger;
use crate::topology::export_group::ExportGroupBuilder;
use crate::topology::fanout::FanoutBuilder;
use crate::topology::flush_control::{FlushSubscriber, conditional_flush};
use crate::topology::payload::Message;
use crate::topology::processors::Processors;
use crate::{telemetry, topology};
use opentelemetry::global;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, Temporality};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::Duration;
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[cfg(feature = "prometheus")]
use crate::telemetry::metrics_server::MetricsServer;
#[cfg(feature = "prometheus")]
use opentelemetry_prometheus_text_exporter::PrometheusExporter;

pub struct Agent {
    config: Box<AgentRun>,
    port_map: HashMap<SocketAddr, Listener>,
    sending_queue_size: usize,
    environment: String,
    logs_rx: Option<(BoundedReceiver<Message<ResourceLogs>>, FlushSubscriber)>,
    pipeline_flush_sub: Option<FlushSubscriber>,
    exporters_flush_sub: Option<FlushSubscriber>,
    otlp_default_receiver: bool,
    init_complete_chan: Option<tokio::sync::oneshot::Sender<bool>>,
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
            otlp_default_receiver: true,
            init_complete_chan: None,
        }
    }

    pub fn with_logs_rx(
        mut self,
        logs_rx: BoundedReceiver<Message<ResourceLogs>>,
        logs_rx_flush_sub: FlushSubscriber,
    ) -> Self {
        self.logs_rx = Some((logs_rx, logs_rx_flush_sub));
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

    pub fn with_init_complete_chan(
        mut self,
        init_complete_chan: tokio::sync::oneshot::Sender<bool>,
    ) -> Self {
        self.init_complete_chan = Some(init_complete_chan);
        self
    }

    /// disable the default OTLP receiver
    pub fn disable_otlp_default_receiver(mut self) -> Self {
        self.otlp_default_receiver = false;
        self
    }

    pub async fn run(
        mut self,
        agent_cancel: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let config = self.config;

        info!("Starting Rotel.");

        let resource_attributes = match &config.otel_resource_attributes {
            Some(s) => crate::init::parse::parse_key_vals::<String, String>(s)?,
            None => Vec::new(),
        };

        // Initialize the TLS library, we may want to do this conditionally
        init_crypto_provider()?;

        let num_cpus = num_cpus::get();

        let mut receivers_task_set = JoinSet::new();
        let mut pipeline_task_set = JoinSet::new();
        let mut exporters_task_set = JoinSet::new();
        #[cfg(feature = "rdkafka")]
        let mut kafka_offset_committer: Option<KafkaOffsetCommitter> = None;
        #[cfg(feature = "file_receiver")]
        let mut file_offset_committer: Option<FileOffsetCommitter> = None;

        let receivers_cancel = CancellationToken::new();
        let pipeline_cancel = CancellationToken::new();
        let exporters_cancel = CancellationToken::new();
        let kafka_offset_committer_cancel = CancellationToken::new();
        #[cfg(feature = "file_receiver")]
        let file_offset_committer_cancel = CancellationToken::new();

        let (trace_pipeline_in_tx, trace_pipeline_in_rx) =
            bounded::<Message<ResourceSpans>>(max(4, num_cpus));
        let trace_otlp_output = OTLPOutput::new(trace_pipeline_in_tx);

        let (metrics_pipeline_in_tx, metrics_pipeline_in_rx) =
            bounded::<Message<ResourceMetrics>>(max(4, num_cpus));
        let metrics_otlp_output = OTLPOutput::new(metrics_pipeline_in_tx);

        let (logs_pipeline_in_tx, logs_pipeline_in_rx) =
            bounded::<Message<ResourceLogs>>(max(4, num_cpus));
        let logs_otlp_output = OTLPOutput::new(logs_pipeline_in_tx);

        let (internal_metrics_pipeline_in_tx, internal_metrics_pipeline_in_rx) =
            bounded::<Message<ResourceMetrics>>(max(4, num_cpus));
        let internal_metrics_otlp_output = OTLPOutput::new(internal_metrics_pipeline_in_tx);

        let rec_config = get_receivers_config(&config, self.otlp_default_receiver)?;
        #[allow(unused_mut)]
        let mut exp_config = get_exporters_config(&config, &self.environment)?;

        #[cfg(feature = "rdkafka")]
        validate_receiver_targets(&rec_config, &exp_config)?;

        // Check if Kafka receiver with offset tracking is enabled
        // Offset tracking is enabled when auto commit is disabled
        #[cfg(feature = "rdkafka")]
        let kafka_offset_tracking_enabled = rec_config.iter().any(|(_, cfg)| {
            #[cfg(feature = "rdkafka")]
            matches!(cfg, ReceiverConfig::Kafka(k) if !k.enable_auto_commit && !k.disable_exporter_indefinite_retry)
        });

        // Check if finite retry is explicitly enabled (when Kafka offset tracking with disable infinite retry is set)
        #[cfg(feature = "rdkafka")]
        let finite_retry_enabled = rec_config.iter().any(|(_, cfg)| {
            #[cfg(feature = "rdkafka")]
            matches!(cfg, ReceiverConfig::Kafka(k) if !k.enable_auto_commit && k.disable_exporter_indefinite_retry)
        });

        #[cfg(feature = "rdkafka")]
        let kafka_target_exporters = kafka_receiver_target_exporters(&rec_config);

        // Validate configuration: disable_exporter_indefinite_retry requires auto_commit to be disabled
        #[cfg(feature = "rdkafka")]
        for (name, cfg) in &rec_config {
            if let ReceiverConfig::Kafka(k) = cfg {
                if k.enable_auto_commit && k.disable_exporter_indefinite_retry {
                    return Err(format!(
                        "Invalid Kafka receiver configuration for '{:?}': \
                        disable_exporter_indefinite_retry=true requires enable_auto_commit=false. \
                        When auto-commit is enabled, Kafka handles offset management automatically \
                        and exporter acknowledgment is not used for offset tracking.",
                        name
                    )
                    .into());
                }
            }
        }

        // HTTP acknowledger will be created per exporter that needs it

        // If Kafka offset tracking is enabled, modify retry configs to be indefinite
        #[cfg(feature = "rdkafka")]
        if kafka_offset_tracking_enabled {
            info!(
                "Kafka offset tracking enabled - setting exporters to retry indefinitely to ensure no data loss. To disable this behavior, use --kafka-receiver-disable-exporter-indefinite-retry"
            );
            if kafka_target_exporters.has_targets() {
                exp_config.set_indefinite_retry_for_targets(
                    &kafka_target_exporters.traces,
                    &kafka_target_exporters.metrics,
                    &kafka_target_exporters.logs,
                );
            } else {
                exp_config.set_indefinite_retry();
            }
        }

        // Check if file receiver with offset tracking is enabled (indefinite retry not disabled)
        #[cfg(feature = "file_receiver")]
        let file_offset_tracking_enabled = rec_config
            .iter()
            .any(|(_, cfg)| matches!(cfg, ReceiverConfig::File(f) if !f.finite_retry_enabled));

        // If file receiver offset tracking is enabled, modify retry configs to be indefinite
        #[cfg(feature = "file_receiver")]
        if file_offset_tracking_enabled {
            info!(
                "File receiver offset tracking enabled - setting exporters to retry indefinitely to ensure no data loss. To disable this behavior, use --file-receiver-disable-exporter-indefinite-retry"
            );
            exp_config.set_indefinite_retry();
        }

        let activation =
            TelemetryActivation::from_config(&rec_config, &exp_config, self.logs_rx.is_some());

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

        #[cfg(feature = "rdkafka")]
        let (kafka_target_traces_output, kafka_target_traces_rx) =
            new_optional_pipeline_output::<ResourceSpans>(
                !kafka_target_exporters.traces.is_empty(),
                max(4, num_cpus),
            );
        #[cfg(feature = "rdkafka")]
        let (kafka_target_metrics_output, kafka_target_metrics_rx) =
            new_optional_pipeline_output::<ResourceMetrics>(
                !kafka_target_exporters.metrics.is_empty(),
                max(4, num_cpus),
            );
        #[cfg(feature = "rdkafka")]
        let (kafka_target_logs_output, kafka_target_logs_rx) =
            new_optional_pipeline_output::<ResourceLogs>(
                !kafka_target_exporters.logs.is_empty(),
                max(4, num_cpus),
            );

        let otlp_rec_enabled = rec_config.contains_key(&Receiver::Otlp);
        // Only notify user if we have an otlp receiver
        match activation.traces {
            TelemetryState::Active => traces_output = Some(trace_otlp_output),
            TelemetryState::Disabled => {
                if otlp_rec_enabled {
                    info!(
                        "OTLP Receiver for traces disabled, OTLP receiver will be configured to not accept traces"
                    );
                }
            }
            TelemetryState::NoListeners => {
                if otlp_rec_enabled {
                    info!(
                        "No exporters are configured for traces, OTLP receiver will be configured to not accept traces"
                    );
                }
            }
        }

        match activation.metrics {
            TelemetryState::Active => {
                metrics_output = Some(metrics_otlp_output);
                internal_metrics_output = Some(internal_metrics_otlp_output);
            }
            TelemetryState::Disabled => {
                if otlp_rec_enabled {
                    info!(
                        "OTLP Receiver for metrics disabled, OTLP receiver will be configured to not accept metrics"
                    );
                }
            }
            TelemetryState::NoListeners => {
                if otlp_rec_enabled {
                    info!(
                        "No exporters are configured for metrics, OTLP receiver will be configured to not accept metrics"
                    );
                }
            }
        }

        match activation.logs {
            TelemetryState::Active => logs_output = Some(logs_otlp_output),
            TelemetryState::Disabled => {
                if otlp_rec_enabled {
                    info!(
                        "OTLP Receiver for logs disabled, OTLP receiver will be configured to not accept logs"
                    );
                }
            }
            TelemetryState::NoListeners => {
                if otlp_rec_enabled {
                    info!(
                        "No exporters are configured for logs, OTLP receiver will be configured to not accept logs"
                    );
                }
            }
        }

        if !config.enable_internal_telemetry {
            internal_metrics_output = None;
        }

        let mut pipeline_flush_sub = self.pipeline_flush_sub.take();

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

        #[allow(unused_mut)]
        let mut meter_provider_builder =
            opentelemetry_sdk::metrics::SdkMeterProvider::builder().with_reader(periodic_reader);

        //
        // Start the Prometheus metrics server if configured
        //

        #[cfg(feature = "prometheus")]
        let (mut prom_task_set, prom_cancel) = {
            info!(?config.prometheus_endpoint, "Starting Prometheus metrics server");
            let mut prom_task_set = JoinSet::new();
            let prom_cancel = CancellationToken::new();

            let prom_exporter = PrometheusExporter::new();

            meter_provider_builder = meter_provider_builder.with_reader(prom_exporter.clone());

            let metrics_listener = Listener::listen_std(config.prometheus_endpoint)?;
            let metrics_server = MetricsServer::new(config.prometheus_endpoint, prom_exporter);
            let cancel_token = prom_cancel.clone();

            prom_task_set.spawn(async move {
                if let Err(e) = metrics_server.serve(metrics_listener, cancel_token).await {
                    error!("Metrics server error: {:?}", e);
                }
                Ok(())
            });

            (prom_task_set, prom_cancel)
        };

        let meter_provider = meter_provider_builder
            .with_resource(Resource::builder().with_service_name("rotel").build())
            .build();

        global::set_meter_provider(meter_provider);

        //
        // Build the exporters now
        //

        let mut trace_fanout = FanoutBuilder::new("traces");
        let mut metrics_fanout = FanoutBuilder::new("metrics");
        let mut logs_fanout = FanoutBuilder::new("logs");
        let mut internal_metrics_fanout = FanoutBuilder::new("internal_metrics");
        let mut trace_senders = HashMap::new();
        let mut metrics_senders = HashMap::new();
        let mut logs_senders = HashMap::new();

        //
        // TRACES
        //
        if activation.traces == TelemetryState::Active {
            let (mut trace_channels, trace_member_retry_cap) = setup_pipeline_channels(
                &exp_config.trace_groups,
                &exp_config.trace_names,
                &mut trace_fanout,
                "traces",
                self.sending_queue_size,
            );
            trace_senders = clone_channel_senders(&trace_channels);

            for (name, mut cfg) in exp_config.trace_names.into_iter().zip(exp_config.traces) {
                let (_, trace_pipeline_out_rx) = trace_channels
                    .remove(&name)
                    .expect("channel pre-allocated above");

                if let Some(&cap) = trace_member_retry_cap.get(&name) {
                    cfg.cap_retry_elapsed_time(cap);
                }

                match cfg {
                    ExporterConfig::Otlp(exp_config) => {
                        let creds_provider = match exp_config.authenticator {
                            Some(Authenticator::Sigv4auth) => Some(AwsCredsProvider::new().await?),
                            _ => None,
                        };
                        let traces = otlp::exporter::build_traces_exporter(
                            exp_config,
                            trace_pipeline_out_rx,
                            self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                            creds_provider,
                        )?;

                        start_otlp_exporter(
                            &mut exporters_task_set,
                            "otlp_traces",
                            traces,
                            exporters_cancel.clone(),
                        );
                    }
                    ExporterConfig::Clickhouse(cfg_builder) => {
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
                    ExporterConfig::Datadog(cfg_builder) => {
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
                    ExporterConfig::Xray(cfg_builder) => {
                        let creds_provider = AwsCredsProvider::new().await?;
                        let builder = cfg_builder.build();
                        let exp = builder.build(
                            trace_pipeline_out_rx,
                            self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                            "production".to_string(),
                            creds_provider,
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
                    ExporterConfig::Blackhole => {
                        let mut exp = BlackholeExporter::new(trace_pipeline_out_rx);

                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            exp.start(token).await;
                            Ok(())
                        });
                    }
                    #[cfg(feature = "rdkafka")]
                    ExporterConfig::Kafka(kafka_config) => {
                        let mut traces_exporter =
                            build_traces_exporter(kafka_config, trace_pipeline_out_rx)?;
                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            traces_exporter.start(token).await;
                            Ok(())
                        });
                    }
                    #[cfg(feature = "file_exporter")]
                    ExporterConfig::File(config) => {
                        let exporter =
                            crate::exporters::file::FileExporterBuilder::build_traces_exporter(
                                &config,
                                trace_pipeline_out_rx,
                            )?;

                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            let res = exporter.start(token).await;
                            if let Err(e) = res {
                                error!(
                                    error = %e,
                                    exporter_type = "file_traces",
                                    "File exporter returned from run loop with error."
                                );
                            }
                            Ok(())
                        });
                    }
                    _ => {}
                }
            }
        }

        //
        // METRICS
        //
        if activation.metrics == TelemetryState::Active {
            let (mut metrics_channels, metrics_member_retry_cap) = setup_pipeline_channels(
                &exp_config.metric_groups,
                &exp_config.metric_names,
                &mut metrics_fanout,
                "metrics",
                self.sending_queue_size,
            );
            metrics_senders = clone_channel_senders(&metrics_channels);

            // Combine metrics and internal_metrics exporters for spawning.
            let combined_metrics_configs = exp_config
                .metric_names
                .into_iter()
                .zip(exp_config.metrics)
                .map(|(name, cfg)| (name, cfg, false))
                .chain(
                    exp_config
                        .internal_metric_names
                        .into_iter()
                        .zip(exp_config.internal_metrics)
                        .map(|(name, cfg)| (name, cfg, true)),
                );

            for (name, mut cfg, is_internal_metrics) in combined_metrics_configs {
                // Skip internal metrics if not enabled
                if is_internal_metrics && !config.enable_internal_telemetry {
                    continue;
                }

                let metrics_pipeline_out_rx = if is_internal_metrics {
                    // Internal metrics are never grouped; create channel inline.
                    let (internal_tx, internal_rx) =
                        bounded::<Vec<Message<ResourceMetrics>>>(self.sending_queue_size);
                    internal_metrics_fanout =
                        internal_metrics_fanout.add_tx(cfg.name(), internal_tx);
                    internal_rx
                } else {
                    if let Some(&cap) = metrics_member_retry_cap.get(&name) {
                        cfg.cap_retry_elapsed_time(cap);
                    }
                    let (_, rx) = metrics_channels
                        .remove(&name)
                        .expect("channel pre-allocated above");
                    rx
                };

                let telemetry_type = match is_internal_metrics {
                    true => "internal_metrics",
                    false => "metrics",
                };

                match cfg {
                    ExporterConfig::Otlp(exp_config) => {
                        let creds_provider = match exp_config.authenticator {
                            Some(Authenticator::Sigv4auth) => Some(AwsCredsProvider::new().await?),
                            _ => None,
                        };

                        let metrics = match is_internal_metrics {
                            true => otlp::exporter::build_internal_metrics_exporter(
                                exp_config.clone(),
                                metrics_pipeline_out_rx,
                                self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                                creds_provider,
                            )?,
                            false => otlp::exporter::build_metrics_exporter(
                                exp_config.clone(),
                                metrics_pipeline_out_rx,
                                self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                                creds_provider,
                            )?,
                        };

                        start_otlp_exporter(
                            &mut exporters_task_set,
                            telemetry_type,
                            metrics,
                            exporters_cancel.clone(),
                        );
                    }
                    ExporterConfig::Clickhouse(cfg_builder) => {
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
                    ExporterConfig::Blackhole => {
                        let mut exp = BlackholeExporter::new(metrics_pipeline_out_rx);

                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            exp.start(token).await;
                            Ok(())
                        });
                    }
                    #[cfg(feature = "rdkafka")]
                    ExporterConfig::Kafka(kafka_config) => {
                        let mut metrics_exporter =
                            build_metrics_exporter(kafka_config, metrics_pipeline_out_rx)?;
                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            metrics_exporter.start(token).await;
                            Ok(())
                        });
                    }
                    #[cfg(feature = "file_exporter")]
                    ExporterConfig::File(config) => {
                        let exporter =
                            crate::exporters::file::FileExporterBuilder::build_metrics_exporter(
                                &config,
                                metrics_pipeline_out_rx,
                            )?;

                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            let res = exporter.start(token).await;
                            if let Err(e) = res {
                                error!(
                                    error = %e,
                                    exporter_type = "file_metrics",
                                    "File exporter returned from run loop with error."
                                );
                            }

                            Ok(())
                        });
                    }
                    ExporterConfig::Awsemf(cfg_builder) => {
                        let creds_provider = AwsCredsProvider::new().await?;
                        let builder = cfg_builder.build();
                        let exp = builder.build(
                            metrics_pipeline_out_rx,
                            self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                            creds_provider,
                        )?;

                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            let res = exp.start(token).await;
                            if let Err(e) = res {
                                error!(
                                    error = e,
                                    "AWS EMF exporter returned from run loop with error."
                                );
                            }

                            Ok(())
                        });
                    }
                    _ => {}
                }
            }
        }

        //
        // LOGS
        //
        if activation.logs == TelemetryState::Active {
            let (mut logs_channels, logs_member_retry_cap) = setup_pipeline_channels(
                &exp_config.log_groups,
                &exp_config.log_names,
                &mut logs_fanout,
                "logs",
                self.sending_queue_size,
            );
            logs_senders = clone_channel_senders(&logs_channels);

            for (name, mut cfg) in exp_config.log_names.into_iter().zip(exp_config.logs) {
                let (_, logs_pipeline_out_rx) = logs_channels
                    .remove(&name)
                    .expect("channel pre-allocated above");

                if let Some(&cap) = logs_member_retry_cap.get(&name) {
                    cfg.cap_retry_elapsed_time(cap);
                }

                match cfg {
                    ExporterConfig::Otlp(exp_config) => {
                        let creds_provider = match exp_config.authenticator {
                            Some(Authenticator::Sigv4auth) => Some(AwsCredsProvider::new().await?),
                            _ => None,
                        };

                        let logs = otlp::exporter::build_logs_exporter(
                            exp_config,
                            logs_pipeline_out_rx,
                            self.exporters_flush_sub.as_mut().map(|sub| sub.subscribe()),
                            creds_provider,
                        )?;

                        start_otlp_exporter(
                            &mut exporters_task_set,
                            "otlp_logs",
                            logs,
                            exporters_cancel.clone(),
                        );
                    }
                    ExporterConfig::Clickhouse(cfg_builder) => {
                        let builder = cfg_builder.build_logs().await?;

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
                    ExporterConfig::Blackhole => {
                        let mut exp = BlackholeExporter::new(logs_pipeline_out_rx);

                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            exp.start(token).await;
                            Ok(())
                        });
                    }
                    #[cfg(feature = "rdkafka")]
                    ExporterConfig::Kafka(kafka_config) => {
                        let mut logs_exporter =
                            build_logs_exporter(kafka_config, logs_pipeline_out_rx)?;
                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            logs_exporter.start(token).await;
                            Ok(())
                        });
                    }
                    #[cfg(feature = "file_exporter")]
                    ExporterConfig::File(config) => {
                        let exporter =
                            crate::exporters::file::FileExporterBuilder::build_logs_exporter(
                                &config,
                                logs_pipeline_out_rx,
                            )?;

                        let token = exporters_cancel.clone();
                        exporters_task_set.spawn(async move {
                            let res = exporter.start(token).await;
                            if let Err(e) = res {
                                error!(
                                    error = %e,
                                    exporter_type = "file_logs",
                                    "File exporter returned from run loop with error."
                                );
                            }
                            Ok(())
                        });
                    }
                    _ => {}
                }
            }
        }

        if traces_output.is_some() {
            let trace_fanout = trace_fanout
                .build()
                .expect("Failed to build trace fanout with single consumer");

            let trace_processors = Processors::initialize(config.otlp_with_trace_processor.clone())
                .map_err(|e| format!("Failed to initialize trace processors: {}", e))?
                .initialize_rust(config.rust_trace_processor.clone())
                .map_err(|e| format!("Failed to initialize Rust trace processors: {}", e))?
                .initialize_async_rust(config.async_rust_trace_processor.clone())
                .map_err(|e| format!("Failed to initialize async Rust trace processors: {}", e))?
                .set_async_preserve_on_panic(config.async_processor_preserve_on_panic);

            let mut trace_pipeline = topology::generic_pipeline::Pipeline::new(
                "traces",
                trace_pipeline_in_rx.clone(),
                trace_fanout,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_traces_batch_config(config.batch.clone()),
                trace_processors,
                resource_attributes.clone(),
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
            let metrics_fanout = metrics_fanout
                .build()
                .expect("Failed to build metrics fanout with single consumer");

            let metrics_processors =
                Processors::initialize(config.otlp_with_metrics_processor.clone())
                    .map_err(|e| format!("Failed to initialize metrics processors: {}", e))?
                    .initialize_rust(config.rust_metrics_processor.clone())
                    .map_err(|e| format!("Failed to initialize Rust metrics processors: {}", e))?
                    .initialize_async_rust(config.async_rust_metrics_processor.clone())
                    .map_err(|e| {
                        format!("Failed to initialize async Rust metrics processors: {}", e)
                    })?
                    .set_async_preserve_on_panic(config.async_processor_preserve_on_panic);

            let mut metrics_pipeline = topology::generic_pipeline::Pipeline::new(
                "metrics",
                metrics_pipeline_in_rx.clone(),
                metrics_fanout,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_metrics_batch_config(config.batch.clone()),
                metrics_processors,
                resource_attributes.clone(),
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
            let logs_fanout = logs_fanout
                .build()
                .expect("Failed to build logs fanout with single consumer");

            let logs_processors = Processors::initialize(config.otlp_with_logs_processor.clone())
                .map_err(|e| format!("Failed to initialize logs processors: {}", e))?
                .initialize_rust(config.rust_logs_processor.clone())
                .map_err(|e| format!("Failed to initialize Rust logs processors: {}", e))?
                .initialize_async_rust(config.async_rust_logs_processor.clone())
                .map_err(|e| format!("Failed to initialize async Rust logs processors: {}", e))?
                .set_async_preserve_on_panic(config.async_processor_preserve_on_panic);

            let mut logs_pipeline = topology::generic_pipeline::Pipeline::new(
                "logs",
                logs_pipeline_in_rx.clone(),
                logs_fanout,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_logs_batch_config(config.batch.clone()),
                logs_processors,
                resource_attributes.clone(),
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

        #[cfg(feature = "rdkafka")]
        if let Some(kafka_target_traces_rx) = kafka_target_traces_rx {
            let trace_fanout =
                build_target_fanout(&kafka_target_exporters.traces, &trace_senders, "traces")?;

            let trace_processors = Processors::initialize(config.otlp_with_trace_processor.clone())
                .map_err(|e| format!("Failed to initialize trace processors: {}", e))?
                .initialize_rust(config.rust_trace_processor.clone())
                .map_err(|e| format!("Failed to initialize Rust trace processors: {}", e))?
                .initialize_async_rust(config.async_rust_trace_processor.clone())
                .map_err(|e| format!("Failed to initialize async Rust trace processors: {}", e))?
                .set_async_preserve_on_panic(config.async_processor_preserve_on_panic);

            let mut trace_pipeline = topology::generic_pipeline::Pipeline::new(
                "traces",
                kafka_target_traces_rx,
                trace_fanout,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_traces_batch_config(config.batch.clone()),
                trace_processors,
                resource_attributes.clone(),
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

        #[cfg(feature = "rdkafka")]
        if let Some(kafka_target_metrics_rx) = kafka_target_metrics_rx {
            let metrics_fanout =
                build_target_fanout(&kafka_target_exporters.metrics, &metrics_senders, "metrics")?;

            let metrics_processors =
                Processors::initialize(config.otlp_with_metrics_processor.clone())
                    .map_err(|e| format!("Failed to initialize metrics processors: {}", e))?
                    .initialize_rust(config.rust_metrics_processor.clone())
                    .map_err(|e| format!("Failed to initialize Rust metrics processors: {}", e))?
                    .initialize_async_rust(config.async_rust_metrics_processor.clone())
                    .map_err(|e| {
                        format!("Failed to initialize async Rust metrics processors: {}", e)
                    })?
                    .set_async_preserve_on_panic(config.async_processor_preserve_on_panic);

            let mut metrics_pipeline = topology::generic_pipeline::Pipeline::new(
                "metrics",
                kafka_target_metrics_rx,
                metrics_fanout,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_metrics_batch_config(config.batch.clone()),
                metrics_processors,
                resource_attributes.clone(),
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

        #[cfg(feature = "rdkafka")]
        if let Some(kafka_target_logs_rx) = kafka_target_logs_rx {
            let logs_fanout =
                build_target_fanout(&kafka_target_exporters.logs, &logs_senders, "logs")?;

            let logs_processors = Processors::initialize(config.otlp_with_logs_processor.clone())
                .map_err(|e| format!("Failed to initialize logs processors: {}", e))?
                .initialize_rust(config.rust_logs_processor.clone())
                .map_err(|e| format!("Failed to initialize Rust logs processors: {}", e))?
                .initialize_async_rust(config.async_rust_logs_processor.clone())
                .map_err(|e| format!("Failed to initialize async Rust logs processors: {}", e))?
                .set_async_preserve_on_panic(config.async_processor_preserve_on_panic);

            let mut logs_pipeline = topology::generic_pipeline::Pipeline::new(
                "logs",
                kafka_target_logs_rx,
                logs_fanout,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_logs_batch_config(config.batch.clone()),
                logs_processors,
                resource_attributes.clone(),
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
            let internal_metrics_fanout = internal_metrics_fanout
                .build()
                .expect("Failed to build internal metrics fanout with single consumer");

            let mut internal_metrics_pipeline = topology::generic_pipeline::Pipeline::new(
                "internal_metrics",
                internal_metrics_pipeline_in_rx.clone(),
                internal_metrics_fanout,
                pipeline_flush_sub.as_mut().map(|sub| sub.subscribe()),
                build_metrics_batch_config(config.batch.clone()),
                Processors::empty(),
                resource_attributes.clone(),
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

        for config in rec_config.values() {
            match config {
                ReceiverConfig::Otlp(config) => {
                    //
                    // OTLP GRPC server
                    //
                    info!(
                        grpc_endpoint = config.otlp_grpc_endpoint.to_string(),
                        http_endpoint = config.otlp_http_endpoint.to_string(),
                        "OTLP receiver listening"
                    );
                    let grpc_srv = OTLPGrpcServer::builder()
                        .with_max_recv_msg_size_mib(config.otlp_grpc_max_recv_msg_size_mib as usize)
                        .with_traces_output(traces_output.clone())
                        .with_metrics_output(metrics_output.clone())
                        .with_logs_output(logs_output.clone())
                        .with_include_metadata(config.otlp_grpc_include_metadata)
                        .with_headers_to_include(config.otlp_grpc_headers_to_include.clone())
                        .build();

                    let grpc_listener = self.port_map.remove(&config.otlp_grpc_endpoint).unwrap();
                    {
                        let receivers_cancel = receivers_cancel.clone();
                        receivers_task_set.spawn(async move {
                            grpc_srv.serve(grpc_listener, receivers_cancel).await
                        });
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
                        .with_include_metadata(config.otlp_http_include_metadata)
                        .with_headers_to_include(config.otlp_http_headers_to_include.clone())
                        .build();

                    let http_listener = self.port_map.remove(&config.otlp_http_endpoint).unwrap();
                    {
                        let receivers_cancel = receivers_cancel.clone();
                        receivers_task_set.spawn(async move {
                            http_srv.serve(http_listener, receivers_cancel).await
                        });
                    }
                }
                #[cfg(feature = "rdkafka")]
                ReceiverConfig::Kafka(config) => {
                    let kafka_traces_output = kafka_target_traces_output
                        .clone()
                        .or_else(|| traces_output.clone());
                    let kafka_metrics_output = kafka_target_metrics_output
                        .clone()
                        .or_else(|| metrics_output.clone());
                    let kafka_logs_output = kafka_target_logs_output
                        .clone()
                        .or_else(|| logs_output.clone());

                    let mut kafka = KafkaReceiver::new(
                        config.clone(),
                        kafka_traces_output,
                        kafka_metrics_output,
                        kafka_logs_output,
                        finite_retry_enabled,
                    )?;

                    // Extract the offset committer before starting the receiver
                    kafka_offset_committer = kafka.take_offset_committer();

                    let receivers_cancel = receivers_cancel.clone();
                    receivers_task_set.spawn(async move { kafka.run(receivers_cancel).await });
                }
                #[cfg(feature = "fluent_receiver")]
                ReceiverConfig::Fluent(config) => {
                    let fluent = FluentReceiver::new(config.clone(), logs_output.clone()).await?;

                    let mut fluent_task_set = JoinSet::new();
                    // Fluent receiver may spawn multiple listener tasks
                    fluent
                        .start(&mut fluent_task_set, &receivers_cancel)
                        .await?;

                    let receivers_cancel = receivers_cancel.clone();
                    receivers_task_set.spawn(async move {
                        loop {
                            select! {
                                e = wait::wait_for_any_task(&mut fluent_task_set) => {
                                    match e {
                                        Ok(()) => {
                                            info!("Unexpected early exit of fluent receiver task.");
                                        },
                                        Err(e) => break Err(e),
                                    }
                                },
                                _ = receivers_cancel.cancelled() => {
                                    break wait::wait_for_tasks_with_timeout(&mut fluent_task_set, Duration::from_millis(500)).await;
                                }
                            }
                        }
                    });
                }
                #[cfg(feature = "file_receiver")]
                ReceiverConfig::File(config) => {
                    let mut file_receiver =
                        FileReceiver::new(config.clone(), logs_output.clone()).await?;

                    // Extract offset committer before starting receiver
                    // It will run separately and outlive the receiver to handle acks
                    file_offset_committer = file_receiver.take_offset_committer();

                    let mut file_task_set = JoinSet::new();
                    file_receiver
                        .start(&mut file_task_set, &receivers_cancel)
                        .await?;

                    let receivers_cancel = receivers_cancel.clone();
                    receivers_task_set.spawn(async move {
                        loop {
                            select! {
                                e = wait::wait_for_any_task(&mut file_task_set) => {
                                    match e {
                                        Ok(()) => {
                                            info!("Unexpected early exit of file receiver task.");
                                        },
                                        Err(e) => break Err(e),
                                    }
                                },
                                _ = receivers_cancel.cancelled() => {
                                    break wait::wait_for_tasks_with_timeout(&mut file_task_set, Duration::from_millis(500)).await;
                                }
                            }
                        }
                    });
                }
                #[cfg(all(target_os = "linux", feature = "kmsg_receiver"))]
                ReceiverConfig::Kmsg(config) => {
                    let kmsg = KmsgReceiver::new(config.clone(), logs_output.clone()).await?;

                    let mut kmsg_task_set = JoinSet::new();
                    kmsg.start(&mut kmsg_task_set, &receivers_cancel).await?;

                    let receivers_cancel = receivers_cancel.clone();
                    receivers_task_set.spawn(async move {
                        loop {
                            select! {
                                e = wait::wait_for_any_task(&mut kmsg_task_set) => {
                                    match e {
                                        Ok(()) => {
                                            info!("Unexpected early exit of kmsg receiver task.");
                                        },
                                        Err(e) => break Err(e),
                                    }
                                },
                                _ = receivers_cancel.cancelled() => {
                                    break wait::wait_for_tasks_with_timeout(&mut kmsg_task_set, Duration::from_millis(500)).await;
                                }
                            }
                        }
                    });
                }
            }
        }

        // Start the Kafka offset committer if we have one
        let mut kafka_offset_committer_task_set = JoinSet::new();
        #[cfg(feature = "rdkafka")]
        if let Some(mut committer) = kafka_offset_committer {
            let cancel_token = kafka_offset_committer_cancel.clone();
            kafka_offset_committer_task_set.spawn(async move {
                if let Err(e) = committer.run(cancel_token).await {
                    warn!("Kafka offset committer error: {:?}", e);
                }
                Ok(())
            });
        }

        // Start the File offset committer if we have one
        let mut file_offset_committer_task_set = JoinSet::new();
        #[cfg(feature = "file_receiver")]
        if let Some(mut committer) = file_offset_committer {
            let cancel_token = file_offset_committer_cancel.clone();
            file_offset_committer_task_set.spawn(async move {
                if let Err(e) = committer.run(cancel_token).await {
                    warn!("File offset committer error: {:?}", e);
                }
                Ok(())
            });
        }

        //
        // Logs input receiver
        //
        if let Some((mut logs_rx, mut logs_rx_flush_sub)) = self.logs_rx {
            let receivers_cancel = receivers_cancel.clone();
            let logs_output = logs_output.clone();
            let mut logs_flush_listener = Some(logs_rx_flush_sub.subscribe());

            receivers_task_set.spawn(async move {
                loop {
                    select! {
                        biased;

                        msg = logs_rx.next() => {
                            match msg {
                                None => break,
                                Some(msg) => {
                                    if let Some(out) = &logs_output {
                                        if let Err(e) = out.send(msg).await {
                                            // todo: is this possibly in a logging loop path?
                                            warn!("Unable to send logs to logs output: {}", e)
                                        }
                                    }
                                }
                            }
                        },
                        Some(resp) = conditional_flush(&mut logs_flush_listener) => {
                            // NOTE: We don't actually drain logs_rx here, instead we rely
                            // on the biased select loop to ensure that messages added to logs_rx
                            // before the flush message are consumed before the conditional_flush arm is
                            // executed.
                            match resp {
                                (Some(req), listener) => {
                                    debug!(request = ?req, "received flush for logs_rx channel");

                                    let logs_rx_len = logs_rx.len();
                                    if logs_rx_len > 0 {
                                        warn!(logs_rx_len, "received flush on logs_rx channel with pending messages");
                                    }

                                    if let Err(e) = listener.ack(req).await {
                                        warn!("unable to ack flush request: {}", e);
                                    }
                                },
                                (None, _) => warn!("logs_rx flush channel was closed")
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

        // Signal completed initialization
        if let Some(init_complete_chan) = self.init_complete_chan.take() {
            if let Err(e) = init_complete_chan.send(true) {
                warn!(error = ?e, "failed to notify completed initialization")
            }
        }

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
                    Ok(()) => {
                        info!("Unexpected early exit of receiver.");
                        },
                    Err(e) => result = Err(e),
                }
            },
            e = wait::wait_for_any_task(&mut pipeline_task_set) => {
                match e {
                    Ok(()) => {
                         info!("Unexpected early exit of pipeline.");
                    }
                    Err(e) => result = Err(e),
                }
            },
            e = wait::wait_for_any_task(&mut exporters_task_set) => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of exporter task."),
                    Err(e) => result = Err(e),
                }
            }
            e = wait::wait_for_any_task(&mut kafka_offset_committer_task_set), if !kafka_offset_committer_task_set.is_empty() => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of Kafka offset committer."),
                    Err(e) => result = Err(e),
                }
            }
            e = wait::wait_for_any_task(&mut file_offset_committer_task_set), if !file_offset_committer_task_set.is_empty() => {
                match e {
                    Ok(()) => warn!("Unexpected early exit of File offset committer."),
                    Err(e) => result = Err(e),
                }
            }
        }
        result?;

        // Step one, cancel the receivers and wait for their termination.
        receivers_cancel.cancel();

        // Wait up until one second for receivers to finish
        let res =
            wait::wait_for_tasks_with_timeout(&mut receivers_task_set, Duration::from_secs(3))
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

        // Now that exporters are done, cancel the Kafka offset committer
        if !kafka_offset_committer_task_set.is_empty() {
            let res = wait::wait_for_tasks_with_timeout(
                &mut kafka_offset_committer_task_set,
                Duration::from_secs(2),
            )
            .await;
            if res.is_err() {
                warn!("Kafka offset committer did not exit on channel close, cancelling.");
            }

            debug!("Cancelling Kafka offset committer after exporters shutdown");
            kafka_offset_committer_cancel.cancel();

            let res = wait::wait_for_tasks_with_timeout(
                &mut kafka_offset_committer_task_set,
                Duration::from_secs(3),
            )
            .await;

            if let Err(e) = res {
                warn!("Kafka offset committer did not exit within timeout: {}", e);
            } else {
                debug!("Kafka offset committer shut down successfully");
            }
        }

        // Now that exporters are done, cancel the File offset committer
        if !file_offset_committer_task_set.is_empty() {
            let res = wait::wait_for_tasks_with_timeout(
                &mut file_offset_committer_task_set,
                Duration::from_secs(2),
            )
            .await;
            if res.is_err() {
                warn!("File offset committer did not exit on channel close, cancelling.");
            }

            debug!("Cancelling File offset committer after exporters shutdown");
            #[cfg(feature = "file_receiver")]
            file_offset_committer_cancel.cancel();

            let res = wait::wait_for_tasks_with_timeout(
                &mut file_offset_committer_task_set,
                Duration::from_secs(3),
            )
            .await;

            if let Err(e) = res {
                warn!("File offset committer did not exit within timeout: {}", e);
            } else {
                debug!("File offset committer shut down successfully");
            }
        }

        #[cfg(feature = "prometheus")]
        {
            prom_cancel.cancel();
            if let Err(e) =
                wait::wait_for_tasks_with_timeout(&mut prom_task_set, Duration::from_secs(1)).await
            {
                warn!(
                    "Prometheus metrics server did not exit within timeout: {}",
                    e
                );
            }
        }

        Ok(())
    }
}

#[cfg(feature = "rdkafka")]
#[derive(Default)]
struct ReceiverTargetExporters {
    traces: Vec<String>,
    metrics: Vec<String>,
    logs: Vec<String>,
}

#[cfg(feature = "rdkafka")]
impl ReceiverTargetExporters {
    fn has_targets(&self) -> bool {
        !(self.traces.is_empty() && self.metrics.is_empty() && self.logs.is_empty())
    }
}

#[cfg(feature = "rdkafka")]
fn kafka_receiver_target_exporters(
    rec_config: &HashMap<Receiver, ReceiverConfig>,
) -> ReceiverTargetExporters {
    let mut targets = ReceiverTargetExporters::default();
    for config in rec_config.values() {
        let ReceiverConfig::Kafka(config) = config else {
            continue;
        };

        if let Some(trace_targets) = &config.target_exporters_traces {
            targets.traces.extend(trace_targets.iter().cloned());
        }
        if let Some(metric_targets) = &config.target_exporters_metrics {
            targets.metrics.extend(metric_targets.iter().cloned());
        }
        if let Some(log_targets) = &config.target_exporters_logs {
            targets.logs.extend(log_targets.iter().cloned());
        }
    }
    targets
}

fn new_optional_pipeline_output<T>(
    enabled: bool,
    capacity: usize,
) -> (
    Option<OTLPOutput<Message<T>>>,
    Option<BoundedReceiver<Message<T>>>,
) {
    if !enabled {
        return (None, None);
    }

    let (tx, rx) = bounded(capacity);
    (Some(OTLPOutput::new(tx)), Some(rx))
}

fn clone_channel_senders<T>(
    channels: &HashMap<
        String,
        (
            BoundedSender<Vec<Message<T>>>,
            BoundedReceiver<Vec<Message<T>>>,
        ),
    >,
) -> HashMap<String, BoundedSender<Vec<Message<T>>>> {
    channels
        .iter()
        .map(|(name, (tx, _))| (name.clone(), tx.clone()))
        .collect()
}

fn build_target_fanout<T>(
    targets: &[String],
    senders: &HashMap<String, BoundedSender<Vec<Message<T>>>>,
    telemetry_type: &'static str,
) -> Result<topology::fanout::Fanout<Vec<Message<T>>>, Box<dyn Error + Send + Sync>>
where
    T: Send + 'static,
{
    let mut fanout = FanoutBuilder::new(telemetry_type);
    for target in targets {
        let Some(sender) = senders.get(target) else {
            return Err(format!("Target exporter '{}' was not initialized", target).into());
        };
        fanout = fanout.add_tx(leak_config_name(target), sender.clone());
    }
    Ok(fanout.build()?)
}

fn leak_config_name(name: &str) -> &'static str {
    Box::leak(name.to_string().into_boxed_str())
}

/// For each export group: builds an `ExportGroup` task, registers its sender with `fanout`,
/// and returns (group_name, active_atomic) pairs for telemetry gauge registration.
/// Member rx values stay in `channels` for the caller to use when spawning exporters.
fn wire_export_groups<T>(
    groups: &[ExportGroupConfig],
    channels: &mut HashMap<
        String,
        (
            BoundedSender<Vec<Message<T>>>,
            BoundedReceiver<Vec<Message<T>>>,
        ),
    >,
    fanout: &mut FanoutBuilder<Vec<Message<T>>>,
    telemetry_type: &'static str,
    sending_queue_size: usize,
) -> Vec<(String, Arc<AtomicU32>)>
where
    T: Clone + Send + Sync + 'static,
{
    let mut atomics = Vec::new();
    for group in groups {
        let mut builder = ExportGroupBuilder::<T>::new(sending_queue_size)
            .trip_after(group.trip_after)
            .probe_after(group.probe_after);

        for member_name in &group.members {
            if let Some((tx, _)) = channels.get(member_name) {
                builder = builder.add_member(tx.clone());
            }
        }

        let export_group = builder.build();
        let active = export_group.active_atomic();

        // Group names are configured once at startup; leaking is acceptable.
        let static_name: &'static str = Box::leak(group.name.clone().into_boxed_str());
        *fanout = std::mem::replace(fanout, FanoutBuilder::new(telemetry_type))
            .add_tx(static_name, export_group.sender());

        atomics.push((group.name.clone(), active));
    }
    atomics
}

/// Registers an ObservableGauge for each export group's active-member index.
fn register_group_gauges(atomics: Vec<(String, Arc<AtomicU32>)>, telemetry_type: &'static str) {
    for (group_name, active_atomic) in atomics {
        global::meter("export_group")
            .u64_observable_gauge("rotel_export_group_active")
            .with_callback(move |observer| {
                observer.observe(
                    active_atomic.load(Ordering::Relaxed) as u64,
                    &[
                        opentelemetry::KeyValue::new("telemetry_type", telemetry_type),
                        opentelemetry::KeyValue::new("group_name", group_name.clone()),
                    ],
                );
            })
            .build();
    }
}

/// Prepares channel plumbing for one telemetry pipeline's export groups:
///   1. Pre-allocates (tx, rx) pairs for every exporter.
///   2. Builds retry-cap map from group configs.
///   3. Wires export groups (spawns tasks, registers senders with fanout).
///   4. Registers non-grouped exporters directly with the fanout.
///   5. Registers ObservableGauges for each group's active-member index.
///
/// Returns `(channels, retry_cap)` for use during exporter spawning.
fn setup_pipeline_channels<T: Clone + Send + Sync + 'static>(
    groups: &[ExportGroupConfig],
    exporter_names: &[String],
    fanout: &mut FanoutBuilder<Vec<Message<T>>>,
    telemetry_type: &'static str,
    sending_queue_size: usize,
) -> (
    HashMap<
        String,
        (
            BoundedSender<Vec<Message<T>>>,
            BoundedReceiver<Vec<Message<T>>>,
        ),
    >,
    HashMap<String, Duration>,
) {
    let grouped_members: HashSet<&str> = groups
        .iter()
        .flat_map(|g| g.members.iter().map(|m| m.as_str()))
        .collect();

    let member_retry_cap: HashMap<String, Duration> = groups
        .iter()
        .flat_map(|g| {
            g.members
                .iter()
                .map(move |m| (m.clone(), g.member_retry_max_elapsed_time))
        })
        .collect();

    let mut channels: HashMap<
        String,
        (
            BoundedSender<Vec<Message<T>>>,
            BoundedReceiver<Vec<Message<T>>>,
        ),
    > = HashMap::new();
    for exporter_name in exporter_names {
        let (tx, rx) = bounded(sending_queue_size);
        channels.insert(exporter_name.clone(), (tx, rx));
    }

    let atomics = wire_export_groups(
        groups,
        &mut channels,
        fanout,
        telemetry_type,
        sending_queue_size,
    );

    for exporter_name in exporter_names {
        if grouped_members.contains(exporter_name.as_str()) {
            continue;
        }
        let Some((tx, _)) = channels.get(exporter_name.as_str()) else {
            continue;
        };
        *fanout = std::mem::replace(fanout, FanoutBuilder::new(telemetry_type))
            .add_tx(leak_config_name(exporter_name), tx.clone());
    }

    register_group_gauges(atomics, telemetry_type);

    (channels, member_retry_cap)
}

fn start_otlp_exporter<Resource, Request, Response>(
    exporters_task_set: &mut JoinSet<Result<(), Box<dyn Error + Send + Sync>>>,
    telemetry_type: &'static str,
    exporter: otlp::exporter::Exporter<Resource, Request, Response>,
    cancel_token: CancellationToken,
) where
    Request: prost::Message + topology::payload::OTLPFrom<Vec<Resource>> + Clone,
    Resource: prost::Message + std::fmt::Debug + Clone,
    [Resource]: BatchSizer,
    Response: prost::Message + std::fmt::Debug + Default + Clone,
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
