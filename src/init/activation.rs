use crate::init::args::Receiver;
use crate::init::config::{ExporterConfigs, ReceiverConfig};
use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct TelemetryActivation {
    pub traces: TelemetryState,
    pub metrics: TelemetryState,
    pub logs: TelemetryState,
}

#[derive(Default, PartialEq, Debug)]
pub enum TelemetryState {
    #[default]
    Active,
    Disabled,
    NoListeners,
}

impl TelemetryActivation {
    pub(crate) fn from_config(
        receiver_config: &HashMap<Receiver, ReceiverConfig>,
        exporter_config: &ExporterConfigs,
        logs_rx_active: bool,
    ) -> Self {
        let mut activation = TelemetryActivation::default();

        // Update based on exporters
        if exporter_config.traces.is_empty() {
            activation.traces = TelemetryState::NoListeners;
        }
        if exporter_config.metrics.is_empty() {
            activation.metrics = TelemetryState::NoListeners;
        }
        if exporter_config.logs.is_empty() {
            activation.logs = TelemetryState::NoListeners;
        }

        if all_traces_receivers_disabled(receiver_config) {
            activation.traces = TelemetryState::Disabled;
        }

        if all_metrics_receivers_disabled(receiver_config) {
            activation.metrics = TelemetryState::Disabled;
        }

        if all_logs_receivers_disabled(receiver_config) && !logs_rx_active {
            activation.logs = TelemetryState::Disabled;
        }

        activation
    }
}

fn all_traces_receivers_disabled(rc: &HashMap<Receiver, ReceiverConfig>) -> bool {
    for receiver_config in rc.values() {
        match receiver_config {
            ReceiverConfig::Otlp(o) => {
                if !o.otlp_receiver_traces_disabled {
                    return false;
                }
            }
            #[cfg(feature = "rdkafka")]
            ReceiverConfig::Kafka(k) => {
                if k.traces {
                    return false;
                }
            }
            #[cfg(feature = "fluent_receiver")]
            ReceiverConfig::Fluent(_) => {}
            #[cfg(feature = "file_receiver")]
            ReceiverConfig::File(_) => {} // File receiver doesn't handle traces
            #[cfg(all(target_os = "linux", feature = "kmsg_receiver"))]
            ReceiverConfig::Kmsg(_) => {} // Kmsg receiver doesn't handle traces
            #[cfg(feature = "node_metrics_receiver")]
            ReceiverConfig::NodeMetrics(_) => {} // Node metrics receiver doesn't handle traces
        }
    }
    true
}

fn all_metrics_receivers_disabled(rc: &HashMap<Receiver, ReceiverConfig>) -> bool {
    for receiver_config in rc.values() {
        match receiver_config {
            ReceiverConfig::Otlp(o) => {
                if !o.otlp_receiver_metrics_disabled {
                    return false;
                }
            }
            #[cfg(feature = "rdkafka")]
            ReceiverConfig::Kafka(k) => {
                if k.metrics {
                    return false;
                }
            }

            #[cfg(feature = "fluent_receiver")]
            ReceiverConfig::Fluent(_) => {}
            #[cfg(feature = "file_receiver")]
            ReceiverConfig::File(_) => {} // File receiver doesn't handle metrics
            #[cfg(all(target_os = "linux", feature = "kmsg_receiver"))]
            ReceiverConfig::Kmsg(_) => {} // Kmsg receiver doesn't handle metrics
            #[cfg(feature = "node_metrics_receiver")]
            ReceiverConfig::NodeMetrics(_) => return false, // Node metrics receiver handles metrics
        }
    }
    true
}

fn all_logs_receivers_disabled(rc: &HashMap<Receiver, ReceiverConfig>) -> bool {
    for receiver_config in rc.values() {
        match receiver_config {
            ReceiverConfig::Otlp(o) => {
                if !o.otlp_receiver_logs_disabled {
                    return false;
                }
            }
            #[cfg(feature = "rdkafka")]
            ReceiverConfig::Kafka(k) => {
                if k.logs {
                    return false;
                }
            }
            #[cfg(feature = "fluent_receiver")]
            ReceiverConfig::Fluent(_) => return false,
            #[cfg(feature = "file_receiver")]
            ReceiverConfig::File(_) => return false, // File receiver handles logs
            #[cfg(all(target_os = "linux", feature = "kmsg_receiver"))]
            ReceiverConfig::Kmsg(_) => return false, // Kmsg receiver handles logs
            #[cfg(feature = "node_metrics_receiver")]
            ReceiverConfig::NodeMetrics(_) => {} // Node metrics receiver doesn't handle logs
        }
    }
    true
}

#[cfg(all(test, feature = "node_metrics_receiver"))]
mod tests {
    use super::*;
    use crate::init::config::ExporterConfig;
    use crate::receivers::node_metrics::config::NodeMetricsConfig;

    /// A receiver map holding only the node metrics receiver
    fn node_metrics_only() -> HashMap<Receiver, ReceiverConfig> {
        HashMap::from([(
            Receiver::NodeMetrics,
            ReceiverConfig::NodeMetrics(NodeMetricsConfig::default()),
        )])
    }

    /// The node metrics receiver is the only source of metrics in its own pipeline.
    ///
    /// This single match arm decides whether the metrics pipeline is activated at all:
    /// folded in with the neighbouring receivers that do not produce metrics, the
    /// receiver would scrape forever into a pipeline that was never started.
    #[test]
    fn test_node_metrics_receiver_keeps_the_metrics_pipeline_enabled() {
        let receivers = node_metrics_only();

        assert!(
            !all_metrics_receivers_disabled(&receivers),
            "a node metrics receiver must keep the metrics pipeline enabled"
        );

        // The `false` above has to come from the node metrics arm, not from an
        // unconditionally false result: with no receivers at all, metrics stay disabled.
        assert!(all_metrics_receivers_disabled(&HashMap::new()));

        // It produces neither traces nor logs, so those pipelines stay disabled — the
        // arms that would activate a whole pipeline for nothing.
        assert!(all_traces_receivers_disabled(&receivers));
        assert!(all_logs_receivers_disabled(&receivers));
    }

    /// The same decision as seen by the agent, which acts on `TelemetryActivation`.
    #[test]
    fn test_activation_with_only_a_node_metrics_receiver() {
        let exporters = ExporterConfigs {
            metrics: vec![ExporterConfig::Blackhole],
            ..Default::default()
        };

        let activation = TelemetryActivation::from_config(&node_metrics_only(), &exporters, false);

        // Metrics are the only pipeline this receiver feeds: it has both a receiver and
        // an exporter, so it is the only one that comes up.
        assert_eq!(activation.metrics, TelemetryState::Active);
        assert_eq!(activation.traces, TelemetryState::Disabled);
        assert_eq!(activation.logs, TelemetryState::Disabled);
    }

    /// Without a metrics exporter there is nowhere to send scrapes, which is reported as
    /// `NoListeners` rather than as a disabled receiver.
    #[test]
    fn test_activation_without_a_metrics_exporter() {
        let activation = TelemetryActivation::from_config(
            &node_metrics_only(),
            &ExporterConfigs::default(),
            false,
        );

        assert_eq!(activation.metrics, TelemetryState::NoListeners);
    }
}
