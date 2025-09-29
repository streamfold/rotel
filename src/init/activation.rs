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

        if all_logs_receivers_disabled(receiver_config) {
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
            ReceiverConfig::Kafka(k) => {
                if k.traces {
                    return false;
                }
            }
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
            ReceiverConfig::Kafka(k) => {
                if k.metrics {
                    return false;
                }
            }
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
            ReceiverConfig::Kafka(k) => {
                if k.logs {
                    return false;
                }
            }
        }
    }
    true
}
