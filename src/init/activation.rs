use crate::init::args::AgentRun;
use crate::init::config::ExporterConfigs;

#[derive(Default)]
pub struct TelemetryActivation {
    pub traces: TelemetryState,
    pub metrics: TelemetryState,
    pub logs: TelemetryState,
}

#[derive(Default, PartialEq)]
pub enum TelemetryState {
    #[default]
    Active,
    Disabled,
    NoListeners,
}

impl TelemetryActivation {
    pub(crate) fn from_config(config: &AgentRun, exporter_config: &ExporterConfigs) -> Self {
        let mut activation = TelemetryActivation::default();

        // Update based on exporters
        if exporter_config.traces.is_none() {
            activation.traces = TelemetryState::NoListeners;
        }
        if exporter_config.metrics.is_none() {
            activation.metrics = TelemetryState::NoListeners;
        }
        if exporter_config.logs.is_none() {
            activation.logs = TelemetryState::NoListeners;
        }

        // Check if any are explicitly disabled
        if config.otlp_receiver_traces_disabled {
            activation.traces = TelemetryState::Disabled
        }
        if config.otlp_receiver_metrics_disabled {
            activation.metrics = TelemetryState::Disabled
        }
        if config.otlp_receiver_logs_disabled {
            activation.logs = TelemetryState::Disabled
        }

        activation
    }
}
