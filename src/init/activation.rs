use crate::init::args::{AgentRun, Exporter};

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
    pub fn from_config(config: &AgentRun) -> Self {
        let mut activation = match config.exporter {
            Exporter::Otlp => {
                let has_global_endpoint = config.otlp_exporter.otlp_exporter_endpoint.is_some();
                let mut activation = TelemetryActivation::default();
                if !has_global_endpoint
                    && config.otlp_exporter.otlp_exporter_traces_endpoint.is_none()
                {
                    activation.traces = TelemetryState::NoListeners
                }
                if !has_global_endpoint
                    && config
                        .otlp_exporter
                        .otlp_exporter_metrics_endpoint
                        .is_none()
                {
                    activation.metrics = TelemetryState::NoListeners
                }
                if !has_global_endpoint
                    && config.otlp_exporter.otlp_exporter_logs_endpoint.is_none()
                {
                    activation.logs = TelemetryState::NoListeners
                }
                activation
            }
            Exporter::Blackhole => TelemetryActivation::default(),
            Exporter::Datadog => {
                // Only supports traces for now
                TelemetryActivation {
                    traces: TelemetryState::Active,
                    metrics: TelemetryState::NoListeners,
                    logs: TelemetryState::NoListeners,
                }
            }
            Exporter::Clickhouse => TelemetryActivation {
                logs: TelemetryState::Active,
                traces: TelemetryState::Active,
                metrics: TelemetryState::Active,
            },
            Exporter::AwsXray => TelemetryActivation {
                logs: TelemetryState::NoListeners,
                traces: TelemetryState::Active,
                metrics: TelemetryState::NoListeners,
            },
        };

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
