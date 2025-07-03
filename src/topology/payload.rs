// SPDX-License-Identifier: Apache-2.0

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

/// Trait for converting telemetry data into OTLP protocol format
pub trait OTLPFrom<T> {
    fn otlp_from(value: T) -> Self;
}

/// Trait for converting telemetry data into OTLP protocol format
pub trait OTLPInto<T> {
    fn otlp_into(self) -> T;
}

impl OTLPFrom<Vec<ResourceSpans>> for ExportTraceServiceRequest {
    fn otlp_from(value: Vec<ResourceSpans>) -> Self {
        ExportTraceServiceRequest {
            resource_spans: value,
        }
    }
}

impl OTLPFrom<Vec<ResourceMetrics>> for ExportMetricsServiceRequest {
    fn otlp_from(value: Vec<ResourceMetrics>) -> Self {
        ExportMetricsServiceRequest {
            resource_metrics: value,
        }
    }
}

impl OTLPFrom<Vec<ResourceLogs>> for ExportLogsServiceRequest {
    fn otlp_from(value: Vec<ResourceLogs>) -> Self {
        ExportLogsServiceRequest {
            resource_logs: value,
        }
    }
}

impl OTLPInto<Vec<ResourceSpans>> for ExportTraceServiceRequest {
    fn otlp_into(self) -> Vec<ResourceSpans> {
        self.resource_spans
    }
}

impl OTLPInto<Vec<ResourceMetrics>> for ExportMetricsServiceRequest {
    fn otlp_into(self) -> Vec<ResourceMetrics> {
        self.resource_metrics
    }
}

impl OTLPInto<Vec<ResourceLogs>> for ExportLogsServiceRequest {
    fn otlp_into(self) -> Vec<ResourceLogs> {
        self.resource_logs
    }
}

#[derive(Clone, Debug)]
pub enum OTLPPayload {
    Traces(Vec<ResourceSpans>),
    Metrics(Vec<ResourceMetrics>),
    Logs(Vec<ResourceLogs>),
}

impl From<Vec<ResourceMetrics>> for OTLPPayload {
    fn from(value: Vec<ResourceMetrics>) -> Self {
        OTLPPayload::Metrics(value)
    }
}

impl From<Vec<ResourceSpans>> for OTLPPayload {
    fn from(value: Vec<ResourceSpans>) -> Self {
        OTLPPayload::Traces(value)
    }
}

impl From<Vec<ResourceLogs>> for OTLPPayload {
    fn from(value: Vec<ResourceLogs>) -> Self {
        OTLPPayload::Logs(value)
    }
}

impl TryFrom<OTLPPayload> for Vec<ResourceSpans> {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Traces(v) => Ok(v),
            OTLPPayload::Metrics(_) => Err("Cannot convert metrics payload to traces"),
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to traces"),
        }
    }
}

impl TryFrom<OTLPPayload> for Vec<ResourceMetrics> {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Metrics(v) => Ok(v),
            OTLPPayload::Traces(_) => Err("Cannot convert traces payload to metrics"),
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to metrics"),
        }
    }
}

impl TryFrom<OTLPPayload> for Vec<ResourceLogs> {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Logs(v) => Ok(v),
            OTLPPayload::Traces(_) => Err("Cannot convert traces payload to logs"),
            OTLPPayload::Metrics(_) => Err("Cannot convert metrics payload to logs"),
        }
    }
}

impl TryFrom<OTLPPayload> for ExportTraceServiceRequest {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Traces(spans) => Ok(ExportTraceServiceRequest {
                resource_spans: spans,
            }),
            OTLPPayload::Metrics(_) => {
                Err("Cannot convert metrics payload to trace service request")
            }
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to trace service request"),
        }
    }
}

impl TryFrom<OTLPPayload> for ExportMetricsServiceRequest {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Metrics(metrics) => Ok(ExportMetricsServiceRequest {
                resource_metrics: metrics,
            }),
            OTLPPayload::Traces(_) => {
                Err("Cannot convert traces payload to metrics service request")
            }
            OTLPPayload::Logs(_) => Err("Cannot convert logs payload to metrics service request"),
        }
    }
}

impl TryFrom<OTLPPayload> for ExportLogsServiceRequest {
    type Error = &'static str;

    fn try_from(value: OTLPPayload) -> Result<Self, Self::Error> {
        match value {
            OTLPPayload::Logs(logs) => Ok(ExportLogsServiceRequest {
                resource_logs: logs,
            }),
            OTLPPayload::Traces(_) => Err("Cannot convert traces payload to logs service request"),
            OTLPPayload::Metrics(_) => {
                Err("Cannot convert metrics payload to logs service request")
            }
        }
    }
}
