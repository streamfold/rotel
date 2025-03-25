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

// Wondering if this could bite someone later, might be better to do a try_from?
impl From<OTLPPayload> for Vec<ResourceSpans> {
    fn from(value: OTLPPayload) -> Self {
        match value {
            OTLPPayload::Metrics(_) => vec![],
            OTLPPayload::Traces(v) => v,
        }
    }
}

impl From<OTLPPayload> for Vec<ResourceMetrics> {
    fn from(value: OTLPPayload) -> Self {
        match value {
            OTLPPayload::Metrics(v) => v,
            OTLPPayload::Traces(_) => vec![],
        }
    }
}

// Wondering if this could bite someone later, might be better to do a try_from?
impl From<OTLPPayload> for ExportTraceServiceRequest {
    fn from(value: OTLPPayload) -> Self {
        match value {
            OTLPPayload::Metrics(_) => ExportTraceServiceRequest::default(),
            OTLPPayload::Traces(spans) => ExportTraceServiceRequest {
                resource_spans: spans,
            },
        }
    }
}

impl From<OTLPPayload> for ExportMetricsServiceRequest {
    fn from(value: OTLPPayload) -> Self {
        match value {
            OTLPPayload::Metrics(metrics) => ExportMetricsServiceRequest {
                resource_metrics: metrics,
            },
            OTLPPayload::Traces(_) => ExportMetricsServiceRequest::default(),
        }
    }
}
