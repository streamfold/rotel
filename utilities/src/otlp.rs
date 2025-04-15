use chrono::Utc;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Status};

pub struct FakeOTLP;

impl FakeOTLP {
    pub fn logs_service_request() -> ExportLogsServiceRequest {
        Self::logs_service_request_with_logs(1, 1)
    }

    pub fn logs_service_request_with_logs(
        num_resource_logs: usize,
        num_logs: usize,
    ) -> ExportLogsServiceRequest {
        let mut exp = ExportLogsServiceRequest {
            resource_logs: Vec::with_capacity(num_resource_logs),
        };
        for _i in 0..num_resource_logs {
            exp.resource_logs.push(Self::resource_logs(num_logs));
        }
        exp
    }

    fn resource_logs(num_logs: usize) -> ResourceLogs {
        let mut log_records = Vec::with_capacity(num_logs);
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap();
        for _ in 0..num_logs {
            let log = LogRecord {
                time_unix_nano: now_ns as u64,
                observed_time_unix_nano: now_ns as u64,
                severity_number: 0,
                severity_text: "WARNING".to_string(),
                body: Some(AnyValue {
                    value: Some(StringValue("This is a log message".to_string())),
                }),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![],
                span_id: vec![],
                event_name: "".to_string(),
            };
            log_records.push(log);
        }

        let scope_logs = ScopeLogs {
            scope: None,
            log_records,
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        };

        ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    string_attr("service.name", "test-service"),
                    string_attr("telemetry.sdk.version", "1.13.0"),
                    string_attr("telemetry.sdk.name", "open-telemetry"),
                    string_attr("k8s.pod.uid", "dc2c3e55-0dfb-4fda-854c-f7a1e5f88fd6"),
                    string_attr("k8s.node.name", "ip-10-250-64-50.ec2.internal"),
                    string_attr(
                        "container.id",
                        "b1e5232f92b315b7d91052e2c1b09de3735bea5b51c983a2a81ff3d69dfd0359",
                    ),
                ],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![scope_logs],
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        }
    }

    pub fn metrics_service_request() -> ExportMetricsServiceRequest {
        Self::metrics_service_request_with_metrics(1, 1)
    }

    pub fn metrics_service_request_with_metrics(
        num_resource_metrics: usize,
        num_metrics: usize,
    ) -> ExportMetricsServiceRequest {
        let mut exp = ExportMetricsServiceRequest {
            resource_metrics: Vec::with_capacity(num_resource_metrics),
        };
        for _i in 0..num_resource_metrics {
            exp.resource_metrics
                .push(Self::resource_metrics(num_metrics));
        }
        exp
    }

    fn resource_metrics(num_metrics: usize) -> ResourceMetrics {
        let mut metrics = Vec::with_capacity(num_metrics);
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap();
        for _ in 0..num_metrics {
            let dp = vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: now_ns as u64,
                time_unix_nano: now_ns as u64,
                exemplars: vec![],
                flags: 0,
                value: Some(Value::AsDouble(100.0)),
            }];
            let metric = Metric {
                name: "test-metric".to_string(),
                description: "An example OTLP Metric".to_string(),
                unit: "".to_string(),
                metadata: vec![],
                data: Some(Data::Gauge(Gauge { data_points: dp })),
            };
            metrics.push(metric)
        }

        let scope_metrics = ScopeMetrics {
            scope: None,
            metrics,
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        };

        ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    string_attr("service.name", "test-service"),
                    string_attr("telemetry.sdk.version", "1.13.0"),
                    string_attr("telemetry.sdk.name", "open-telemetry"),
                    string_attr("k8s.pod.uid", "dc2c3e55-0dfb-4fda-854c-f7a1e5f88fd6"),
                    string_attr("k8s.node.name", "ip-10-250-64-50.ec2.internal"),
                    string_attr(
                        "container.id",
                        "b1e5232f92b315b7d91052e2c1b09de3735bea5b51c983a2a81ff3d69dfd0359",
                    ),
                ],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![scope_metrics],
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn trace_service_request() -> ExportTraceServiceRequest {
        Self::trace_service_request_with_spans(1, 1)
    }

    pub fn trace_service_request_with_spans(
        num_res_spans: usize,
        num_spans: usize,
    ) -> ExportTraceServiceRequest {
        let mut exp = ExportTraceServiceRequest {
            resource_spans: Vec::with_capacity(num_res_spans),
        };
        for _i in 0..num_res_spans {
            exp.resource_spans.push(Self::resource_spans(num_spans));
        }
        exp
    }

    fn resource_spans(num_spans: usize) -> ResourceSpans {
        let spans = Self::trace_spans(num_spans);

        let scope_spans = ScopeSpans {
            scope: Some(InstrumentationScope {
                name: "scope".to_string(),
                version: "0.0.1".to_string(),
                attributes: vec![string_attr("module", "api")],
                dropped_attributes_count: 0,
            }),
            spans,
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        };
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_attr("service.name", "test-service"),
                    string_attr("telemetry.sdk.version", "1.13.0"),
                    string_attr("telemetry.sdk.name", "open-telemetry"),
                    string_attr("k8s.pod.uid", "dc2c3e55-0dfb-4fda-854c-f7a1e5f88fd6"),
                    string_attr("k8s.node.name", "ip-10-250-64-50.ec2.internal"),
                    string_attr(
                        "container.id",
                        "b1e5232f92b315b7d91052e2c1b09de3735bea5b51c983a2a81ff3d69dfd0359",
                    ),
                ],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![scope_spans],
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_string(),
        }
    }

    pub fn trace_spans(num_spans: usize) -> Vec<v1::Span> {
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap();
        let finish_ns = now_ns + 1_000_000;
        let mut spans = Vec::with_capacity(num_spans);
        for _ in 0..num_spans {
            let span = v1::Span {
                trace_id: vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                span_id: vec![2, 2, 2, 2, 2, 2, 2, 2],
                trace_state: "rojo=00f067aa0ba902b7".to_string(),
                parent_span_id: vec![1, 1, 1, 1, 1, 1, 1, 1],
                flags: 0,
                name: "foo".to_string(),
                kind: SpanKind::Internal.into(),
                start_time_unix_nano: now_ns as u64,
                end_time_unix_nano: finish_ns as u64,
                attributes: vec![
                    string_attr("http.method", "POST"),
                    string_attr("http.request.path", "/items"),
                ],
                dropped_attributes_count: 0,
                events: vec![],
                dropped_events_count: 0,
                links: vec![],
                dropped_links_count: 0,
                status: Some(Status::default()),
            };
            spans.push(span);
        }
        spans
    }
}

pub fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(StringValue(value.to_string())),
        }),
    }
}
