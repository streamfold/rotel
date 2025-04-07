use crate::processor::model::Span;
use crate::processor::model::Value::{BoolValue, BytesValue, DoubleValue, IntValue, StringValue};
use chrono::Utc;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};
use std::sync::{Arc, Mutex};

pub fn prepare_resource_spans() -> crate::processor::model::ResourceSpans {
    let export_req = trace_service_request_with_spans(1, 1);
    let mut resource_span = crate::processor::model::ResourceSpans {
        resource: Arc::new(Mutex::new(None)),
        scope_spans: Arc::new(Mutex::new(vec![])),
        schema_url: Arc::new(Mutex::new("".to_string())),
    };
    for rs in export_req.resource_spans {
        if rs.resource.is_some() {
            let resource = rs.resource.unwrap();
            let kvs = build_rotel_sdk_resource(resource);
            let res = Arc::new(Mutex::new(Some(crate::processor::model::Resource {
                attributes: Arc::new(Mutex::new(kvs.clone())),
                dropped_attributes_count: Arc::new(Mutex::new(0)),
            })));
            resource_span.resource = res.clone()
        }
        let mut scope_spans = vec![];
        for ss in rs.scope_spans {
            let scope_span = crate::processor::model::ScopeSpans {
                spans: Arc::new(Mutex::new(vec![])),
                schema_url: Arc::new(Mutex::new("".to_string())),
            };
            for s in ss.spans {
                let span = Span {
                    trace_id: s.trace_id,
                    span_id: s.span_id,
                    trace_state: s.trace_state,
                    parent_span_id: s.parent_span_id,
                    flags: s.flags,
                    name: s.name,
                    kind: s.kind,
                    start_time_unix_nano: s.start_time_unix_nano,
                    end_time_unix_nano: s.end_time_unix_nano,
                    attributes: Arc::new(Mutex::new(vec![])),
                    dropped_attributes_count: s.dropped_attributes_count,
                    dropped_events_count: s.dropped_events_count,
                    dropped_links_count: s.dropped_links_count,
                    status: Arc::new(Mutex::new(None)),
                };
                scope_span
                    .spans
                    .lock()
                    .unwrap()
                    .push(Arc::new(Mutex::new(span)))
            }
            scope_spans.push(Arc::new(Mutex::new(scope_span)))
        }
        resource_span.scope_spans = Arc::new(Mutex::new(scope_spans))
    }
    resource_span
}

fn build_rotel_sdk_resource(
    resource: Resource,
) -> Vec<Arc<Mutex<crate::processor::model::KeyValue>>> {
    let mut kvs = vec![];
    for a in resource.attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => {}
            Some(v) => match v.value {
                None => {
                    let kv = crate::processor::model::KeyValue {
                        key,
                        value: Arc::new(Mutex::new(Some(crate::processor::model::AnyValue {
                            value: Arc::new(Mutex::new(None)),
                        }))),
                    };
                    kvs.push(Arc::new(Mutex::new(kv)));
                }
                Some(v) => match v {
                    Value::StringValue(s) => {
                        let kv = crate::processor::model::KeyValue {
                            key,
                            value: Arc::new(Mutex::new(Some(crate::processor::model::AnyValue {
                                value: Arc::new(Mutex::new(Some(StringValue(s)))),
                            }))),
                        };
                        kvs.push(Arc::new(Mutex::new(kv)));
                    }
                    Value::BoolValue(b) => {
                        let kv = crate::processor::model::KeyValue {
                            key,
                            value: Arc::new(Mutex::new(Some(crate::processor::model::AnyValue {
                                value: Arc::new(Mutex::new(Some(BoolValue(b)))),
                            }))),
                        };
                        kvs.push(Arc::new(Mutex::new(kv)));
                    }
                    Value::IntValue(i) => {
                        let kv = crate::processor::model::KeyValue {
                            key,
                            value: Arc::new(Mutex::new(Some(crate::processor::model::AnyValue {
                                value: Arc::new(Mutex::new(Some(IntValue(i)))),
                            }))),
                        };
                        kvs.push(Arc::new(Mutex::new(kv)));
                    }
                    Value::DoubleValue(d) => {
                        let kv = crate::processor::model::KeyValue {
                            key,
                            value: Arc::new(Mutex::new(Some(crate::processor::model::AnyValue {
                                value: Arc::new(Mutex::new(Some(DoubleValue(d)))),
                            }))),
                        };
                        kvs.push(Arc::new(Mutex::new(kv)));
                    }
                    Value::ArrayValue(_) => {}
                    Value::KvlistValue(_) => {}
                    Value::BytesValue(b) => {
                        let kv = crate::processor::model::KeyValue {
                            key,
                            value: Arc::new(Mutex::new(Some(crate::processor::model::AnyValue {
                                value: Arc::new(Mutex::new(Some(BytesValue(b)))),
                            }))),
                        };
                        kvs.push(Arc::new(Mutex::new(kv)));
                    }
                },
            },
        }
    }
    kvs
}

#[allow(dead_code)]
pub fn trace_service_request() -> ExportTraceServiceRequest {
    trace_service_request_with_spans(1, 1)
}

pub fn trace_service_request_with_spans(
    num_res_spans: usize,
    num_spans: usize,
) -> ExportTraceServiceRequest {
    let mut exp = ExportTraceServiceRequest {
        resource_spans: Vec::with_capacity(num_res_spans),
    };
    for _i in 0..num_res_spans {
        exp.resource_spans.push(resource_spans(num_spans));
    }
    exp
}

fn resource_spans(num_spans: usize) -> ResourceSpans {
    let spans = trace_spans(num_spans);

    let scope_spans = ScopeSpans {
        scope: None,
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
            status: None,
        };
        spans.push(span);
    }
    spans
}

pub fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}
