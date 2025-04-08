use crate::processor::model::Value::{BoolValue, BytesValue, DoubleValue, IntValue, StringValue};
use crate::processor::model::{Resource, ScopeSpans, Span, Value};
use std::mem;
use std::sync::{Arc, Mutex};

pub fn transform_spans(
    scope_spans: Vec<Arc<Mutex<ScopeSpans>>>,
) -> Vec<opentelemetry_proto::tonic::trace::v1::ScopeSpans> {
    let mut new_scope_spans = vec![];
    for ss in scope_spans.iter() {
        let ss = ss.lock().unwrap();
        let ss = ss.spans.lock().unwrap();
        let mut new_spans = vec![];
        for span in ss.iter() {
            let mut guard = span.lock().unwrap();
            let moved_data = mem::replace(
                &mut *guard,
                Span {
                    trace_id: vec![],
                    span_id: vec![],
                    trace_state: "".to_string(),
                    parent_span_id: vec![],
                    flags: 0,
                    name: "".to_string(),
                    kind: 0,
                    start_time_unix_nano: 0,
                    end_time_unix_nano: 0,
                    attributes: Arc::new(Default::default()),
                    dropped_attributes_count: 0,
                    dropped_events_count: 0,
                    dropped_links_count: 0,
                    status: Arc::new(Default::default()),
                },
            );
            new_spans.push(opentelemetry_proto::tonic::trace::v1::Span {
                trace_id: moved_data.trace_id,
                span_id: moved_data.span_id,
                trace_state: moved_data.trace_state,
                parent_span_id: moved_data.parent_span_id,
                flags: moved_data.flags,
                name: moved_data.name,
                kind: moved_data.kind,
                start_time_unix_nano: moved_data.start_time_unix_nano,
                end_time_unix_nano: moved_data.end_time_unix_nano,
                attributes: vec![],
                dropped_attributes_count: moved_data.dropped_attributes_count,
                events: vec![],
                dropped_events_count: moved_data.dropped_events_count,
                links: vec![],
                dropped_links_count: moved_data.dropped_links_count,
                status: None,
            })
        }
        new_scope_spans.push(opentelemetry_proto::tonic::trace::v1::ScopeSpans {
            scope: None,
            spans: new_spans,
            schema_url: "".to_string(),
        })
    }
    new_scope_spans
}

pub fn transform_resource(
    resource: Resource,
) -> Option<opentelemetry_proto::tonic::resource::v1::Resource> {
    let attributes = resource.attributes.lock().unwrap();
    let mut new_attrs = vec![];
    for attr in attributes.iter() {
        let kv = attr.lock().unwrap();
        let key = kv.key.lock().unwrap();
        let key = key.to_string();
        let mut any_value = kv.value.lock().unwrap();
        let any_value = any_value.take();
        if any_value.is_some() {
            let value = any_value.unwrap();
            let value = value.value.lock().unwrap().take();
            if value.is_some() {
                let value = value.unwrap();
                match value {
                    StringValue(s) => {
                        new_attrs.push(opentelemetry_proto::tonic::common::v1::KeyValue {
                            key,
                            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s))
                            }),
                        })
                    }
                    BoolValue(b) => {
                        new_attrs.push(opentelemetry_proto::tonic::common::v1::KeyValue {
                            key,
                            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b))
                            }),
                        })
                    }
                    IntValue(i) => {
                        new_attrs.push(opentelemetry_proto::tonic::common::v1::KeyValue {
                            key,
                            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i))
                            }),
                        })
                    }
                    DoubleValue(d) => {
                        new_attrs.push(opentelemetry_proto::tonic::common::v1::KeyValue {
                            key,
                            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d))
                            }),
                        })
                    }
                    Value::ArrayValue(_) => {}
                    Value::KvListValue(_) => {}
                    BytesValue(b) => {
                        new_attrs.push(opentelemetry_proto::tonic::common::v1::KeyValue {
                            key,
                            value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b))
                            }),
                        })
                    }
                }
            }
        } else {
            new_attrs.push(opentelemetry_proto::tonic::common::v1::KeyValue { key, value: None })
        }
    }
    Some(opentelemetry_proto::tonic::resource::v1::Resource {
        attributes: new_attrs,
        dropped_attributes_count: 0,
    })
}
