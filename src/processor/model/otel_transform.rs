use crate::processor::model::Value::{
    ArrayValue, BoolValue, BytesValue, DoubleValue, IntValue, StringValue,
};
use crate::processor::model::{
    AnyValue, InstrumentationScope, KeyValue, ResourceSpans, Span, Status,
};
use std::sync::{Arc, Mutex};

pub fn transform(rs: opentelemetry_proto::tonic::trace::v1::ResourceSpans) -> ResourceSpans {
    let mut resource_span = ResourceSpans {
        resource: Arc::new(Mutex::new(None)),
        scope_spans: Arc::new(Mutex::new(vec![])),
        schema_url: rs.schema_url,
    };
    if rs.resource.is_some() {
        let resource = rs.resource.unwrap();
        let kvs = build_rotel_sdk_resource(resource);
        let res = Arc::new(Mutex::new(Some(crate::processor::model::Resource {
            attributes: Arc::new(Mutex::new(kvs.clone())),
            // TODO - copy dropped_attributes_count
            dropped_attributes_count: 0,
        })));
        resource_span.resource = res.clone()
    }
    let mut scope_spans = vec![];
    for ss in rs.scope_spans {
        let mut scope = None;
        if ss.scope.is_some() {
            let s = ss.scope.unwrap();
            scope = Some(InstrumentationScope {
                name: s.name,
                version: s.version,
                attributes: Arc::new(Mutex::new(convert_attributes(s.attributes))),
                dropped_attributes_count: s.dropped_attributes_count,
            })
        }
        let scope_span = crate::processor::model::ScopeSpans {
            scope: Arc::new(Mutex::new(scope)),
            spans: Arc::new(Mutex::new(vec![])),
            schema_url: ss.schema_url,
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
                // TODO add attributes copy
                attributes: Arc::new(Mutex::new(vec![])),
                dropped_attributes_count: s.dropped_attributes_count,
                dropped_events_count: s.dropped_events_count,
                dropped_links_count: s.dropped_links_count,
                status: Arc::new(Mutex::new(s.status.map(|status| Status {
                    message: status.message,
                    code: status.code,
                }))),
            };
            scope_span
                .spans
                .lock()
                .unwrap()
                .push(Arc::new(Mutex::new(span)))
        }
        scope_spans.push(Arc::new(Mutex::new(scope_span)))
    }
    resource_span.scope_spans = Arc::new(Mutex::new(scope_spans));
    resource_span
}

fn convert_attributes(
    attributes: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
) -> Vec<crate::processor::model::KeyValue> {
    let mut kvs = vec![];
    for a in attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => kvs.push(KeyValue {
                key,
                value: Arc::new(Mutex::new(None)),
            }),
            Some(v) => {
                let converted = convert_value(v);
                kvs.push(KeyValue {
                    key,
                    value: Arc::new(Mutex::new(Some(converted))),
                })
            }
        }
    }
    kvs
}

fn build_rotel_sdk_resource(
    resource: opentelemetry_proto::tonic::resource::v1::Resource,
) -> Vec<Arc<Mutex<crate::processor::model::KeyValue>>> {
    let mut kvs = vec![];
    for a in resource.attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => kvs.push(Arc::new(Mutex::new(KeyValue {
                key,
                value: Arc::new(Mutex::new(None)),
            }))),
            Some(v) => {
                let converted = convert_value(v);
                kvs.push(Arc::new(Mutex::new(KeyValue {
                    key,
                    value: Arc::new(Mutex::new(Some(converted))),
                })))
            }
        }
    }
    kvs
}

pub fn convert_value(v: opentelemetry_proto::tonic::common::v1::AnyValue) -> AnyValue {
    match v.value {
        None => AnyValue {
            value: Arc::new(Mutex::new(None)),
        },
        Some(v) => match v {
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => AnyValue {
                value: Arc::new(Mutex::new(Some(StringValue(s)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b) => AnyValue {
                value: Arc::new(Mutex::new(Some(BoolValue(b)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => AnyValue {
                value: Arc::new(Mutex::new(Some(IntValue(i)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d) => AnyValue {
                value: Arc::new(Mutex::new(Some(DoubleValue(d)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(a) => {
                let mut values = vec![];
                for v in a.values.iter() {
                    let conv = convert_value(v.clone());
                    values.push(Arc::new(Mutex::new(Some(conv))));
                }
                AnyValue {
                    value: Arc::new(Mutex::new(Some(ArrayValue(
                        crate::processor::model::ArrayValue {
                            values: Arc::new(Mutex::new(values)),
                        },
                    )))),
                }
            }
            opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(kvl) => {
                let mut key_values = vec![];
                for kv in kvl.values {
                    let mut new_value = None;
                    if kv.value.is_some() {
                        new_value = Some(convert_value(kv.value.unwrap()));
                    }
                    key_values.push(KeyValue {
                        key: Arc::new(Mutex::new(kv.key)),
                        value: Arc::new(Mutex::new(new_value)),
                    })
                }
                AnyValue {
                    value: Arc::new(Mutex::new(Some(
                        crate::processor::model::Value::KvListValue(
                            crate::processor::model::KeyValueList {
                                values: Arc::new(Mutex::new(key_values)),
                            },
                        ),
                    ))),
                }
            }
            opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b) => AnyValue {
                value: Arc::new(Mutex::new(Some(BytesValue(b)))),
            },
        },
    }
}
