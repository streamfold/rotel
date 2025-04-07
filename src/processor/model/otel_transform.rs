use crate::processor::model::Value::{BoolValue, BytesValue, DoubleValue, IntValue, StringValue};
use crate::processor::model::{ResourceSpans, Span};
use std::sync::{Arc, Mutex};

// pub fn prepare_resource_spans_vec(
//     value: Vec<opentelemetry_proto::tonic::trace::v1::ResourceSpans>,
// ) -> Vec<ResourceSpans> {
//     let mut resource_spans = vec![];
//     for rs in value {
//         resource_spans.push(prepare_resource_spans(rs))
//     }
//     resource_spans
// }

pub fn transform(rs: opentelemetry_proto::tonic::trace::v1::ResourceSpans) -> ResourceSpans {
    let mut resource_span = ResourceSpans {
        resource: Arc::new(Mutex::new(None)),
        scope_spans: Arc::new(Mutex::new(vec![])),
        schema_url: Arc::new(Mutex::new("".to_string())),
    };
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
    resource_span.scope_spans = Arc::new(Mutex::new(scope_spans));
    resource_span
}

fn build_rotel_sdk_resource(
    resource: opentelemetry_proto::tonic::resource::v1::Resource,
) -> Vec<Arc<Mutex<crate::processor::model::KeyValue>>> {
    let mut kvs = vec![];
    for a in resource.attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => {}
            Some(v) => {
                let v = v;
                match v.value {
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
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            s,
                        ) => {
                            let kv = crate::processor::model::KeyValue {
                                key,
                                value: Arc::new(Mutex::new(Some(
                                    crate::processor::model::AnyValue {
                                        value: Arc::new(Mutex::new(Some(StringValue(s)))),
                                    },
                                ))),
                            };
                            kvs.push(Arc::new(Mutex::new(kv)));
                        }
                        opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b) => {
                            let kv = crate::processor::model::KeyValue {
                                key,
                                value: Arc::new(Mutex::new(Some(
                                    crate::processor::model::AnyValue {
                                        value: Arc::new(Mutex::new(Some(BoolValue(b)))),
                                    },
                                ))),
                            };
                            kvs.push(Arc::new(Mutex::new(kv)));
                        }
                        opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => {
                            let kv = crate::processor::model::KeyValue {
                                key,
                                value: Arc::new(Mutex::new(Some(
                                    crate::processor::model::AnyValue {
                                        value: Arc::new(Mutex::new(Some(IntValue(i)))),
                                    },
                                ))),
                            };
                            kvs.push(Arc::new(Mutex::new(kv)));
                        }
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(
                            d,
                        ) => {
                            let kv = crate::processor::model::KeyValue {
                                key,
                                value: Arc::new(Mutex::new(Some(
                                    crate::processor::model::AnyValue {
                                        value: Arc::new(Mutex::new(Some(DoubleValue(d)))),
                                    },
                                ))),
                            };
                            kvs.push(Arc::new(Mutex::new(kv)));
                        }
                        opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(_) => {
                        }
                        opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(
                            _,
                        ) => {}
                        opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b) => {
                            let kv = crate::processor::model::KeyValue {
                                key,
                                value: Arc::new(Mutex::new(Some(
                                    crate::processor::model::AnyValue {
                                        value: Arc::new(Mutex::new(Some(BytesValue(b)))),
                                    },
                                ))),
                            };
                            kvs.push(Arc::new(Mutex::new(kv)));
                        }
                    },
                }
            }
        }
    }
    kvs
}
