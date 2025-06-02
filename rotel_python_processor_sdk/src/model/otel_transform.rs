use crate::model::common::RValue::{
    BoolValue, BytesValue, DoubleValue, IntValue, RVArrayValue, StringValue,
};
use crate::model::common::*;
use crate::model::logs::*;
use crate::model::trace::*;
use std::sync::{Arc, Mutex};

pub fn transform_resource_spans(
    rs: opentelemetry_proto::tonic::trace::v1::ResourceSpans,
) -> RResourceSpans {
    let mut resource_span = RResourceSpans {
        resource: Arc::new(Mutex::new(None)),
        scope_spans: Arc::new(Mutex::new(vec![])),
        schema_url: rs.schema_url,
    };
    if rs.resource.is_some() {
        let resource = rs.resource.unwrap();
        let dropped_attributes_count = resource.dropped_attributes_count;
        let kvs = build_rotel_sdk_resource(resource);
        let res = Arc::new(Mutex::new(Some(crate::model::RResource {
            attributes: Arc::new(Mutex::new(kvs.to_owned())),
            dropped_attributes_count: Arc::new(Mutex::new(dropped_attributes_count)),
        })));
        resource_span.resource = res.clone()
    }
    let mut scope_spans = vec![];
    for ss in rs.scope_spans {
        let mut scope = None;
        if ss.scope.is_some() {
            let s = ss.scope.unwrap();
            scope = Some(RInstrumentationScope {
                name: s.name,
                version: s.version,
                attributes_raw: s.attributes,
                attributes_arc: None,
                dropped_attributes_count: s.dropped_attributes_count,
            })
        }
        let scope_span = RScopeSpans {
            scope: Arc::new(Mutex::new(scope)),
            spans: Arc::new(Mutex::new(vec![])),
            schema_url: ss.schema_url,
        };
        for s in ss.spans {
            let span = RSpan {
                trace_id: s.trace_id,
                span_id: s.span_id,
                trace_state: s.trace_state,
                parent_span_id: s.parent_span_id,
                flags: s.flags,
                name: s.name,
                kind: s.kind,
                start_time_unix_nano: s.start_time_unix_nano,
                end_time_unix_nano: s.end_time_unix_nano,
                events_raw: s.events,
                events_arc: None,
                links_raw: s.links,
                links_arc: None,
                attributes_arc: None,
                attributes_raw: s.attributes,
                dropped_attributes_count: s.dropped_attributes_count,
                dropped_events_count: s.dropped_events_count,
                dropped_links_count: s.dropped_links_count,
                status: Arc::new(Mutex::new(s.status.map(|status| RStatus {
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

pub fn transform_resource_logs(
    rl: opentelemetry_proto::tonic::logs::v1::ResourceLogs,
) -> RResourceLogs {
    let mut resource_logs = RResourceLogs {
        resource: Arc::new(Mutex::new(None)),
        scope_logs: Arc::new(Mutex::new(vec![])),
        schema_url: rl.schema_url,
    };
    if rl.resource.is_some() {
        let resource = rl.resource.unwrap();
        let dropped_attributes_count = resource.dropped_attributes_count;
        let kvs = build_rotel_sdk_resource(resource);
        let res = Arc::new(Mutex::new(Some(crate::model::RResource {
            attributes: Arc::new(Mutex::new(kvs.to_owned())),
            dropped_attributes_count: Arc::new(Mutex::new(dropped_attributes_count)),
        })));
        resource_logs.resource = res.clone();
    }

    let mut scope_logs_vec = vec![];
    for sl in rl.scope_logs {
        let mut scope = None;
        if sl.scope.is_some() {
            let s = sl.scope.unwrap();
            scope = Some(RInstrumentationScope {
                name: s.name,
                version: s.version,
                attributes_raw: s.attributes,
                attributes_arc: None,
                dropped_attributes_count: s.dropped_attributes_count,
            });
        }

        let mut log_records_vec = vec![];
        for lr in sl.log_records {
            log_records_vec.push(Arc::new(Mutex::new(transform_log_record(lr))));
        }

        let scope_log = RScopeLogs {
            scope: Arc::new(Mutex::new(scope)),
            log_records: Arc::new(Mutex::new(log_records_vec)),
            schema_url: sl.schema_url,
        };
        scope_logs_vec.push(Arc::new(Mutex::new(scope_log)));
    }
    resource_logs.scope_logs = Arc::new(Mutex::new(scope_logs_vec));
    resource_logs
}

fn transform_log_record(lr: opentelemetry_proto::tonic::logs::v1::LogRecord) -> RLogRecord {
    RLogRecord {
        time_unix_nano: lr.time_unix_nano,
        observed_time_unix_nano: lr.observed_time_unix_nano,
        severity_number: lr.severity_number,
        severity_text: lr.severity_text,
        body: lr.body.map_or(
            RAnyValue {
                value: Arc::new(Mutex::new(None)),
            },
            convert_value,
        ),
        attributes_arc: None,
        attributes_raw: lr.attributes,
        dropped_attributes_count: lr.dropped_attributes_count,
        flags: lr.flags,
        trace_id: lr.trace_id,
        span_id: lr.span_id,
        event_name: lr.event_name,
    }
}

pub fn convert_attributes(
    attributes: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
) -> Vec<RKeyValue> {
    let mut kvs = vec![];
    for a in attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => kvs.push(RKeyValue {
                key,
                value: Arc::new(Mutex::new(None)),
            }),
            Some(v) => {
                let converted = convert_value(v);
                kvs.push(RKeyValue {
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
) -> Vec<Arc<Mutex<RKeyValue>>> {
    let mut kvs = vec![];
    for a in resource.attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => kvs.push(Arc::new(Mutex::new(RKeyValue {
                key,
                value: Arc::new(Mutex::new(None)),
            }))),
            Some(v) => {
                let converted = convert_value(v);
                kvs.push(Arc::new(Mutex::new(RKeyValue {
                    key,
                    value: Arc::new(Mutex::new(Some(converted))),
                })))
            }
        }
    }
    kvs
}

pub fn convert_value(v: opentelemetry_proto::tonic::common::v1::AnyValue) -> RAnyValue {
    match v.value {
        None => RAnyValue {
            value: Arc::new(Mutex::new(None)),
        },
        Some(v) => match v {
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => RAnyValue {
                value: Arc::new(Mutex::new(Some(StringValue(s)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b) => RAnyValue {
                value: Arc::new(Mutex::new(Some(BoolValue(b)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => RAnyValue {
                value: Arc::new(Mutex::new(Some(IntValue(i)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d) => RAnyValue {
                value: Arc::new(Mutex::new(Some(DoubleValue(d)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(a) => {
                let mut values = vec![];
                for v in a.values.iter() {
                    let conv = convert_value(v.clone());
                    values.push(Arc::new(Mutex::new(Some(conv))));
                }
                RAnyValue {
                    value: Arc::new(Mutex::new(Some(RVArrayValue(RArrayValue {
                        values: Arc::new(Mutex::new(values)),
                    })))),
                }
            }
            opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(kvl) => {
                let mut key_values = vec![];
                for kv in kvl.values {
                    let mut new_value = None;
                    if kv.value.is_some() {
                        new_value = Some(convert_value(kv.value.unwrap()));
                    }
                    key_values.push(RKeyValue {
                        key: Arc::new(Mutex::new(kv.key)),
                        value: Arc::new(Mutex::new(new_value)),
                    })
                }
                RAnyValue {
                    value: Arc::new(Mutex::new(Some(RValue::KvListValue(RKeyValueList {
                        values: Arc::new(Mutex::new(key_values)),
                    })))),
                }
            }
            opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b) => RAnyValue {
                value: Arc::new(Mutex::new(Some(BytesValue(b)))),
            },
        },
    }
}
