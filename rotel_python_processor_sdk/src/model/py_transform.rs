use crate::model::common::RValue::*;
use crate::model::common::*;
use crate::model::logs::*;
use crate::model::trace::*;

use crate::model::resource::RResource;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use std::mem;
use std::sync::{Arc, Mutex};

pub fn transform_spans(
    scope_spans: Vec<Arc<Mutex<RScopeSpans>>>,
) -> Vec<opentelemetry_proto::tonic::trace::v1::ScopeSpans> {
    let mut new_scope_spans = vec![];
    for ss in scope_spans.iter() {
        let ss = ss.lock().unwrap();
        let schema = ss.schema_url.clone();
        let scope = convert_scope(ss.scope.clone());
        let ss = ss.spans.lock().unwrap();
        let mut spans = vec![];
        for span in ss.iter() {
            let mut guard = span.lock().unwrap();
            let moved_data = mem::replace(
                &mut *guard,
                RSpan {
                    trace_id: vec![],
                    span_id: vec![],
                    trace_state: "".to_string(),
                    parent_span_id: vec![],
                    flags: 0,
                    name: "".to_string(),
                    kind: 0,
                    events_arc: None,
                    events_raw: vec![],
                    links_arc: None,
                    links_raw: vec![],
                    start_time_unix_nano: 0,
                    end_time_unix_nano: 0,
                    attributes_arc: None,
                    attributes_raw: vec![],
                    dropped_attributes_count: 0,
                    dropped_events_count: 0,
                    dropped_links_count: 0,
                    status: Arc::new(Mutex::new(None)),
                },
            );
            let status = Arc::into_inner(moved_data.status).unwrap();
            let status = status.into_inner().unwrap();
            spans.push(opentelemetry_proto::tonic::trace::v1::Span {
                trace_id: moved_data.trace_id,
                span_id: moved_data.span_id,
                trace_state: moved_data.trace_state,
                parent_span_id: moved_data.parent_span_id,
                flags: moved_data.flags,
                name: moved_data.name,
                kind: moved_data.kind,
                start_time_unix_nano: moved_data.start_time_unix_nano,
                end_time_unix_nano: moved_data.end_time_unix_nano,
                attributes: convert_attributes(
                    moved_data.attributes_raw,
                    moved_data.attributes_arc,
                ),
                dropped_attributes_count: moved_data.dropped_attributes_count,
                events: convert_events(moved_data.events_raw, moved_data.events_arc),
                links: convert_links(moved_data.links_raw, moved_data.links_arc),
                dropped_events_count: moved_data.dropped_events_count,
                dropped_links_count: moved_data.dropped_links_count,
                status: status.map(|s| opentelemetry_proto::tonic::trace::v1::Status {
                    message: s.message,
                    code: s.code,
                }),
            })
        }
        new_scope_spans.push(opentelemetry_proto::tonic::trace::v1::ScopeSpans {
            scope,
            spans,
            schema_url: schema,
        })
    }
    new_scope_spans
}

pub fn transform_logs(
    scope_logs: Vec<Arc<Mutex<RScopeLogs>>>,
) -> Vec<opentelemetry_proto::tonic::logs::v1::ScopeLogs> {
    let mut new_scope_logs = vec![];
    for sl in scope_logs.iter() {
        let sl = sl.lock().unwrap();
        let schema = sl.schema_url.clone();
        let scope = convert_scope(sl.scope.clone());
        let sl_records = sl.log_records.lock().unwrap();
        let mut log_records = vec![];
        for lr in sl_records.iter() {
            let mut guard = lr.lock().unwrap();
            let moved_data = mem::replace(
                &mut *guard,
                RLogRecord {
                    time_unix_nano: 0,
                    observed_time_unix_nano: 0,
                    severity_number: 0,
                    severity_text: "".to_string(),
                    body: Arc::new(Mutex::new(Some(RAnyValue {
                        value: Arc::new(Mutex::new(None)),
                    }))),
                    attributes_arc: None,
                    attributes_raw: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: "".to_string(),
                },
            );
            let body = Arc::into_inner(moved_data.body)
                .unwrap()
                .into_inner()
                .unwrap();
            let body: Option<AnyValue> = body.map_or(None, |b| Some(convert_value(b)));
            log_records.push(opentelemetry_proto::tonic::logs::v1::LogRecord {
                body,
                time_unix_nano: moved_data.time_unix_nano,
                observed_time_unix_nano: moved_data.observed_time_unix_nano,
                severity_number: moved_data.severity_number,
                severity_text: moved_data.severity_text,
                attributes: convert_attributes(
                    moved_data.attributes_raw,
                    moved_data.attributes_arc,
                ),
                dropped_attributes_count: moved_data.dropped_attributes_count,
                flags: moved_data.flags,
                trace_id: moved_data.trace_id,
                span_id: moved_data.span_id,
                event_name: moved_data.event_name,
            });
        }
        new_scope_logs.push(opentelemetry_proto::tonic::logs::v1::ScopeLogs {
            scope,
            log_records,
            schema_url: schema,
        });
    }
    new_scope_logs
}

fn convert_events(
    events_raw: Vec<opentelemetry_proto::tonic::trace::v1::span::Event>,
    events_arc: Option<Arc<Mutex<Vec<Arc<Mutex<REvent>>>>>>,
) -> Vec<opentelemetry_proto::tonic::trace::v1::span::Event> {
    if events_arc.is_none() {
        return events_raw;
    }
    let events = Arc::into_inner(events_arc.unwrap()).unwrap();
    let mut events = events.into_inner().unwrap();
    events
        .drain(..) // Creates an iterator that removes all elements
        .map(|e| {
            let e = Arc::into_inner(e).unwrap();
            let e = e.into_inner().unwrap();
            opentelemetry_proto::tonic::trace::v1::span::Event {
                time_unix_nano: e.time_unix_nano,
                name: e.name.clone(),
                attributes: convert_attributes(vec![], Some(e.attributes)),
                dropped_attributes_count: e.dropped_attributes_count,
            }
        })
        .collect()
}

fn convert_links(
    links_raw: Vec<opentelemetry_proto::tonic::trace::v1::span::Link>,
    links_arc: Option<Arc<Mutex<Vec<Arc<Mutex<RLink>>>>>>,
) -> Vec<opentelemetry_proto::tonic::trace::v1::span::Link> {
    if links_arc.is_none() {
        return links_raw;
    }
    let links = Arc::into_inner(links_arc.unwrap()).unwrap();
    let mut links = links.into_inner().unwrap();
    links
        .drain(..) // Creates an iterator that removes all elements
        .map(|l| {
            let l = Arc::into_inner(l).unwrap();
            let l = l.into_inner().unwrap();
            opentelemetry_proto::tonic::trace::v1::span::Link {
                trace_id: l.trace_id,
                span_id: l.span_id,
                attributes: convert_attributes(vec![], Some(l.attributes)),
                dropped_attributes_count: l.dropped_attributes_count,
                trace_state: l.trace_state,
                flags: l.flags,
            }
        })
        .collect()
}

fn convert_scope(
    scope: Arc<Mutex<Option<RInstrumentationScope>>>,
) -> Option<opentelemetry_proto::tonic::common::v1::InstrumentationScope> {
    let guard = scope.lock().unwrap();
    if guard.is_none() {
        return None;
    }
    let scope = guard.clone().unwrap();
    let attrs = convert_attributes(scope.attributes_raw, scope.attributes_arc);
    Some(
        opentelemetry_proto::tonic::common::v1::InstrumentationScope {
            name: scope.name,
            version: scope.version,
            attributes: attrs,
            dropped_attributes_count: scope.dropped_attributes_count,
        },
    )
}

fn convert_attributes(
    attr_raw: Vec<KeyValue>,
    attrs_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
) -> Vec<KeyValue> {
    if attrs_arc.is_none() {
        return attr_raw;
    }
    let attrs = attrs_arc.unwrap();
    let attrs = attrs.lock().unwrap();
    let mut new_attrs = vec![];
    for attr in attrs.iter() {
        let key = attr.key.lock().unwrap();
        let key = key.to_string();
        let mut any_value = attr.value.lock().unwrap();
        let any_value = any_value.take();
        match any_value {
            None => new_attrs.push(KeyValue { key, value: None }),
            Some(v) => {
                let converted = convert_value(v);
                new_attrs.push(KeyValue {
                    key,
                    value: Some(converted),
                })
            }
        }
    }
    new_attrs
}

pub fn transform_resource(
    resource: RResource,
) -> Option<opentelemetry_proto::tonic::resource::v1::Resource> {
    let attributes = Arc::into_inner(resource.attributes).unwrap();
    let attributes = attributes.into_inner().unwrap();
    let dropped_attributes_count = Arc::into_inner(resource.dropped_attributes_count).unwrap();
    let dropped_attributes_count = dropped_attributes_count.into_inner().unwrap();
    let mut new_attrs = vec![];
    for attr in attributes.iter() {
        let kv = attr.lock().unwrap();
        let key = kv.key.lock().unwrap();
        let key = key.to_string();
        let mut any_value = kv.value.lock().unwrap();
        let any_value = any_value.take();
        match any_value {
            None => new_attrs.push(KeyValue { key, value: None }),
            Some(v) => {
                let converted = convert_value(v);
                new_attrs.push(KeyValue {
                    key,
                    value: Some(converted),
                })
            }
        }
    }
    Some(opentelemetry_proto::tonic::resource::v1::Resource {
        attributes: new_attrs,
        dropped_attributes_count,
    })
}

pub fn convert_value(v: RAnyValue) -> opentelemetry_proto::tonic::common::v1::AnyValue {
    let inner_value = Arc::into_inner(v.value).unwrap();
    let inner_value = inner_value.into_inner().unwrap();
    if inner_value.is_none() {
        return opentelemetry_proto::tonic::common::v1::AnyValue { value: None };
    }
    match inner_value.unwrap() {
        StringValue(s) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)),
        },
        BoolValue(b) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)),
        },
        IntValue(i) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)),
        },
        DoubleValue(d) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)),
        },
        RVArrayValue(a) => {
            let mut values = vec![];
            let inner_values = Arc::into_inner(a.values).unwrap();
            let mut inner_values = inner_values.into_inner().unwrap();
            while !inner_values.is_empty() {
                let inner_v = inner_values.pop().expect("inner value should not be none");
                let inner_v = Arc::into_inner(inner_v).unwrap();
                let inner_v = inner_v.into_inner().unwrap();
                if inner_v.is_none() {
                    values.push(opentelemetry_proto::tonic::common::v1::AnyValue { value: None })
                } else {
                    let converted = convert_value(inner_v.unwrap());
                    values.push(converted);
                }
            }
            opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(
                        opentelemetry_proto::tonic::common::v1::ArrayValue { values },
                    ),
                ),
            }
        }
        KvListValue(kvl) => {
            let mut values = vec![];
            let inner_values = Arc::into_inner(kvl.values).unwrap();
            let inner_values = inner_values.into_inner().unwrap();
            // TODO: We might need to remove these from the vec?
            for kv in inner_values {
                let key = Arc::into_inner(kv.key).unwrap();
                let key = key.into_inner().unwrap();
                let value = Arc::into_inner(kv.value).unwrap();
                let value = value.into_inner().unwrap();
                let mut new_value = None;
                if value.is_some() {
                    new_value = Some(convert_value(value.unwrap()));
                }
                values.push(KeyValue {
                    key,
                    value: new_value,
                });
            }
            opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(
                        opentelemetry_proto::tonic::common::v1::KeyValueList { values },
                    ),
                ),
            }
        }
        BytesValue(b) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b)),
        },
    }
}
