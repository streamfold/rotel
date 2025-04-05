use crate::processor::model::Value::{BoolValue, BytesValue, DoubleValue, IntValue, StringValue};
use crate::processor::py::rotel_python_processor_sdk;
use crate::processor::py::PyResourceSpans;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use std::ffi::CString;
use std::mem;
use std::sync::{Arc, Mutex, MutexGuard, Once};
use tracing::{error, event};

#[derive(Debug, Clone)]
pub struct AnyValue {
    pub value: Arc<Mutex<Option<Value>>>,
}

#[derive(Debug, Clone)]
pub enum Value {
    StringValue(String),
    BoolValue(bool),
    IntValue(i64),
    DoubleValue(f64),
    ArrayValue(ArrayValue),
    KvlistValue(KeyValueList),
    BytesValue(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct ArrayValue {
    pub values: Vec<AnyValue>,
}

#[derive(Debug, Clone)]
pub struct KeyValueList {
    pub values: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Arc<Mutex<String>>,
    pub value: Arc<Mutex<Option<AnyValue>>>,
}

#[derive(Debug, Clone)]
pub struct Resource {
    pub attributes: Arc<Mutex<Vec<Arc<Mutex<KeyValue>>>>>,
    pub dropped_attributes_count: Arc<Mutex<u32>>,
}

#[derive(Debug, Clone)]
pub struct ResourceSpans {
    pub resource: Arc<Mutex<Option<Resource>>>,
    pub scope_spans: Arc<Mutex<Vec<Arc<Mutex<ScopeSpans>>>>>,
    pub schema_url: Arc<Mutex<String>>,
}

#[derive(Debug, Clone)]
pub struct ScopeSpans {
    //pub scope: ::core::option::Option<super::super::common::v1::InstrumentationScope>,
    pub spans: Arc<Mutex<Vec<Arc<Mutex<Span>>>>>,
    pub schema_url: Arc<Mutex<String>>,
}

#[derive(Debug, Clone)]
pub struct Span {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub trace_state: String,
    pub parent_span_id: Vec<u8>,
    pub flags: u32,
    pub name: String,
    pub kind: i32,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: Arc<Mutex<Vec<Arc<Mutex<KeyValue>>>>>,
    pub dropped_attributes_count: u32,
    //pub events: ::prost::alloc::vec::Vec<span::Event>,
    pub dropped_events_count: u32,
    //pub links: ::prost::alloc::vec::Vec<span::Link>,
    pub dropped_links_count: u32,
    pub status: Arc<Mutex<Option<Status>>>,
}
#[derive(Debug, Clone)]
pub struct Status {
    pub message: String,
    pub code: i32,
}

static INIT: Once = Once::new();

pub fn initialize(processor: &str) {
    INIT.call_once(|| {
        pyo3::append_to_inittab!(rotel_python_processor_sdk);
        pyo3::prepare_freethreaded_python();
        let res = Python::with_gil(|py| -> PyResult<()> {
            let py_mod = PyModule::from_code(
                py,
                CString::new(processor)?.as_c_str(),
                c_str!("example.py"),
                c_str!("example"),
            )?;
            Ok(())
        });
    });
}

pub trait PythonProcessable {
    fn process(self, processor: &str) -> Self;
}

impl PythonProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {
    fn process(self, processor: &str) -> Self {
        initialize(processor);
        let inner = prepare_resource_spans(self);
        // Build the PyObject
        let spans = PyResourceSpans {
            resource: inner.resource.clone(),
            scope_spans: inner.scope_spans.clone(),
            schema_url: Arc::new(Default::default()),
        };
        let res = Python::with_gil(|py| -> PyResult<()> {
            // let py_mod = PyModule::from_code(
            //     py,
            //     CString::new(processor)?.as_c_str(),
            //     c_str!("example.py"),
            //     c_str!("example"),
            // )?;
            let py_mod = PyModule::import(py, "example")?;
            let result_py_object = py_mod.getattr("process")?.call1((spans,));
            if result_py_object.is_err() {
                let err = result_py_object.unwrap_err();
                return Err(err);
            }
            Ok(())
        });
        if res.is_err() {
            error!("{}", res.err().unwrap().to_string())
        }
        let mut resource_span = opentelemetry_proto::tonic::trace::v1::ResourceSpans {
            resource: None,
            scope_spans: vec![],
            schema_url: "".to_string(),
        };
        let mut resource = inner.resource.lock().unwrap();
        let resource = resource.take();
        if resource.is_some() {
            resource_span.resource = convert_resource(resource.unwrap());
        }
        let scope_spans = inner.scope_spans.lock().unwrap();
        resource_span.scope_spans = convert_scope_spans(scope_spans);
        resource_span
    }
}

impl PythonProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
    fn process(self, processor: &str) -> Self {
        // Noop
        self
    }
}

impl PythonProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    fn process(self, processor: &str) -> Self {
        // Noop
        self
    }
}

fn convert_scope_spans(
    scope_spans: MutexGuard<Vec<Arc<Mutex<ScopeSpans>>>>,
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

fn convert_resource(
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
                    Value::KvlistValue(_) => {}
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

pub fn prepare_resource_spans_vec(
    value: Vec<opentelemetry_proto::tonic::trace::v1::ResourceSpans>,
) -> Vec<ResourceSpans> {
    let mut resource_spans = vec![];
    for rs in value {
        resource_spans.push(prepare_resource_spans(rs))
    }
    resource_spans
}

pub fn prepare_resource_spans(
    rs: opentelemetry_proto::tonic::trace::v1::ResourceSpans,
) -> ResourceSpans {
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
