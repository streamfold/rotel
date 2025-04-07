pub mod otel_transform;
mod py_transform;

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
        let inner = otel_transform::transform(self);
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
            resource_span.resource = py_transform::transform_resource(resource.unwrap());
        }
        let scope_spans = Arc::into_inner(inner.scope_spans)
            .unwrap()
            .into_inner()
            .unwrap();
        //let scope_spans = inner.scope_spans.lock().unwrap();
        resource_span.scope_spans = py_transform::transform_spans(scope_spans);
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
