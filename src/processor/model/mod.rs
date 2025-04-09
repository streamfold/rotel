pub mod otel_transform;
mod py_transform;

use crate::processor::py::PyResourceSpans;
use pyo3::prelude::*;
use std::ffi::CString;
use std::sync::{Arc, Mutex};
use tower::BoxError;
use tracing::error;

#[derive(Debug, Clone)]
pub struct AnyValue {
    pub value: Arc<Mutex<Option<Value>>>,
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum Value {
    StringValue(String),
    BoolValue(bool),
    IntValue(i64),
    DoubleValue(f64),
    ArrayValue(ArrayValue),
    KvListValue(KeyValueList),
    BytesValue(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct ArrayValue {
    pub values: Arc<Mutex<Vec<Arc<Mutex<Option<AnyValue>>>>>>,
}

#[allow(deprecated)]
impl ArrayValue {
    pub(crate) fn convert_to_py(&self, py: Python) -> PyResult<PyObject> {
        Ok(crate::processor::py::PyArrayValue(self.values.clone()).into_py(py))
    }
}

#[derive(Debug, Clone)]
pub struct KeyValueList {
    pub values: Arc<Mutex<Vec<KeyValue>>>,
}

#[allow(deprecated)]
impl KeyValueList {
    pub(crate) fn convert_to_py(&self, py: Python) -> PyResult<PyObject> {
        Ok(crate::processor::py::PyKeyValueList(self.values.clone()).into_py(py))
    }
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
    pub _schema_url: Arc<Mutex<String>>,
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

pub fn register_processor(code: String, script: String, module: String) -> Result<(), BoxError> {
    let res = Python::with_gil(|py| -> PyResult<()> {
        PyModule::from_code(
            py,
            CString::new(code)?.as_c_str(),
            CString::new(script)?.as_c_str(),
            CString::new(module)?.as_c_str(),
        )?;
        Ok(())
    });
    match res {
        Ok(_) => Ok(()),
        Err(e) => Err(BoxError::from(e.to_string())),
    }
}

pub trait PythonProcessable {
    fn process(self, processor: &str) -> Self;
}

impl PythonProcessable for opentelemetry_proto::tonic::trace::v1::ResourceSpans {
    fn process(self, processor: &str) -> Self {
        let inner = otel_transform::transform(self);
        // Build the PyObject
        let spans = PyResourceSpans {
            resource: inner.resource.clone(),
            scope_spans: inner.scope_spans.clone(),
            // TODO actually copy schema_url
            schema_url: Arc::new(Default::default()),
        };
        let res = Python::with_gil(|py| -> PyResult<()> {
            let py_mod = PyModule::import(py, processor)?;
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
        resource_span.scope_spans = py_transform::transform_spans(scope_spans);
        resource_span
    }
}

impl PythonProcessable for opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}

impl PythonProcessable for opentelemetry_proto::tonic::logs::v1::ResourceLogs {
    fn process(self, _processor: &str) -> Self {
        // Noop
        self
    }
}
