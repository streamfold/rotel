pub mod otel_transform;
pub mod py_transform;

use crate::py::{rotel_python_processor_sdk, PyResourceSpans};
use pyo3::prelude::*;
use std::ffi::CString;
use std::sync::{Arc, Mutex, Once};
use tower::BoxError;
use tracing::error;

static PROCESSOR_INIT: Once = Once::new();

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
        Ok(crate::py::PyArrayValue(self.values.clone()).into_py(py))
    }
}

#[derive(Debug, Clone)]
pub struct KeyValueList {
    pub values: Arc<Mutex<Vec<KeyValue>>>,
}

#[allow(deprecated)]
impl KeyValueList {
    pub(crate) fn convert_to_py(&self, py: Python) -> PyResult<PyObject> {
        Ok(crate::py::PyKeyValueList(self.values.clone()).into_py(py))
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
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone)]
pub struct ResourceSpans {
    pub resource: Arc<Mutex<Option<Resource>>>,
    pub scope_spans: Arc<Mutex<Vec<Arc<Mutex<ScopeSpans>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct ScopeSpans {
    pub scope: Arc<Mutex<Option<InstrumentationScope>>>,
    pub spans: Arc<Mutex<Vec<Arc<Mutex<Span>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone, Default)]
pub struct InstrumentationScope {
    pub name: String,
    pub version: String,
    pub attributes: Arc<Mutex<Vec<KeyValue>>>,
    pub dropped_attributes_count: u32,
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
    pub attributes: Arc<Mutex<Vec<KeyValue>>>,
    pub dropped_attributes_count: u32,
    pub events: Arc<Mutex<Vec<Arc<Mutex<Event>>>>>,
    pub dropped_events_count: u32,
    pub links: Arc<Mutex<Vec<Arc<Mutex<Link>>>>>,
    pub dropped_links_count: u32,
    pub status: Arc<Mutex<Option<Status>>>,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Arc<Mutex<Vec<KeyValue>>>,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Status {
    pub message: String,
    pub code: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum StatusCode {
    /// The default status.
    Unset = 0,
    /// The Span has been validated by an Application developer or Operator to
    /// have completed successfully.
    Ok = 1,
    /// The Span contains an error.
    Error = 2,
}

#[derive(Debug, Clone)]
pub struct Link {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub trace_state: String,
    pub attributes: Arc<Mutex<Vec<KeyValue>>>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
}

pub fn register_processor(code: String, script: String, module: String) -> Result<(), BoxError> {
    PROCESSOR_INIT.call_once(|| {
        pyo3::append_to_inittab!(rotel_python_processor_sdk);
        pyo3::prepare_freethreaded_python();
    });

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
            schema_url: inner.schema_url.clone(),
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
            schema_url: inner.schema_url,
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
