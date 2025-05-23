pub mod otel_transform;
pub mod py_transform;

use crate::py;
use crate::py::{rotel_sdk, ResourceLogs, ResourceSpans};
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use pyo3::prelude::*;
use std::ffi::CString;
use std::sync::{Arc, Mutex, Once};
use tower::BoxError;
use tracing::error;

static PROCESSOR_INIT: Once = Once::new();

#[derive(Debug, Clone)]
pub struct RAnyValue {
    pub value: Arc<Mutex<Option<RValue>>>,
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum RValue {
    StringValue(String),
    BoolValue(bool),
    IntValue(i64),
    DoubleValue(f64),
    RVArrayValue(RArrayValue),
    KvListValue(RKeyValueList),
    BytesValue(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct RArrayValue {
    pub values: Arc<Mutex<Vec<Arc<Mutex<Option<RAnyValue>>>>>>,
}

#[allow(deprecated)]
impl RArrayValue {
    pub(crate) fn convert_to_py(&self, py: Python) -> PyResult<PyObject> {
        Ok(py::ArrayValue(self.values.clone()).into_py(py))
    }
}

#[derive(Debug, Clone)]
pub struct RKeyValueList {
    pub values: Arc<Mutex<Vec<RKeyValue>>>,
}

#[allow(deprecated)]
impl RKeyValueList {
    pub(crate) fn convert_to_py(&self, py: Python) -> PyResult<PyObject> {
        Ok(py::KeyValueList(self.values.clone()).into_py(py))
    }
}

#[derive(Debug, Clone)]
pub struct RKeyValue {
    pub key: Arc<Mutex<String>>,
    pub value: Arc<Mutex<Option<RAnyValue>>>,
}

#[derive(Debug, Clone)]
pub struct RResource {
    pub attributes: Arc<Mutex<Vec<Arc<Mutex<RKeyValue>>>>>,
    pub dropped_attributes_count: Arc<Mutex<u32>>,
}

#[derive(Debug, Clone)]
pub struct RResourceSpans {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_spans: Arc<Mutex<Vec<Arc<Mutex<RScopeSpans>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct RScopeSpans {
    pub scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    pub spans: Arc<Mutex<Vec<Arc<Mutex<RSpan>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone, Default)]
pub struct RInstrumentationScope {
    pub name: String,
    pub version: String,
    pub attributes_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
    pub attributes_raw: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone)]
pub struct RSpan {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub trace_state: String,
    pub parent_span_id: Vec<u8>,
    pub flags: u32,
    pub name: String,
    pub kind: i32,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
    pub attributes_raw: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub events_raw: Vec<Event>,
    pub events_arc: Option<Arc<Mutex<Vec<Arc<Mutex<REvent>>>>>>,
    pub dropped_events_count: u32,
    pub links_arc: Option<Arc<Mutex<Vec<Arc<Mutex<RLink>>>>>>,
    pub links_raw: Vec<Link>,
    pub dropped_links_count: u32,
    pub status: Arc<Mutex<Option<RStatus>>>,
}

#[derive(Debug, Clone)]
pub struct REvent {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct RStatus {
    pub message: String,
    pub code: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum RStatusCode {
    /// The default status.
    Unset = 0,
    /// The Span has been validated by an Application developer or Operator to
    /// have completed successfully.
    Ok = 1,
    /// The Span contains an error.
    Error = 2,
}

#[derive(Debug, Clone)]
pub struct RLink {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub trace_state: String,
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
}

// Structures for Logs
#[derive(Debug, Clone)]
pub struct RResourceLogs {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_logs: Arc<Mutex<Vec<Arc<Mutex<RScopeLogs>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct RScopeLogs {
    pub scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    pub log_records: Arc<Mutex<Vec<Arc<Mutex<RLogRecord>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct RLogRecord {
    pub time_unix_nano: u64,
    pub observed_time_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: RAnyValue,
    pub attributes_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
    pub attributes_raw: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub event_name: String,
}

pub fn register_processor(code: String, script: String, module: String) -> Result<(), BoxError> {
    PROCESSOR_INIT.call_once(|| {
        pyo3::append_to_inittab!(rotel_sdk);
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
        let inner = otel_transform::transform_resource_spans(self);
        // Build the PyObject
        let spans = ResourceSpans {
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
    fn process(self, processor: &str) -> Self {
        let inner = otel_transform::transform_resource_logs(self);
        // Build the PyObject
        let spans = ResourceLogs {
            resource: inner.resource.clone(),
            scope_logs: inner.scope_logs.clone(),
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
        let mut resource_logs = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
            resource: None,
            scope_logs: vec![],
            schema_url: inner.schema_url,
        };
        let mut resource = inner.resource.lock().unwrap();
        let resource = resource.take();
        if resource.is_some() {
            resource_logs.resource = py_transform::transform_resource(resource.unwrap());
        }
        let scope_logs = Arc::into_inner(inner.scope_logs)
            .unwrap()
            .into_inner()
            .unwrap();
        resource_logs.scope_logs = py_transform::transform_logs(scope_logs);
        resource_logs
    }
}
