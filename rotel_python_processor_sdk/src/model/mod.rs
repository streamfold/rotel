pub mod common;
pub mod logs;
pub mod otel_transform;
pub mod py_transform;
pub mod resource;
pub mod trace;

use crate::py::logs::*;
use crate::py::rotel_sdk;
use crate::py::trace::*;
use pyo3::prelude::*;
use std::ffi::CString;
use std::sync::{Arc, Once};
use tower::BoxError;
use tracing::error;

static PROCESSOR_INIT: Once = Once::new();

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
