use crate::model::common::RValue::StringValue;
use crate::model::common::{RAnyValue, RInstrumentationScope};
use crate::model::logs::{RLogRecord, RScopeLogs};
use crate::model::otel_transform::convert_attributes;
use crate::model::resource::RResource;
use crate::py::common::{AnyValue, KeyValue};
use crate::py::{handle_poison_error, AttributesList, InstrumentationScope, Resource};
use pyo3::{pyclass, pymethods, Py, PyErr, PyRef, PyRefMut, PyResult, Python};
use std::sync::{Arc, Mutex};

#[pyclass]
#[derive(Clone)]
pub struct ResourceLogs {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_logs: Arc<Mutex<Vec<Arc<Mutex<RScopeLogs>>>>>,
    pub schema_url: String,
}

#[pymethods]
impl ResourceLogs {
    #[getter]
    fn resource(&self) -> PyResult<Option<Resource>> {
        let v = self.resource.lock().map_err(handle_poison_error)?;
        if v.is_none() {
            return Ok(None);
        }
        let inner = v.clone().unwrap();
        Ok(Some(Resource {
            attributes: inner.attributes.clone(),
            dropped_attributes_count: inner.dropped_attributes_count.clone(),
            entity_refs: inner.entity_refs.clone(),
        }))
    }
    #[setter]
    fn set_resource(&mut self, resource: Resource) -> PyResult<()> {
        let mut inner = self.resource.lock().map_err(handle_poison_error)?;
        *inner = Some(RResource {
            attributes: resource.attributes,
            dropped_attributes_count: resource.dropped_attributes_count,
            entity_refs: resource.entity_refs,
        });
        Ok(())
    }
    #[getter]
    fn scope_logs(&self) -> PyResult<ScopeLogsList> {
        Ok(ScopeLogsList(self.scope_logs.clone()))
    }
    #[setter]
    fn set_scope_logs(&mut self, scope_logs: Vec<ScopeLogs>) -> PyResult<()> {
        let mut inner = self.scope_logs.lock().unwrap();
        inner.clear();
        for sc in scope_logs {
            inner.push(Arc::new(Mutex::new(RScopeLogs {
                scope: sc.scope.clone(),
                log_records: sc.log_records.clone(),
                schema_url: sc.schema_url.clone(),
            })));
        }
        Ok(())
    }
    #[getter]
    fn schema_url(&self) -> PyResult<String> {
        Ok(self.schema_url.clone())
    }
    #[setter]
    fn set_schema_url(&mut self, schema_url: String) -> PyResult<()> {
        self.schema_url = schema_url;
        Ok(())
    }
}

#[pyclass]
pub struct ScopeLogsList(Arc<Mutex<Vec<Arc<Mutex<RScopeLogs>>>>>);

#[pymethods]
impl ScopeLogsList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<ScopeLogsListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = ScopeLogsListIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<ScopeLogs> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => {
                let item = item.lock().unwrap();
                Ok(ScopeLogs {
                    scope: item.scope.clone(),
                    log_records: item.log_records.clone(),
                    schema_url: item.schema_url.clone(),
                })
            }
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &ScopeLogs) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        inner[index] = Arc::new(Mutex::new(RScopeLogs {
            scope: value.scope.clone(),
            log_records: value.log_records.clone(),
            schema_url: value.schema_url.clone(),
        }));
        Ok(())
    }
    fn __delitem__(&self, index: usize) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        inner.remove(index);
        Ok(())
    }
    fn append(&self, item: &ScopeLogs) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(Arc::new(Mutex::new(RScopeLogs {
            scope: item.scope.clone(),
            log_records: item.log_records.clone(),
            schema_url: item.schema_url.clone(),
        })));
        Ok(())
    }
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
}

#[pyclass]
struct ScopeLogsListIter {
    inner: std::vec::IntoIter<Arc<Mutex<RScopeLogs>>>,
}

#[pymethods]
impl ScopeLogsListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<ScopeLogs>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        let inner = inner.lock().unwrap();
        let x = Ok(Some(ScopeLogs {
            scope: inner.scope.clone(),
            log_records: inner.log_records.clone(),
            schema_url: inner.schema_url.clone(),
        }));
        x
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ScopeLogs {
    scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    log_records: Arc<Mutex<Vec<Arc<Mutex<RLogRecord>>>>>,
    schema_url: String,
}

#[pymethods]
impl ScopeLogs {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ScopeLogs {
            scope: Arc::new(Mutex::new(None)),
            log_records: Arc::new(Mutex::new(vec![])),
            schema_url: "".to_string(),
        })
    }
    #[getter]
    fn log_records(&self) -> PyResult<LogRecords> {
        Ok(LogRecords(self.log_records.clone()))
    }
    #[setter]
    fn set_log_records(&mut self, log_records: Vec<LogRecord>) -> PyResult<()> {
        let mut inner = self.log_records.lock().unwrap();
        inner.clear();
        for lr in log_records {
            inner.push(lr.inner);
        }
        Ok(())
    }
    #[getter]
    fn scope(&self) -> PyResult<Option<InstrumentationScope>> {
        {
            let v = self.scope.lock().map_err(handle_poison_error)?;
            if v.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(InstrumentationScope(self.scope.clone())))
    }
    #[setter]
    fn set_scope(&mut self, scope: InstrumentationScope) -> PyResult<()> {
        let mut v = self.scope.lock().map_err(handle_poison_error)?;
        let inner = scope.0.lock().map_err(handle_poison_error)?;
        v.replace(inner.clone().unwrap());
        Ok(())
    }
    #[getter]
    fn schema_url(&self) -> String {
        self.schema_url.clone()
    }
    #[setter]
    fn set_schema_url(&mut self, schema_url: String) -> PyResult<()> {
        self.schema_url = schema_url;
        Ok(())
    }
}

#[pyclass]
pub struct LogRecords(Arc<Mutex<Vec<Arc<Mutex<RLogRecord>>>>>);

#[pymethods]
impl LogRecords {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<LogRecordsIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = LogRecordsIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<LogRecord> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(LogRecord {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &LogRecord) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        inner[index] = value.inner.clone();
        Ok(())
    }

    fn __delitem__(&self, index: usize) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        inner.remove(index);
        Ok(())
    }

    fn append(&self, item: &LogRecord) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(item.inner.clone());
        Ok(())
    }

    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
}

#[pyclass]
struct LogRecordsIter {
    inner: std::vec::IntoIter<Arc<Mutex<RLogRecord>>>,
}

#[pymethods]
impl LogRecordsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<LogRecord>> {
        let lr = slf.inner.next();
        if lr.is_none() {
            return Ok(None);
        }
        let inner = lr.unwrap();
        Ok(Some(LogRecord {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct LogRecord {
    inner: Arc<Mutex<RLogRecord>>,
}

#[pymethods]
impl LogRecord {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(LogRecord {
            inner: Arc::new(Mutex::new(RLogRecord {
                time_unix_nano: 0,
                observed_time_unix_nano: 0,
                severity_number: 0,
                severity_text: "".to_string(),
                body: Arc::new(Mutex::new(Some(RAnyValue {
                    value: Arc::new(Mutex::new(Some(StringValue("".to_string())))),
                }))),
                attributes_raw: vec![],
                attributes_arc: None,
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: vec![],
                span_id: vec![],
                event_name: "".to_string(),
            })),
        })
    }

    #[getter]
    fn time_unix_nano(&self) -> PyResult<u64> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.time_unix_nano)
    }

    #[setter]
    fn set_time_unix_nano(&mut self, new_value: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.time_unix_nano = new_value;
        Ok(())
    }

    #[getter]
    fn observed_time_unix_nano(&self) -> PyResult<u64> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.observed_time_unix_nano)
    }

    #[setter]
    fn set_observed_time_unix_nano(&mut self, new_value: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.observed_time_unix_nano = new_value;
        Ok(())
    }

    #[getter]
    fn severity_number(&self) -> PyResult<i32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.severity_number)
    }

    #[setter]
    fn set_severity_number(&mut self, new_value: i32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.severity_number = new_value;
        Ok(())
    }

    #[getter]
    fn severity_text(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.severity_text.clone())
    }

    #[setter]
    fn set_severity_text(&mut self, new_value: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.severity_text = new_value;
        Ok(())
    }

    #[getter]
    fn body(&self) -> PyResult<AnyValue> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AnyValue {
            inner: v.body.clone(),
        })
    }

    #[setter]
    fn set_body(&mut self, new_value: &AnyValue) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.body = new_value.inner.clone();
        Ok(())
    }

    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let mut binding = self.inner.lock().map_err(handle_poison_error)?;
        if binding.attributes_arc.is_some() {
            let attr_arc = binding.attributes_arc.take().unwrap();
            let attr_arc_copy = attr_arc.clone();
            binding.attributes_arc.replace(attr_arc);
            Ok(AttributesList(attr_arc_copy))
        } else {
            let attrs = convert_attributes(binding.attributes_raw.clone());
            let attrs = Arc::new(Mutex::new(attrs));
            binding.attributes_arc.replace(attrs.clone());
            Ok(AttributesList(attrs))
        }
    }

    #[setter]
    fn set_attributes(&mut self, attrs: Vec<KeyValue>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut new_attrs = Vec::with_capacity(attrs.len());
        for kv in attrs {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error).unwrap();
            new_attrs.push(kv_lock.clone());
        }
        inner
            .attributes_arc
            .replace(Arc::new(Mutex::new(new_attrs)));
        Ok(())
    }

    #[getter]
    fn dropped_attributes_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.dropped_attributes_count)
    }

    #[setter]
    fn set_dropped_attributes_count(&mut self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.dropped_attributes_count = new_value;
        Ok(())
    }

    #[getter]
    fn flags(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.flags)
    }

    #[setter]
    fn set_flags(&mut self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.flags = new_value;
        Ok(())
    }

    #[getter]
    fn trace_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.trace_id.clone())
    }

    #[setter]
    fn set_trace_id(&mut self, new_value: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.trace_id = new_value;
        Ok(())
    }

    #[getter]
    fn span_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.span_id.clone())
    }

    #[setter]
    fn set_span_id(&mut self, new_value: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.span_id = new_value;
        Ok(())
    }
    #[getter]
    fn event_name(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.event_name.clone())
    }

    #[setter]
    fn set_event_name(&mut self, new_value: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.event_name = new_value;
        Ok(())
    }
}
