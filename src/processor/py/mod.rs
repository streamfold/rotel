use crate::processor::model::Value::{ArrayValue, BoolValue, DoubleValue, IntValue, StringValue};
use crate::processor::model::{AnyValue, KeyValue, Resource, ScopeSpans, Span};
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

// Wrapper for AnyValue that can be exposed to Python
#[pyclass]
struct PyAnyValue {
    inner: Arc<Mutex<Option<AnyValue>>>,
}

#[pymethods]
impl PyAnyValue {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(PyAnyValue {
            inner: Arc::new(Mutex::new(Some(AnyValue {
                value: Arc::new(Mutex::new(Some(StringValue("".to_string())))),
            }))),
        })
    }
    #[getter]
    #[allow(deprecated)]
    fn value<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let binding = v.clone().unwrap().value.clone();
        let bind_lock = binding.lock();
        let x = match bind_lock.unwrap().clone() {
            Some(StringValue(s)) => Ok(s.into_py(py)),
            Some(BoolValue(b)) => Ok(b.into_py(py)),
            Some(IntValue(i)) => Ok(i.into_py(py)),
            Some(DoubleValue(d)) => Ok(d.into_py(py)),
            Some(ArrayValue(a)) => Ok(a.convert_to_py(py)?),
            None => Ok(py.None()),
            // TODO add remaining types
            _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Unsupported value type".to_string(),
            )),
        };
        x // to avoid dropping
    }
    #[setter]
    fn set_string_value(&mut self, new_value: &str) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;

        // TODO, can we unwrap here or need a None check?
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(StringValue(new_value.to_string()));
        Ok(())
    }
    #[setter]
    fn set_bool_value(&mut self, new_value: bool) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(BoolValue(new_value));
        Ok(())
    }
    #[setter]
    fn set_int_value(&mut self, new_value: i64) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(IntValue(new_value));
        Ok(())
    }
    #[setter]
    fn set_double_value(&mut self, new_value: f64) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(DoubleValue(new_value));
        Ok(())
    }
    #[setter]
    fn set_array_value(&mut self, new_value: PyArrayValue) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clone().unwrap().value.lock().unwrap().replace(ArrayValue(
            crate::processor::model::ArrayValue {
                values: new_value.0.clone(),
            },
        ));
        Ok(())
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyArrayValue(pub Arc<Mutex<Vec<Arc<Mutex<Option<AnyValue>>>>>>);

// pub struct PyArrayValue {
//     pub inner: Arc<Mutex<Vec<Arc<Mutex<Option<AnyValue>>>>>>,
// }

#[pymethods]
impl PyArrayValue {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(PyArrayValue(Arc::new(Mutex::new(vec![]))))
    }
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<PyArrayValueIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = PyArrayValueIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<PyAnyValue> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(PyAnyValue {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn append(&self, item: &PyAnyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        k.push(item.inner.clone());
        Ok(())
    }
}

#[pyclass]
struct PyArrayValueIter {
    inner: std::vec::IntoIter<Arc<Mutex<Option<AnyValue>>>>,
}

#[pymethods]
impl PyArrayValueIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyAnyValue>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(PyAnyValue {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Clone)]
struct PyKeyValue {
    inner: Arc<Mutex<KeyValue>>,
}

#[pymethods]
impl PyKeyValue {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue {
                key: Arc::new(Mutex::new("".to_string())),
                value: Arc::new(Mutex::new(None)),
            })),
        })
    }
    // Helper methods creating new inner value types
    #[staticmethod]
    fn new_string_value(key: &str, value: &str) -> PyResult<PyKeyValue> {
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = AnyValue {
            value: Arc::new(Mutex::new(Some(StringValue(value.to_string())))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_bool_value(key: &str, py: Python, value: PyObject) -> PyResult<PyKeyValue> {
        let b = value.extract::<bool>(py)?;
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = AnyValue {
            value: Arc::new(Mutex::new(Some(BoolValue(b)))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_int_value(key: &str, py: Python, value: PyObject) -> PyResult<PyKeyValue> {
        let i = value.extract::<i64>(py)?;
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = AnyValue {
            value: Arc::new(Mutex::new(Some(IntValue(i)))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_double_value(key: &str, py: Python, value: PyObject) -> PyResult<PyKeyValue> {
        let f = value.extract::<f64>(py)?;
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = AnyValue {
            value: Arc::new(Mutex::new(Some(DoubleValue(f)))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_array_value(key: &str, value: PyArrayValue) -> PyResult<PyKeyValue> {
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = AnyValue {
            value: Arc::new(Mutex::new(Some(ArrayValue(
                crate::processor::model::ArrayValue {
                    values: value.0.clone(),
                },
            )))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue { key, value })),
        })
    }
    #[getter]
    #[allow(deprecated)]
    fn key(&self, py: Python) -> PyResult<PyObject> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let binding = v.key.clone();
        let bind_lock = binding.lock();
        let x = Ok(bind_lock.unwrap().clone().into_py(py));
        x
    }
    #[setter]
    fn set_key(&mut self, new_value: &str) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let binding = v.key.clone();
        let mut bind_lock = binding.lock().unwrap();
        bind_lock.clear();
        bind_lock.insert_str(0, new_value);
        Ok(())
    }
    #[getter]
    fn value(&self) -> PyResult<PyAnyValue> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let binding = v.value.clone();
        Ok(PyAnyValue {
            inner: binding.clone(),
        })
    }
    #[setter]
    fn set_value(&mut self, new_value: &PyAnyValue) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let binding = v.value.clone();
        let bind_lock = binding.lock();
        let binding = new_value.inner.clone();
        let x = binding.lock().unwrap();
        bind_lock.unwrap().replace(x.clone().unwrap());
        Ok(())
    }
}

#[pyclass]
pub struct PyResource {
    pub attributes: Arc<Mutex<Vec<Arc<Mutex<KeyValue>>>>>,
}

#[pymethods]
impl PyResource {
    #[getter]
    fn attributes(&self) -> PyResult<PyAttributes> {
        Ok(PyAttributes(self.attributes.clone()))
    }
    #[setter]
    fn set_attributes(&mut self, new_value: &PyAttributes) -> PyResult<()> {
        let mut attrs = self.attributes.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let v = new_value.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        attrs.clear();
        for kv in v.iter() {
            attrs.push(kv.clone())
        }
        Ok(())
    }
}

#[pyclass]
struct PyAttributes(Arc<Mutex<Vec<Arc<Mutex<KeyValue>>>>>);

#[pymethods]
impl PyAttributes {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(PyAttributes(Arc::new(Mutex::new(vec![]))))
    }

    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<PyAttributesIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = PyAttributesIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<PyKeyValue> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(PyKeyValue {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }

    fn append<'py>(&self, item: &PyKeyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        k.push(item.inner.clone());
        Ok(())
    }

    fn append_attributes<'py>(&self, py: Python<'py>, items: Vec<PyObject>) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;

        for i in items.iter() {
            let x = i.extract::<PyKeyValue>(py)?;
            k.push(x.inner.clone());
        }
        Ok(())
    }

    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(inner.len())
    }
}

#[pyclass]
struct PyAttributesIter {
    inner: std::vec::IntoIter<Arc<Mutex<KeyValue>>>,
}

#[pymethods]
impl PyAttributesIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyKeyValue>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(PyKeyValue {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyResourceSpans {
    pub resource: Arc<Mutex<Option<Resource>>>,
    pub scope_spans: Arc<Mutex<Vec<Arc<Mutex<ScopeSpans>>>>>,
    pub schema_url: Arc<Mutex<String>>,
}

#[pymethods]
impl PyResourceSpans {
    #[getter]
    fn resource(&self) -> PyResult<Option<PyResource>> {
        let v = self.resource.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        if v.is_none() {
            return Ok(None);
        }
        let inner = v.clone().unwrap();
        Ok(Some(PyResource {
            attributes: inner.attributes.clone(),
        }))
    }
    #[getter]
    fn scope_spans(&self) -> PyResult<Option<PyScopeSpans>> {
        Ok(Some(PyScopeSpans(self.scope_spans.clone())))
    }
}

#[pyclass]
struct PyScopeSpans(Arc<Mutex<Vec<Arc<Mutex<ScopeSpans>>>>>);

#[pymethods]
impl PyScopeSpans {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<PyScopeSpansIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = PyScopeSpansIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(inner.len())
    }
}

#[pyclass]
struct PyScopeSpansIter {
    inner: std::vec::IntoIter<Arc<Mutex<ScopeSpans>>>,
}

#[pymethods]
impl PyScopeSpansIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyScopeSpan>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        let x = Ok(Some(PyScopeSpan {
            spans: inner.lock().unwrap().spans.clone(),
        }));
        x
    }
}

#[pyclass]
struct PyScopeSpan {
    spans: Arc<Mutex<Vec<Arc<Mutex<Span>>>>>,
}

#[pymethods]
impl PyScopeSpan {
    #[getter]
    fn spans(&self) -> PyResult<PySpans> {
        Ok(PySpans(self.spans.clone()))
    }
}

#[pyclass]
struct PySpans(Arc<Mutex<Vec<Arc<Mutex<Span>>>>>);

#[pymethods]
impl PySpans {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<PySpansIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = PySpansIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(inner.len())
    }
}

#[pyclass]
struct PySpansIter {
    inner: std::vec::IntoIter<Arc<Mutex<Span>>>,
}

#[pymethods]
impl PySpansIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PySpan>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(PySpan {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
struct PySpan {
    inner: Arc<Mutex<Span>>,
}

#[pymethods]
impl PySpan {
    #[getter]
    fn trace_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.trace_id.clone())
    }
    #[setter]
    fn set_trace_id(&mut self, new_value: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.trace_id = new_value;
        Ok(())
    }
    #[getter]
    fn span_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.span_id.clone())
    }
    #[setter]
    fn set_span_id(&self, new_value: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.span_id = new_value;
        Ok(())
    }
    #[getter]
    fn trace_state(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.trace_state.clone())
    }
    #[setter]
    fn set_trace_state(&self, new_value: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.trace_state = new_value;
        Ok(())
    }
    #[getter]
    fn parent_span_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.parent_span_id.clone())
    }
    #[setter]
    fn set_parent_span_id(&self, new_value: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.parent_span_id = new_value;
        Ok(())
    }
    #[getter]
    fn flags(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.flags)
    }
    #[setter]
    fn set_flags(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.flags = new_value;
        Ok(())
    }
    #[getter]
    fn name(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.name.clone())
    }
    #[setter]
    fn set_name(&self, new_value: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.name = new_value;
        Ok(())
    }
    #[getter]
    fn kind(&self) -> PyResult<i32> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.kind)
    }
    #[setter]
    fn set_kind(&self, new_value: i32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.kind = new_value;
        Ok(())
    }
    #[getter]
    fn start_time_unix_nano(&self) -> PyResult<u64> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.start_time_unix_nano)
    }
    #[setter]
    fn set_start_time_unix_nano(&self, new_value: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.start_time_unix_nano = new_value;
        Ok(())
    }
    #[getter]
    fn end_time_unix_nano(&self) -> PyResult<u64> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.end_time_unix_nano)
    }
    #[setter]
    fn set_end_time_unix_nano(&self, new_value: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.end_time_unix_nano = new_value;
        Ok(())
    }
    // TODO
    // pub attributes: Arc<Mutex<Vec<Arc<Mutex<KeyValue>>>>>,
    #[getter]
    fn dropped_attributes_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.dropped_attributes_count)
    }
    #[setter]
    fn set_dropped_attributes_count(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.dropped_attributes_count = new_value;
        Ok(())
    }
    // TODO
    // //pub events: ::prost::alloc::vec::Vec<span::Event>,
    #[getter]
    fn dropped_events_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.dropped_events_count)
    }
    #[setter]
    fn set_dropped_events_count(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.dropped_events_count = new_value;
        Ok(())
    }
    // TODO
    // //pub links: ::prost::alloc::vec::Vec<span::Link>,
    #[getter]
    fn dropped_links_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.dropped_links_count)
    }
    #[setter]
    fn set_dropped_links_count(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.dropped_links_count = new_value;
        Ok(())
    }
    // pub status: Arc<Mutex<Option<Status>>>,
}

#[pyclass]
struct LoggingStdout;

#[pymethods]
impl LoggingStdout {
    fn write(&self, data: &str) {
        println!("stdout from python: {:?}", data);
    }
}

// Module initialization
#[pymodule]
pub fn rotel_python_processor_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAnyValue>()?;
    m.add_class::<PyArrayValue>()?;
    m.add_class::<PyKeyValue>()?;
    m.add_class::<PyResource>()?;
    m.add_class::<PyAttributes>()?;
    m.add_class::<PySpan>()?;
    Ok(())
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::processor::model::Value::{BoolValue, StringValue};
    use pyo3::ffi::c_str;
    use std::ffi::CString;
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub fn initialize() {
        INIT.call_once(|| {
            pyo3::append_to_inittab!(rotel_python_processor_sdk);
            pyo3::prepare_freethreaded_python();
        });
    }

    fn run_script<'py, T: IntoPyObject<'py>>(script: &str, py: Python<'py>, pv: T) -> PyResult<()> {
        let sys = py.import("sys")?;
        sys.setattr("stdout", LoggingStdout.into_py(py))?;
        let code = std::fs::read_to_string(format!("./contrib/processor/tests/{}", script))?;
        let py_mod = PyModule::from_code(
            py,
            CString::new(code)?.as_c_str(),
            c_str!("example.py"),
            c_str!("example"),
        )?;

        let result_py_object = py_mod.getattr("process")?.call1((pv,));
        if result_py_object.is_err() {
            let err = result_py_object.unwrap_err();
            return Err(err);
        }
        Ok(())
    }

    #[test]
    fn test_read_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));

        let pv = PyAnyValue {
            inner: any_value_arc.clone(),
        };
        Python::with_gil(|py| -> PyResult<()> { run_script("read_value_test.py", py, pv) })
            .unwrap();
        let av = any_value_arc.lock().unwrap().clone().unwrap();
        let avx = av.value.lock().unwrap().clone();
        match avx.unwrap() {
            StringValue(s) => {
                assert_eq!(s, "foo");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_string_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let pv = PyAnyValue {
            inner: any_value_arc.clone(),
        };
        Python::with_gil(|py| -> PyResult<()> { run_script("write_string_value_test.py", py, pv) })
            .unwrap();
        let av = any_value_arc.lock().unwrap().clone().unwrap();
        let avx = av.value.lock().unwrap().clone();
        match avx.unwrap() {
            StringValue(s) => {
                assert_eq!(s, "changed");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_bool_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));

        let pv = PyAnyValue {
            inner: any_value_arc.clone(),
        };

        Python::with_gil(|py| -> PyResult<()> { run_script("write_bool_value_test.py", py, pv) })
            .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            BoolValue(b) => {
                assert!(b);
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn read_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> { run_script("read_key_value_key_test.py", py, kv) })
            .unwrap();
        let av = key.clone().lock().unwrap().clone();
        assert_eq!(av, "key".to_string());
        println!("{:?}", av);
    }

    #[test]
    fn write_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_key_value_key_test.py", py, kv)
        })
        .unwrap();
        let av = key.clone().lock().unwrap().clone();
        assert_eq!(av, "new_key".to_string());
        println!("{:?}", av);
    }

    #[test]
    fn read_key_value_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("read_key_value_value_test.py", py, kv)
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            StringValue(s) => {
                assert_eq!(s, "foo");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn write_key_value_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = PyKeyValue {
            inner: Arc::new(Mutex::new(KeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_key_value_value_test.py", py, kv)
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            StringValue(s) => {
                assert_eq!(s, "changed");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn read_resource_attributes() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = PyResource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("read_resource_attributes_test.py", py, resource)
        })
        .unwrap();
    }

    #[test]
    fn read_and_write_attributes_array_value() {
        initialize();

        let arc_value = Some(StringValue("foo".to_string()));
        let any_value_arc = Some(AnyValue {
            value: Arc::new(Mutex::new(arc_value)),
        });
        let array_value = crate::processor::model::ArrayValue {
            values: Arc::new(Mutex::new(vec![Arc::new(Mutex::new(
                any_value_arc.clone(),
            ))])),
        };
        let array_value_arc = Arc::new(Mutex::new(Some(ArrayValue(array_value))));
        let any_value_array_value_wrapper = Some(AnyValue {
            value: array_value_arc.clone(),
        });

        let any_value_array_value_wrapper_arc = Arc::new(Mutex::new(any_value_array_value_wrapper));

        let key = Arc::new(Mutex::new("key".to_string()));
        let kv = KeyValue {
            key: key.clone(),
            value: any_value_array_value_wrapper_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = PyResource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "read_and_write_attributes_array_value_test.py",
                py,
                resource,
            )
        })
        .unwrap();
    }

    #[test]
    fn write_resource_attributes_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = PyResource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "write_resource_attributes_key_value_key_test.py",
                py,
                resource,
            )
        })
        .unwrap();
        let av = key.clone().lock().unwrap().clone();
        assert_eq!(av, "new_key".to_string());
        println!("{:?}", av);
    }

    #[test]
    fn write_resource_attributes_key_value_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = PyResource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "write_resource_attributes_key_value_value_test.py",
                py,
                resource,
            )
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            StringValue(s) => {
                assert_eq!(s, "changed");
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn resource_attributes_append_attribute() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));
        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = PyResource {
            attributes: attrs_arc.clone(),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_attributes_append_attribute.py", py, resource)
        })
        .unwrap();
        println!("{:#?}", attrs_arc.lock().unwrap());
    }

    #[test]
    fn resource_attributes_set_attributes() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(AnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));
        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = PyResource {
            attributes: attrs_arc.clone(),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_attributes_set_attributes.py", py, resource)
        })
        .unwrap();
        println!("{:#?}", attrs_arc.lock().unwrap());
        let attrs = attrs_arc.lock().unwrap();
        assert_eq!(2, attrs.len());
        for kv in attrs.iter() {
            let guard = kv.lock();
            let kv_guard = guard.unwrap();
            let key = kv_guard.key.lock().unwrap().to_string();
            let value = kv_guard.value.lock().unwrap();
            assert_ne!(key, "key");
            assert!(key == "os.name" || key == "os.version");
            assert!(value.is_some());
            let av = value.clone().unwrap();
            let value = av.value.lock().unwrap();
            assert!(value.is_some());
        }
    }

    #[test]
    fn resource_spans_append_attributes() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::processor::model::otel_transform::transform(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = PyResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: Arc::new(Mutex::new(vec![])),
            schema_url: Arc::new(Mutex::new("".to_string())),
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_spans_append_attribute.py", py, py_resource_spans)
        })
        .unwrap();
        println!("{:#?}", resource_spans.resource.lock().unwrap());
    }

    #[test]
    fn resource_spans_iterate_spans() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans = crate::processor::model::otel_transform::transform(
            export_req.resource_spans[0].clone(),
        );
        let py_resource_spans = PyResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: Arc::new(Mutex::new("".to_string())),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_spans_iterate_spans.py", py, py_resource_spans)
        })
        .unwrap();
        println!("{:#?}", resource_spans.resource.lock().unwrap());
    }
}
