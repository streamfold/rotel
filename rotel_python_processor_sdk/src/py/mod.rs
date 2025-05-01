use crate::model::otel_transform::convert_attributes;
use crate::model::RValue::{
    BoolValue, BytesValue, DoubleValue, IntValue, KvListValue, RVArrayValue, StringValue,
};
use crate::model::{
    RAnyValue, REvent, RInstrumentationScope, RKeyValue, RLink, RResource, RScopeSpans, RSpan,
    RStatus,
};
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use std::vec;

// Wrapper for AnyValue that can be exposed to Python
#[pyclass]
struct AnyValue {
    inner: Arc<Mutex<Option<RAnyValue>>>,
}

#[pymethods]
impl AnyValue {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(AnyValue {
            inner: Arc::new(Mutex::new(Some(RAnyValue {
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
            Some(BytesValue(b)) => Ok(b.into_py(py)),
            Some(RVArrayValue(a)) => Ok(a.convert_to_py(py)?),
            Some(KvListValue(k)) => Ok(k.convert_to_py(py)?),
            None => Ok(py.None()),
        };
        x // to avoid dropping
    }
    #[setter]
    fn set_string_value(&mut self, new_value: &str) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;

        // TODO WE need none checks on these setters
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
    fn set_bytes_value(&mut self, new_value: Vec<u8>) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(BytesValue(new_value));
        Ok(())
    }
    #[setter]
    fn set_array_value(&mut self, new_value: ArrayValue) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(RVArrayValue(crate::model::RArrayValue {
                values: new_value.0.clone(),
            }));
        Ok(())
    }
    #[setter]
    fn set_key_value_list_value(&mut self, new_value: KeyValueList) -> PyResult<()> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(KvListValue(crate::model::RKeyValueList {
                values: new_value.0.clone(),
            }));
        Ok(())
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ArrayValue(pub Arc<Mutex<Vec<Arc<Mutex<Option<RAnyValue>>>>>>);

#[pymethods]
impl ArrayValue {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ArrayValue(Arc::new(Mutex::new(vec![]))))
    }
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<ArrayValueIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = ArrayValueIter {
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
    fn __getitem__(&self, index: usize) -> PyResult<AnyValue> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(AnyValue {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn append(&self, item: &AnyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        k.push(item.inner.clone());
        Ok(())
    }
}

#[pyclass]
struct ArrayValueIter {
    inner: std::vec::IntoIter<Arc<Mutex<Option<RAnyValue>>>>,
}

#[pymethods]
impl ArrayValueIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<AnyValue>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(AnyValue {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Clone)]
pub struct KeyValueList(pub Arc<Mutex<Vec<RKeyValue>>>);

#[pymethods]
impl KeyValueList {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(KeyValueList(Arc::new(Mutex::new(vec![]))))
    }
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<KeyValueListIter>> {
        let iter = KeyValueListIter {
            inner: self.0.clone(),
            idx: 0,
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }
    fn __getitem__(&self, index: usize) -> PyResult<KeyValue> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(KeyValue {
                inner: Arc::new(Mutex::new(item.clone())),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(inner.len())
    }
    fn append(&self, item: KeyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let inner = item.inner.lock().unwrap();
        let inner = inner.clone();
        k.push(inner);
        Ok(())
    }
}

#[pyclass]
struct KeyValueListIter {
    inner: Arc<Mutex<Vec<RKeyValue>>>,
    idx: usize,
}

#[pymethods]
impl KeyValueListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self) -> PyResult<Option<KeyValue>> {
        // Acquire a lock on the Mutex to access the inner Vec
        let guard = self.inner.lock().unwrap();
        if self.idx > guard.len() - 1 {
            return Ok(None);
        }
        let v = guard.get(self.idx);
        if v.is_none() {
            return Ok(None);
        }
        let kv = v.unwrap();
        self.idx += 1;
        Ok(Some(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })),
        }))
    }
}

#[pyclass]
#[derive(Clone)]
struct KeyValue {
    inner: Arc<Mutex<RKeyValue>>,
}

#[pymethods]
impl KeyValue {
    // Helper methods creating new inner value types
    #[staticmethod]
    fn new_string_value(key: &str, value: &str) -> PyResult<KeyValue> {
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = RAnyValue {
            value: Arc::new(Mutex::new(Some(StringValue(value.to_string())))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_bool_value(key: &str, py: Python, value: PyObject) -> PyResult<KeyValue> {
        let b = value.extract::<bool>(py)?;
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = RAnyValue {
            value: Arc::new(Mutex::new(Some(BoolValue(b)))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_int_value(key: &str, py: Python, value: PyObject) -> PyResult<KeyValue> {
        let i = value.extract::<i64>(py)?;
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = RAnyValue {
            value: Arc::new(Mutex::new(Some(IntValue(i)))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_double_value(key: &str, py: Python, value: PyObject) -> PyResult<KeyValue> {
        let f = value.extract::<f64>(py)?;
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = RAnyValue {
            value: Arc::new(Mutex::new(Some(DoubleValue(f)))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_bytes_value(key: &str, py: Python, value: PyObject) -> PyResult<KeyValue> {
        let f = value.extract::<Vec<u8>>(py)?;
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = RAnyValue {
            value: Arc::new(Mutex::new(Some(BytesValue(f)))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_array_value(key: &str, value: ArrayValue) -> PyResult<KeyValue> {
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = RAnyValue {
            value: Arc::new(Mutex::new(Some(RVArrayValue(crate::model::RArrayValue {
                values: value.0.clone(),
            })))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue { key, value })),
        })
    }
    // Helper methods for class
    #[staticmethod]
    fn new_kv_list(key: &str, value: KeyValueList) -> PyResult<KeyValue> {
        let key = Arc::new(Mutex::new(key.to_string()));
        let value = RAnyValue {
            value: Arc::new(Mutex::new(Some(KvListValue(crate::model::RKeyValueList {
                values: value.0.clone(),
            })))),
        };
        let value = Arc::new(Mutex::new(Some(value)));
        Ok(KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue { key, value })),
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
    fn value(&self) -> PyResult<AnyValue> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let binding = v.value.clone();
        Ok(AnyValue {
            inner: binding.clone(),
        })
    }
    #[setter]
    fn set_value(&mut self, new_value: &AnyValue) -> PyResult<()> {
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
#[derive(Clone)]
pub struct Resource {
    pub attributes: Arc<Mutex<Vec<Arc<Mutex<RKeyValue>>>>>,
    pub dropped_attributes_count: Arc<Mutex<u32>>,
}

#[pymethods]
impl Resource {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Resource {
            attributes: Arc::new(Mutex::new(vec![])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        })
    }
    #[getter]
    fn attributes(&self) -> PyResult<Attributes> {
        Ok(Attributes(self.attributes.clone()))
    }
    #[setter]
    fn set_attributes(&mut self, new_value: &Attributes) -> PyResult<()> {
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
    #[getter]
    fn dropped_attributes_count(&self) -> PyResult<u32> {
        let dropped = self.dropped_attributes_count.lock().unwrap();
        Ok(dropped.clone())
    }
    #[setter]
    fn set_dropped_attributes_count(&mut self, new_value: u32) -> PyResult<()> {
        let mut dropped = self.dropped_attributes_count.lock().unwrap();
        *dropped = new_value;
        Ok(())
    }
}

#[pyclass]
struct Attributes(Arc<Mutex<Vec<Arc<Mutex<RKeyValue>>>>>);

#[pymethods]
impl Attributes {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Attributes(Arc::new(Mutex::new(vec![]))))
    }

    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<AttributesIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = AttributesIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<KeyValue> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(KeyValue {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }

    fn append<'py>(&self, item: &KeyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        k.push(item.inner.clone());
        Ok(())
    }

    fn append_attributes(&self, items: Vec<KeyValue>) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;

        for kv in items.iter() {
            k.push(kv.inner.clone());
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
struct AttributesIter {
    inner: std::vec::IntoIter<Arc<Mutex<RKeyValue>>>,
}

#[pymethods]
impl AttributesIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<KeyValue>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(KeyValue {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ResourceSpans {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_spans: Arc<Mutex<Vec<Arc<Mutex<RScopeSpans>>>>>,
    pub schema_url: String,
}

#[pymethods]
impl ResourceSpans {
    #[getter]
    fn resource(&self) -> PyResult<Option<Resource>> {
        let v = self.resource.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        if v.is_none() {
            return Ok(None);
        }
        let inner = v.clone().unwrap();
        Ok(Some(Resource {
            attributes: inner.attributes.clone(),
            dropped_attributes_count: inner.dropped_attributes_count.clone(),
        }))
    }
    #[setter]
    fn set_resource(&mut self, resource: Resource) -> PyResult<()> {
        let mut inner = self.resource.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        *inner = Some(RResource {
            attributes: resource.attributes,
            dropped_attributes_count: resource.dropped_attributes_count,
        });
        Ok(())
    }
    #[getter]
    fn scope_spans(&self) -> PyResult<ScopeSpansList> {
        Ok(ScopeSpansList(self.scope_spans.clone()))
    }
    #[setter]
    fn set_scope_spans(&mut self, scope_spans: Vec<ScopeSpans>) -> PyResult<()> {
        let mut inner = self.scope_spans.lock().unwrap();
        inner.clear();
        for sc in scope_spans {
            inner.push(Arc::new(Mutex::new(RScopeSpans {
                scope: sc.scope.clone(),
                spans: sc.spans.clone(),
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
struct ScopeSpansList(Arc<Mutex<Vec<Arc<Mutex<RScopeSpans>>>>>);

#[pymethods]
impl ScopeSpansList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<ScopeSpansListIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = ScopeSpansListIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<ScopeSpans> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => {
                let item = item.lock().unwrap();
                Ok(ScopeSpans {
                    scope: item.scope.clone(),
                    spans: item.spans.clone(),
                    schema_url: item.schema_url.clone(),
                })
            }
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }

    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(inner.len())
    }
}

#[pyclass]
struct ScopeSpansListIter {
    inner: std::vec::IntoIter<Arc<Mutex<RScopeSpans>>>,
}

#[pymethods]
impl ScopeSpansListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<ScopeSpans>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        let inner = inner.lock().unwrap();
        let x = Ok(Some(ScopeSpans {
            scope: inner.scope.clone(),
            spans: inner.spans.clone(),
            schema_url: inner.schema_url.clone(),
        }));
        x
    }
}

#[pyclass]
#[derive(Clone)]
struct ScopeSpans {
    scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    spans: Arc<Mutex<Vec<Arc<Mutex<RSpan>>>>>,
    schema_url: String,
}

#[pymethods]
impl ScopeSpans {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ScopeSpans {
            scope: Arc::new(Mutex::new(None)),
            spans: Arc::new(Mutex::new(vec![])),
            schema_url: "".to_string(),
        })
    }
    #[getter]
    fn spans(&self) -> PyResult<Spans> {
        Ok(Spans(self.spans.clone()))
    }
    #[setter]
    fn set_spans(&mut self, spans: Vec<Span>) -> PyResult<()> {
        let mut inner = self.spans.lock().unwrap();
        inner.clear();
        for s in spans {
            inner.push(s.inner);
        }
        Ok(())
    }
    #[getter]
    fn scope(&self) -> PyResult<Option<InstrumentationScope>> {
        {
            let v = self.scope.lock().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
            })?;
            if v.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(InstrumentationScope(self.scope.clone())))
    }
    #[setter]
    fn set_scope(&mut self, scope: InstrumentationScope) -> PyResult<()> {
        let mut v = self.scope.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let inner = scope.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
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
#[derive(Clone)]
struct InstrumentationScope(Arc<Mutex<Option<RInstrumentationScope>>>);

#[pymethods]
impl InstrumentationScope {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(InstrumentationScope(Arc::new(Mutex::new(Some(
            RInstrumentationScope {
                // TODO: Probably provide the otel defaults here?
                name: "".to_string(),
                version: "".to_string(),
                attributes_raw: vec![],
                attributes_arc: None,
                dropped_attributes_count: 0,
            },
        )))))
    }
    #[getter]
    fn name(&self) -> PyResult<String> {
        let binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let v = binding
            .clone()
            .ok_or(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "InstrumentationScope is None",
            ))?;
        Ok(v.name)
    }
    #[setter]
    fn set_name(&self, name: String) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let updated_scope = match binding.take() {
            Some(current) => RInstrumentationScope {
                name,
                version: current.version,
                attributes_arc: current.attributes_arc,
                attributes_raw: current.attributes_raw,
                dropped_attributes_count: current.dropped_attributes_count,
            },
            None => RInstrumentationScope {
                name,
                ..Default::default()
            },
        };
        binding.replace(updated_scope);
        Ok(())
    }
    #[getter]
    fn version(&self) -> PyResult<String> {
        let binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let v = binding
            .clone()
            .ok_or(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "InstrumentationScope is None",
            ))?;
        Ok(v.version)
    }
    #[setter]
    fn set_version(&self, version: String) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let updated_scope = match binding.take() {
            Some(current) => RInstrumentationScope {
                version,
                attributes_arc: current.attributes_arc,
                attributes_raw: current.attributes_raw,
                name: current.name,
                dropped_attributes_count: current.dropped_attributes_count,
            },
            None => RInstrumentationScope {
                version,
                ..Default::default()
            },
        };
        binding.replace(updated_scope);
        Ok(())
    }
    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let mut binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        if binding.is_none() {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "InstrumentationScope is None, should never occur here",
            );
        }
        let arc = binding.take().unwrap();
        let mut arc_copy = arc.clone();
        // Now we need to see if we have an existing attributes list
        if arc_copy.attributes_arc.is_some() {
            let attr_arc = arc_copy.attributes_arc.take().unwrap();
            let attr_arc_copy = attr_arc.clone();
            arc_copy.attributes_arc.replace(attr_arc);
            binding.replace(arc_copy);
            Ok(AttributesList(attr_arc_copy))
        } else {
            let attrs = convert_attributes(arc_copy.attributes_raw.clone());
            let attrs = Arc::new(Mutex::new(attrs));
            arc_copy.attributes_arc.replace(attrs.clone());
            binding.replace(arc_copy);
            Ok(AttributesList(attrs.clone()))
        }
    }
    #[getter]
    fn dropped_attributes_count(&self) -> PyResult<u32> {
        let binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let v = binding
            .clone()
            .ok_or(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "InstrumentationScope is None",
            ))?;
        Ok(v.dropped_attributes_count)
    }
    #[setter]
    fn set_dropped_attributes_count(&self, dropped_attributes_count: u32) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let updated_scope = match binding.take() {
            Some(current) => RInstrumentationScope {
                name: current.name,
                version: current.version,
                attributes_arc: current.attributes_arc,
                attributes_raw: current.attributes_raw,
                dropped_attributes_count,
            },
            None => RInstrumentationScope {
                dropped_attributes_count,
                ..Default::default()
            },
        };
        binding.replace(updated_scope);
        Ok(())
    }
}

// TODO: Remove this and update PyAttributesIter to use a Arc<Mutex<Vec<KeyValue>>>.
// Careful observer will notice this looks like PyAttributes called from PyResource, however
// that class has additional ArcMutexes around the KeyValues. We tried out a new pattern here for the scope attributes
// and it appears to be working well. For the sake of safety I want to finish additional testing before going back and
// refactoring. WHen we do we should be able to remove this and share a single attributes and attributes iter type.
#[pyclass]
struct AttributesList(Arc<Mutex<Vec<RKeyValue>>>);

#[pymethods]
impl AttributesList {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(AttributesList(Arc::new(Mutex::new(vec![]))))
    }
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<AttributesListIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = AttributesListIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<KeyValue> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(KeyValue {
                inner: Arc::new(Mutex::new(item.clone())),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn append<'py>(&self, item: &KeyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let inner = item.inner.lock().unwrap();
        let inner = inner.clone();
        k.push(inner);
        Ok(())
    }
    fn append_attributes(&self, items: Vec<KeyValue>) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        for kv in items.iter() {
            let inner = kv.inner.lock().unwrap();
            let inner = inner.clone();
            k.push(inner.clone());
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
struct AttributesListIter {
    inner: std::vec::IntoIter<RKeyValue>,
}

#[pymethods]
impl AttributesListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<KeyValue>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(KeyValue {
            inner: Arc::new(Mutex::new(inner)),
        }))
    }
}

#[pyclass]
struct Spans(Arc<Mutex<Vec<Arc<Mutex<RSpan>>>>>);

#[pymethods]
impl Spans {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<SpansIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = SpansIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }
    fn __getitem__(&self, index: usize) -> PyResult<Span> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(Span {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn append(&self, item: &Span) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        k.push(item.inner.clone());
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
struct SpansIter {
    inner: std::vec::IntoIter<Arc<Mutex<RSpan>>>,
}

#[pymethods]
impl SpansIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Span>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(Span {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Clone)]
struct Span {
    inner: Arc<Mutex<RSpan>>,
}

#[pymethods]
impl Span {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Span {
            inner: Arc::new(Mutex::new(RSpan {
                trace_id: vec![],
                span_id: vec![],
                trace_state: "".to_string(),
                parent_span_id: vec![],
                flags: 0,
                name: "".to_string(),
                kind: 0,
                start_time_unix_nano: 0,
                end_time_unix_nano: 0,
                attributes_raw: vec![],
                attributes_arc: None,
                dropped_attributes_count: 0,
                events_raw: vec![],
                events_arc: None,
                dropped_events_count: 0,
                links_raw: vec![],
                links_arc: None,
                dropped_links_count: 0,
                status: Arc::new(Mutex::new(None)),
            })),
        })
    }
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
    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let mut binding = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let attrs = convert_attributes(binding.attributes_raw.clone());
        let attrs = Arc::new(Mutex::new(attrs));
        binding.attributes_arc.replace(attrs.clone());
        Ok(AttributesList(attrs))
    }
    #[setter]
    fn set_attributes(&mut self, attrs: &AttributesList) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let mut new_attrs = Vec::with_capacity(attrs.0.lock().unwrap().len());
        for kv in attrs.0.lock().unwrap().iter() {
            new_attrs.push(kv.clone())
        }
        let new_attrs = Arc::new(Mutex::new(new_attrs));
        inner.attributes_arc.replace(new_attrs.clone());
        Ok(())
    }
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
    #[getter]
    fn events(&self) -> PyResult<Events> {
        let mut v = self.inner.lock().unwrap();
        let new_events = v
            .events_raw
            .iter()
            .map(|e| {
                Arc::new(Mutex::new(REvent {
                    time_unix_nano: e.time_unix_nano,
                    name: e.name.clone(),
                    attributes: Arc::new(Mutex::new(convert_attributes(e.attributes.to_owned()))),
                    dropped_attributes_count: 0,
                }))
            })
            .collect();
        let new_events = Arc::new(Mutex::new(new_events));
        v.events_arc.replace(new_events.clone());
        Ok(Events(new_events))
    }
    #[setter]
    fn set_events(&self, events: Vec<Event>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let mut new_events = Vec::with_capacity(events.len());
        for event in events {
            new_events.push(event.inner);
        }
        v.events_arc.replace(Arc::new(Mutex::new(new_events)));
        Ok(())
    }
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
    #[getter]
    fn links(&self) -> PyResult<Links> {
        let mut v = self.inner.lock().unwrap();
        let new_links = v
            .links_raw
            .iter()
            .map(|l| {
                Arc::new(Mutex::new(RLink {
                    trace_id: l.trace_id.to_owned(),
                    span_id: l.span_id.to_owned(),
                    trace_state: l.trace_state.to_owned(),
                    attributes: Arc::new(Mutex::new(convert_attributes(l.attributes.to_owned()))),
                    dropped_attributes_count: l.dropped_attributes_count,
                    flags: l.flags,
                }))
            })
            .collect();
        let new_links = Arc::new(Mutex::new(new_links));
        v.links_arc.replace(new_links.clone());
        Ok(Links(new_links))
    }
    #[setter]
    fn set_links(&self, links: Vec<Link>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let mut new_links = Vec::with_capacity(links.len());
        for link in links {
            new_links.push(link.inner);
        }
        v.links_arc.replace(Arc::new(Mutex::new(new_links)));
        Ok(())
    }
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
    #[getter]
    fn status(&self) -> PyResult<Option<Status>> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        {
            let status = v.status.lock().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
            })?;
            if status.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(Status(v.status.clone())))
    }
    #[setter]
    fn set_status(&self, status: Status) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let new_status = status.0.lock().unwrap();
        if new_status.is_none() {
            v.status = Arc::new(Mutex::new(None));
        } else {
            v.status = Arc::new(Mutex::new(new_status.clone()));
        }
        Ok(())
    }
}

#[pyclass]
struct Events(Arc<Mutex<Vec<Arc<Mutex<REvent>>>>>);

#[pymethods]
impl Events {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<EventsIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = EventsIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }
    fn __getitem__(&self, index: usize) -> PyResult<Event> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(Event {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(inner.len())
    }
    fn append(&self, item: &Event) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        k.push(item.inner.clone());
        Ok(())
    }
}

#[pyclass]
struct EventsIter {
    inner: std::vec::IntoIter<Arc<Mutex<REvent>>>,
}

#[pymethods]
impl EventsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Event>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(Event {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Debug, Clone)]
struct Event {
    inner: Arc<Mutex<REvent>>,
}

#[pymethods]
impl Event {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Event {
            inner: Arc::new(Mutex::new(REvent {
                time_unix_nano: 0,
                name: "".to_string(),
                attributes: Arc::new(Mutex::new(vec![])),
                dropped_attributes_count: 0,
            })),
        })
    }
    #[getter]
    fn time_unix_nano(&self) -> PyResult<u64> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.time_unix_nano)
    }
    #[setter]
    fn set_time_unix_nano(&mut self, unix_nano: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.time_unix_nano = unix_nano;
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
    fn set_name(&mut self, name: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.name = name;
        Ok(())
    }
    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let binding = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(AttributesList(binding.attributes.clone()))
    }
    #[setter]
    fn set_attributes(&mut self, attrs: &AttributesList) -> PyResult<()> {
        let inner = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let mut v = inner.attributes.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clear();
        for kv in attrs.0.lock().unwrap().iter() {
            v.push(kv.clone())
        }
        Ok(())
    }
    #[getter]
    fn dropped_attributes_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.dropped_attributes_count)
    }
    #[setter]
    fn set_dropped_attributes_count(&mut self, count: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.dropped_attributes_count = count;
        Ok(())
    }
}

#[pyclass]
struct Links(Arc<Mutex<Vec<Arc<Mutex<RLink>>>>>);

#[pymethods]
impl Links {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<LinksIter>> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let iter = LinksIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }
    fn __getitem__(&self, index: usize) -> PyResult<Link> {
        let inner = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        match inner.get(index) {
            Some(item) => Ok(Link {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn append<'py>(&self, item: &Link) -> PyResult<()> {
        let mut k = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        k.push(item.inner.clone());
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
struct LinksIter {
    inner: std::vec::IntoIter<Arc<Mutex<RLink>>>,
}

#[pymethods]
impl LinksIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Link>> {
        let kv = slf.inner.next();
        if kv.is_none() {
            return Ok(None);
        }
        let inner = kv.unwrap();
        Ok(Some(Link {
            inner: inner.clone(),
        }))
    }
}

#[pyclass]
#[derive(Debug, Clone)]
struct Link {
    inner: Arc<Mutex<RLink>>,
}

#[pymethods]
impl Link {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Link {
            inner: Arc::new(Mutex::new(RLink {
                trace_id: vec![],
                span_id: vec![],
                trace_state: "".to_string(),
                attributes: Arc::new(Mutex::new(vec![])),
                dropped_attributes_count: 0,
                flags: 0,
            })),
        })
    }
    #[getter]
    fn trace_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(v.trace_id.clone())
    }
    #[setter]
    fn set_trace_id(&mut self, trace_id: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.trace_id = trace_id;
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
    fn set_span_id(&mut self, span_id: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.span_id = span_id;
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
    fn set_trace_state(&mut self, trace_state: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.trace_state = trace_state;
        Ok(())
    }
    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let binding = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        Ok(AttributesList(binding.attributes.clone()))
    }
    #[setter]
    fn set_attributes(&mut self, attrs: &AttributesList) -> PyResult<()> {
        let inner = self.inner.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let mut v = inner.attributes.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        v.clear();
        for kv in attrs.0.lock().unwrap().iter() {
            v.push(kv.clone())
        }
        Ok(())
    }
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
}

#[pyclass]
#[derive(Clone)]
struct Status(Arc<Mutex<Option<RStatus>>>);

#[pymethods]
impl Status {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Status(Arc::new(Mutex::new(Some(RStatus {
            message: "".to_string(),
            code: 0,
        })))))
    }
    #[getter]
    fn message(&self) -> PyResult<String> {
        let binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let v = binding
            .clone()
            .ok_or(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Status is None",
            ))?;
        Ok(v.message)
    }
    #[setter]
    fn set_message(&mut self, message: String) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let updated_status = match binding.take() {
            Some(current) => RStatus {
                message,
                code: current.code,
            },
            None => RStatus {
                message,
                ..Default::default()
            },
        };
        binding.replace(updated_status);
        Ok(())
    }
    #[getter]
    fn code(&self) -> PyResult<i32> {
        let binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let v = binding
            .clone()
            .ok_or(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Status is None",
            ))?;
        Ok(v.code)
    }
    #[setter]
    fn set_code(&mut self, code: StatusCode) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to lock mutex")
        })?;
        let updated_status = match binding.take() {
            Some(current) => RStatus {
                message: current.message,
                code: code as i32,
            },
            None => RStatus {
                code: code as i32,
                ..Default::default()
            },
        };
        binding.replace(updated_status);
        Ok(())
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StatusCode {
    /// The default status.
    Unset = 0,
    /// The Span has been validated by an Application developer or Operator to
    /// have completed successfully.
    Ok = 1,
    /// The Span contains an error.
    Error = 2,
}

#[pymethods]
impl StatusCode {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(StatusCode::Unset)
    }
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unset => "STATUS_CODE_UNSET",
            Self::Ok => "STATUS_CODE_OK",
            Self::Error => "STATUS_CODE_ERROR",
        }
    }
}

#[pyclass]
struct LoggingStdout;

#[pymethods]
impl LoggingStdout {
    fn write(&self, data: &str) {
        println!("stdout from python: {:?}", data);
    }
}

// Python module definition
#[pymodule]
pub fn rotel_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let open_telemetry_module = PyModule::new(m.py(), "open_telemetry")?;
    let trace_module = PyModule::new(open_telemetry_module.py(), "trace")?;
    let resource_module = PyModule::new(open_telemetry_module.py(), "resource")?;
    let common_module = PyModule::new(open_telemetry_module.py(), "common")?;
    let trace_v1_module = PyModule::new(trace_module.py(), "v1")?;
    let common_v1_module = PyModule::new(common_module.py(), "v1")?;
    let resource_v1_module = PyModule::new(resource_module.py(), "v1")?;

    trace_module.add_submodule(&trace_v1_module)?;
    common_module.add_submodule(&common_v1_module)?;
    resource_module.add_submodule(&resource_v1_module)?;
    open_telemetry_module.add_submodule(&trace_module)?;
    open_telemetry_module.add_submodule(&resource_module)?;
    open_telemetry_module.add_submodule(&common_module)?;
    m.add_submodule(&open_telemetry_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry", &open_telemetry_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.trace", &trace_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.trace.v1", &trace_v1_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.resource", &resource_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.resource.v1", &resource_v1_module)?;

    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.common", &common_module)?;
    m.py()
        .import("sys")?
        .getattr("modules")?
        .set_item("rotel_sdk.open_telemetry.common.v1", &common_v1_module)?;

    common_v1_module.add_class::<AnyValue>()?;
    common_v1_module.add_class::<ArrayValue>()?;
    common_v1_module.add_class::<KeyValueList>()?;
    common_v1_module.add_class::<KeyValue>()?;
    common_v1_module.add_class::<Attributes>()?;
    common_v1_module.add_class::<InstrumentationScope>()?;

    resource_v1_module.add_class::<Resource>()?;

    trace_v1_module.add_class::<ResourceSpans>()?;
    trace_v1_module.add_class::<ScopeSpans>()?;
    trace_v1_module.add_class::<Span>()?;
    trace_v1_module.add_class::<Event>()?;
    trace_v1_module.add_class::<Link>()?;
    trace_v1_module.add_class::<Status>()?;
    trace_v1_module.add_class::<StatusCode>()?;

    Ok(())
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use pyo3::ffi::c_str;
    use std::ffi::CString;
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub fn initialize() {
        INIT.call_once(|| {
            pyo3::append_to_inittab!(rotel_sdk);
            pyo3::prepare_freethreaded_python();
        });
    }

    fn run_script<'py, T: IntoPyObject<'py>>(script: &str, py: Python<'py>, pv: T) -> PyResult<()> {
        let sys = py.import("sys")?;
        sys.setattr("stdout", LoggingStdout.into_py(py))?;
        let code = std::fs::read_to_string(format!("../contrib/processor/tests/{}", script))?;
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
        // For debugging python stdout -- println!("{:?}", result_py_object.unwrap());
        Ok(())
    }

    #[test]
    fn test_read_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));

        let pv = AnyValue {
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let pv = AnyValue {
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));

        let pv = AnyValue {
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
    fn write_bytes_any_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));

        let pv = AnyValue {
            inner: any_value_arc.clone(),
        };

        Python::with_gil(|py| -> PyResult<()> { run_script("write_bytes_value_test.py", py, pv) })
            .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            BytesValue(b) => {
                assert_eq!(b"111111".to_vec(), b);
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn read_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
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
    fn write_key_value_bytes_value() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = KeyValue {
            inner: Arc::new(Mutex::new(RKeyValue {
                key: key.clone(),
                value: any_value_arc.clone(),
            })),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_key_value_bytes_value_test.py", py, kv)
        })
        .unwrap();
        match arc_value.lock().unwrap().clone().unwrap() {
            BytesValue(s) => {
                assert_eq!(b"111111".to_vec(), s);
            }
            _ => panic!("wrong type"),
        }
        println!("{:?}", any_value_arc.lock().unwrap().clone().unwrap());
    }

    #[test]
    fn read_resource_attributes() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = Resource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
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
        let any_value_arc = Some(RAnyValue {
            value: Arc::new(Mutex::new(arc_value)),
        });
        let array_value = crate::model::RArrayValue {
            values: Arc::new(Mutex::new(vec![Arc::new(Mutex::new(
                any_value_arc.clone(),
            ))])),
        };
        let array_value_arc = Arc::new(Mutex::new(Some(RVArrayValue(array_value))));
        let any_value_array_value_wrapper = Some(RAnyValue {
            value: array_value_arc.clone(),
        });

        let any_value_array_value_wrapper_arc = Arc::new(Mutex::new(any_value_array_value_wrapper));

        let key = Arc::new(Mutex::new("key".to_string()));
        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_array_value_wrapper_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = Resource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
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
    fn read_and_write_attributes_key_value_list_value() {
        initialize();

        let value = Some(StringValue("foo".to_string()));
        let any_value = Some(RAnyValue {
            value: Arc::new(Mutex::new(value)),
        });
        let any_value_arc = Arc::new(Mutex::new(any_value));
        let arc_key = Arc::new(Mutex::new("inner_key".to_string()));

        let kev_value = RKeyValue {
            key: arc_key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_list = crate::model::RKeyValueList {
            values: Arc::new(Mutex::new(vec![kev_value])),
        };

        let array_value_arc = Arc::new(Mutex::new(Some(KvListValue(kv_list))));
        let any_value_array_value_wrapper = Some(RAnyValue {
            value: array_value_arc.clone(),
        });

        let any_value_array_value_wrapper_arc = Arc::new(Mutex::new(any_value_array_value_wrapper));

        let key = Arc::new(Mutex::new("key".to_string()));
        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_array_value_wrapper_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = Resource {
            attributes: attrs_arc.clone(),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
        };

        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "read_and_write_attributes_key_value_list_test.py",
                py,
                resource,
            )
        })
        .unwrap();

        let mut value = attrs_arc.lock().unwrap();
        let value = value.pop().unwrap();
        let value = Arc::into_inner(value).unwrap().into_inner().unwrap();
        let value = Arc::into_inner(value.value).unwrap().into_inner().unwrap();
        let value = value.unwrap().value;
        let value = Arc::into_inner(value)
            .unwrap()
            .into_inner()
            .unwrap()
            .unwrap();
        match value {
            KvListValue(k) => {
                let mut value = k.values.lock().unwrap().clone();
                let value = value.pop();
                match value {
                    None => {
                        panic!("wrong type")
                    }
                    Some(v) => {
                        let v = v.value.lock().unwrap().clone();
                        match v {
                            None => {
                                panic!("wrong type")
                            }
                            Some(v) => {
                                let value = v.value.lock().unwrap().clone().unwrap();
                                match value {
                                    StringValue(s) => assert_eq!("baz", s),
                                    _ => panic!("wrong type"),
                                }
                            }
                        }
                    }
                }
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn write_resource_attributes_key_value_key() {
        initialize();
        let arc_value = Arc::new(Mutex::new(Some(StringValue("foo".to_string()))));
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = Resource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));

        let resource = Resource {
            attributes: Arc::new(Mutex::new(vec![kv_arc.clone()])),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));
        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = Resource {
            attributes: attrs_arc.clone(),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
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
        let any_value_arc = Arc::new(Mutex::new(Some(RAnyValue {
            value: arc_value.clone(),
        })));
        let key = Arc::new(Mutex::new("key".to_string()));

        let kv = RKeyValue {
            key: key.clone(),
            value: any_value_arc.clone(),
        };

        let kv_arc = Arc::new(Mutex::new(kv));
        let attrs_arc = Arc::new(Mutex::new(vec![kv_arc.clone()]));
        let resource = Resource {
            attributes: attrs_arc.clone(),
            dropped_attributes_count: Arc::new(Mutex::new(0)),
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
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: Arc::new(Mutex::new(vec![])),
            schema_url: resource_spans.schema_url,
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
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("resource_spans_iterate_spans.py", py, py_resource_spans)
        })
        .unwrap();
        println!("{:#?}", resource_spans.resource.lock().unwrap());
    }

    #[test]
    fn read_and_write_instrumentation_scope() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "read_and_write_instrumentation_scope_test.py",
                py,
                py_resource_spans,
            )
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let scope_spans = scope_spans.pop().unwrap();
        let scope = scope_spans.scope.unwrap();
        assert_eq!("name_changed", scope.name);
        assert_eq!("0.0.2", scope.version);
        assert_eq!(100, scope.dropped_attributes_count);
        assert_eq!(scope.attributes.len(), 2);
        for attr in &scope.attributes {
            let value = attr.value.clone().unwrap();
            let value = value.value.unwrap();
            match attr.key.as_str() {
                "key_changed" => match value {
                    opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => {
                        assert_eq!(i, 200);
                    }
                    _ => {
                        panic!("wrong type for key_changed: {:?}", value);
                    }
                },
                "severity" => match value {
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                        assert_eq!(s, "WARN");
                    }
                    _ => {
                        panic!("wrong type for severity: {:?}", value);
                    }
                },
                _ => {
                    panic!("unexpected key")
                }
            }
        }
    }
    #[test]
    fn set_instrumentation_scope() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        let res = Python::with_gil(|py| -> PyResult<()> {
            run_script("set_instrumentation_scope_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let scope_spans = scope_spans.pop().unwrap();
        let scope = scope_spans.scope.unwrap();
        print!("scope spans:\n{:?}", scope);
        assert_eq!("name_changed", scope.name);
        assert_eq!("0.0.2", scope.version);
        assert_eq!(100, scope.dropped_attributes_count);
        assert_eq!(scope.attributes.len(), 1);
        for attr in &scope.attributes {
            let value = attr.value.clone().unwrap();
            let value = value.value.unwrap();
            match attr.key.as_str() {
                "severity" => match value {
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => {
                        assert_eq!(s, "WARN");
                    }
                    _ => {
                        panic!("wrong type for severity: {:?}", value);
                    }
                },
                _ => {
                    panic!("unexpected key")
                }
            }
        }
    }

    #[test]
    fn read_and_write_spans() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("read_and_write_spans_test.py", py, py_resource_spans)
        })
        .unwrap();

        let resource = Arc::into_inner(resource_spans.resource);
        let resource = resource.unwrap().into_inner().unwrap().unwrap();
        let dropped = Arc::into_inner(resource.dropped_attributes_count);
        let dropped = dropped.unwrap().into_inner().unwrap();

        assert_eq!(15, dropped);

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        let mut span = scope_spans.spans.pop().unwrap();
        assert_eq!(b"5555555555".to_vec(), span.trace_id);
        assert_eq!(b"6666666666".to_vec(), span.span_id);
        assert_eq!("test=1234567890", span.trace_state);
        assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
        assert_eq!(1, span.flags);
        assert_eq!("py_processed_span", span.name);
        assert_eq!(4, span.kind);
        assert_eq!(1234567890, span.start_time_unix_nano);
        assert_eq!(1234567890, span.end_time_unix_nano);
        assert_eq!(100, span.dropped_attributes_count);
        assert_eq!(200, span.dropped_events_count);
        assert_eq!(300, span.dropped_links_count);
        assert_eq!("error message", span.status.clone().unwrap().message);
        assert_eq!(2, span.status.unwrap().code);
        assert_eq!(1, span.events.len());
        assert_eq!("py_processed_event", span.events[0].name);
        assert_eq!(1234567890, span.events[0].time_unix_nano);
        assert_eq!(400, span.events[0].dropped_attributes_count);
        assert_eq!(1, span.events[0].attributes.len());
        assert_eq!("event_attr_key", &span.events[0].attributes[0].key);
        let value = span.events[0].attributes[0]
            .value
            .clone()
            .unwrap()
            .value
            .unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("event_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }

        assert_eq!(2, span.links.len());
        // get the newly added link
        let new_link = span.links.remove(1);
        assert_eq!(b"88888888".to_vec(), new_link.trace_id);
        assert_eq!(b"99999999".to_vec(), new_link.span_id);
        assert_eq!("test=1234567890", new_link.trace_state);
        assert_eq!(300, new_link.dropped_attributes_count);
        assert_eq!(1, new_link.flags);
        assert_eq!(1, new_link.attributes.len());
        let value = new_link.attributes[0].value.clone().unwrap().value.unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("link_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }

        assert_eq!(3, span.attributes.len());
        let new_attr = span.attributes.remove(2);
        assert_eq!("span_attr_key", new_attr.key);
        let value = new_attr.value.clone().unwrap().value.unwrap();
        match value {
            Value::StringValue(s) => {
                assert_eq!("span_attr_value", s)
            }
            _ => panic!("unexpected type"),
        }
    }
    #[test]
    fn set_resource_spans_resource() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script(
                "write_resource_spans_resource_test.py",
                py,
                py_resource_spans,
            )
        })
        .unwrap();

        let resource = Arc::into_inner(resource_spans.resource).unwrap();
        let resource = resource.into_inner().unwrap().unwrap();
        let resource = crate::model::py_transform::transform_resource(resource).unwrap();
        assert_eq!(2, resource.attributes.len());
        assert_eq!(35, resource.dropped_attributes_count);
        for attr in &resource.attributes {
            match attr.key.as_str() {
                "key" => assert_eq!(
                    Value::StringValue("value".to_string()),
                    attr.value.clone().unwrap().value.unwrap()
                ),
                "boolean" => assert_eq!(
                    Value::BoolValue(true),
                    attr.value.clone().unwrap().value.unwrap()
                ),
                _ => panic!("unexpected attribute key"),
            }
        }
    }
    #[test]
    fn set_span_events() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_span_events_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        let span = scope_spans.spans.pop().unwrap();

        assert_eq!(2, span.events.len());
        let event = &span.events[0];
        assert_eq!("first_event", event.name);
        assert_eq!(123, event.time_unix_nano);
        assert_eq!(1, event.dropped_attributes_count);
        assert_eq!(1, event.attributes.len());
        let attr = &event.attributes[0];
        assert_eq!("first_event_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("first_event_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );

        let event = &span.events[1];
        assert_eq!("second_event", event.name);
        assert_eq!(456, event.time_unix_nano);
        assert_eq!(2, event.dropped_attributes_count);
        assert_eq!(1, event.attributes.len());
        let attr = &event.attributes[0];
        assert_eq!("second_event_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("second_event_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        )
    }
    #[test]
    fn set_scope_spans() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_scope_spans_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        assert_eq!(
            "https://github.com/streamfold/rotel",
            scope_spans.schema_url
        );
        let inst_scope = scope_spans.scope.unwrap();
        assert_eq!("rotel-sdk", inst_scope.name);
        assert_eq!("v1.0.0", inst_scope.version);
        let attr = &inst_scope.attributes[0];
        assert_eq!("rotel-sdk", attr.key);
        assert_eq!(
            Value::StringValue("v1.0.0".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );

        let span = scope_spans.spans.pop().unwrap();
        assert_eq!(b"5555555555".to_vec(), span.trace_id);
        assert_eq!(b"6666666666".to_vec(), span.span_id);
        assert_eq!("test=1234567890", span.trace_state);
        assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
        assert_eq!(1, span.flags);
        assert_eq!("py_processed_span", span.name);
        assert_eq!(4, span.kind);
        assert_eq!(1234567890, span.start_time_unix_nano);
        assert_eq!(1234567890, span.end_time_unix_nano);
        let attr = &span.attributes[0];
        assert_eq!("span_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("span_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );
    }

    #[test]
    fn set_spans() {
        initialize();
        let export_req = utilities::otlp::FakeOTLP::trace_service_request_with_spans(1, 1);
        let resource_spans =
            crate::model::otel_transform::transform(export_req.resource_spans[0].clone());
        let py_resource_spans = ResourceSpans {
            resource: resource_spans.resource.clone(),
            scope_spans: resource_spans.scope_spans.clone(),
            schema_url: resource_spans.schema_url,
        };
        Python::with_gil(|py| -> PyResult<()> {
            run_script("write_spans_test.py", py, py_resource_spans)
        })
        .unwrap();

        let scope_spans_vec = Arc::into_inner(resource_spans.scope_spans).unwrap();
        let scope_spans_vec = scope_spans_vec.into_inner().unwrap();

        let mut scope_spans = crate::model::py_transform::transform_spans(scope_spans_vec);
        let mut scope_spans = scope_spans.pop().unwrap();
        let span = scope_spans.spans.pop().unwrap();
        assert_eq!(b"5555555555".to_vec(), span.trace_id);
        assert_eq!(b"6666666666".to_vec(), span.span_id);
        assert_eq!("test=1234567890", span.trace_state);
        assert_eq!(b"7777777777".to_vec(), span.parent_span_id);
        assert_eq!(1, span.flags);
        assert_eq!("py_processed_span", span.name);
        assert_eq!(4, span.kind);
        assert_eq!(1234567890, span.start_time_unix_nano);
        assert_eq!(1234567890, span.end_time_unix_nano);
        let attr = &span.attributes[0];
        assert_eq!("span_attr_key", attr.key);
        assert_eq!(
            Value::StringValue("span_attr_value".to_string()),
            attr.value.clone().unwrap().value.unwrap()
        );
    }
}
