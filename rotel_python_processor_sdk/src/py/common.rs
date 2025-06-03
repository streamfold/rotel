use crate::model::common::RValue::{
    BoolValue, BytesValue, DoubleValue, IntValue, KvListValue, RVArrayValue, StringValue,
};
use crate::model::common::{
    RAnyValue, RArrayValue, RInstrumentationScope, RKeyValue, RKeyValueList,
};
use crate::model::otel_transform::convert_attributes;
use crate::py::{handle_poison_error, AttributesList};
use pyo3::exceptions::PyRuntimeError;
#[allow(deprecated)]
use pyo3::{pyclass, pymethods, IntoPy, Py, PyErr, PyObject, PyRef, PyRefMut, PyResult, Python};
use std::sync::{Arc, Mutex};

// Wrapper for AnyValue that can be exposed to Python
#[pyclass]
pub struct AnyValue {
    pub inner: Arc<Mutex<Option<RAnyValue>>>,
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
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
        let v = self.inner.lock().map_err(handle_poison_error)?;

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
        let v = self.inner.lock().map_err(handle_poison_error)?;
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(RVArrayValue(RArrayValue {
                values: new_value.0.clone(),
            }));
        Ok(())
    }
    #[setter]
    fn set_key_value_list_value(&mut self, new_value: KeyValueList) -> PyResult<()> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        v.clone()
            .unwrap()
            .value
            .lock()
            .unwrap()
            .replace(KvListValue(RKeyValueList {
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
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = ArrayValueIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
    fn __getitem__(&self, index: usize) -> PyResult<AnyValue> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(AnyValue {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &AnyValue) -> PyResult<()> {
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
    fn append(&self, item: &AnyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(item.inner.clone());
        Ok(())
    }
}

#[pyclass]
pub struct ArrayValueIter {
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
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(KeyValue {
                inner: Arc::new(Mutex::new(item.clone())),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &KeyValue) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        let v = value.inner.lock().unwrap();
        inner[index] = v.clone();
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
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
    fn append(&self, item: KeyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        let inner = item.inner.lock().unwrap();
        let inner = inner.clone();
        k.push(inner);
        Ok(())
    }
}

#[pyclass]
pub struct KeyValueListIter {
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
pub struct KeyValue {
    pub inner: Arc<Mutex<RKeyValue>>,
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
            value: Arc::new(Mutex::new(Some(RVArrayValue(RArrayValue {
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
            value: Arc::new(Mutex::new(Some(KvListValue(RKeyValueList {
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
        let binding = v.key.clone();
        let bind_lock = binding.lock();
        let x = Ok(bind_lock.unwrap().clone().into_py(py));
        x
    }
    #[setter]
    fn set_key(&mut self, new_value: &str) -> PyResult<()> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        let binding = v.key.clone();
        let mut bind_lock = binding.lock().unwrap();
        bind_lock.clear();
        bind_lock.insert_str(0, new_value);
        Ok(())
    }
    #[getter]
    fn value(&self) -> PyResult<AnyValue> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        let binding = v.value.clone();
        Ok(AnyValue {
            inner: binding.clone(),
        })
    }
    #[setter]
    fn set_value(&mut self, new_value: &AnyValue) -> PyResult<()> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
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
pub struct InstrumentationScope(pub Arc<Mutex<Option<RInstrumentationScope>>>);

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
        let binding = self.0.lock().map_err(handle_poison_error)?;
        let v = binding.clone().ok_or(PyErr::new::<PyRuntimeError, _>(
            "InstrumentationScope is None",
        ))?;
        Ok(v.name)
    }
    #[setter]
    fn set_name(&self, name: String) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(handle_poison_error)?;
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
        let binding = self.0.lock().map_err(handle_poison_error)?;
        let v = binding.clone().ok_or(PyErr::new::<PyRuntimeError, _>(
            "InstrumentationScope is None",
        ))?;
        Ok(v.version)
    }
    #[setter]
    fn set_version(&self, version: String) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(handle_poison_error)?;
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
        let mut binding = self.0.lock().map_err(handle_poison_error)?;
        if binding.is_none() {
            PyErr::new::<PyRuntimeError, _>(
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
        let binding = self.0.lock().map_err(handle_poison_error)?;
        let v = binding.clone().ok_or(PyErr::new::<PyRuntimeError, _>(
            "InstrumentationScope is None",
        ))?;
        Ok(v.dropped_attributes_count)
    }
    #[setter]
    fn set_dropped_attributes_count(&self, dropped_attributes_count: u32) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(handle_poison_error)?;
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
