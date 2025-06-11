use crate::model::common::RKeyValue;
use crate::py::common::KeyValue;
use crate::py::handle_poison_error;
use pyo3::{pyclass, pymethods, Py, PyErr, PyRef, PyRefMut, PyResult, Python};
use std::sync::{Arc, Mutex};
use std::vec;

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
    fn set_attributes(&mut self, new_value: Vec<KeyValue>) -> PyResult<()> {
        let mut attrs = self.attributes.lock().map_err(handle_poison_error)?;
        attrs.clear();
        for kv in new_value.iter() {
            attrs.push(kv.inner.clone());
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
pub struct Attributes(Arc<Mutex<Vec<Arc<Mutex<RKeyValue>>>>>);

#[pymethods]
impl Attributes {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Attributes(Arc::new(Mutex::new(vec![]))))
    }

    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<AttributesIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = AttributesIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<KeyValue> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(KeyValue {
                inner: item.clone(),
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
    fn append<'py>(&self, item: &KeyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(item.inner.clone());
        Ok(())
    }

    fn append_attributes(&self, items: Vec<KeyValue>) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;

        for kv in items.iter() {
            k.push(kv.inner.clone());
        }
        Ok(())
    }

    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
}

#[pyclass]
pub struct AttributesIter {
    inner: vec::IntoIter<Arc<Mutex<RKeyValue>>>,
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

// TODO: Remove this and update PyAttributesIter to use a Arc<Mutex<Vec<KeyValue>>>.
// Careful observer will notice this looks like PyAttributes called from PyResource, however
// that class has additional ArcMutexes around the KeyValues. We tried out a new pattern here for the scope attributes
// and it appears to be working well. For the sake of safety I want to finish additional testing before going back and
// refactoring. WHen we do we should be able to remove this and share a single attributes and attributes iter type.
#[pyclass]
pub struct AttributesList(pub Arc<Mutex<Vec<RKeyValue>>>);

#[pymethods]
impl AttributesList {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(AttributesList(Arc::new(Mutex::new(vec![]))))
    }
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<AttributesListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = AttributesListIter {
            inner: inner.clone().into_iter(),
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
        inner[index] = RKeyValue {
            key: v.key.clone(),
            value: v.value.clone(),
        };
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
    fn append<'py>(&self, item: &KeyValue) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        let inner = item.inner.lock().unwrap();
        let inner = inner.clone();
        k.push(inner);
        Ok(())
    }
    fn append_attributes(&self, items: Vec<KeyValue>) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        for kv in items.iter() {
            let inner = kv.inner.lock().unwrap();
            let inner = inner.clone();
            k.push(inner.clone());
        }
        Ok(())
    }
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
}

#[pyclass]
struct AttributesListIter {
    inner: vec::IntoIter<RKeyValue>,
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
