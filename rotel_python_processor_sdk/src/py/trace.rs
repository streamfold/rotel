use crate::model::common::RInstrumentationScope;
use crate::model::otel_transform::convert_attributes;
use crate::model::resource::RResource;
use crate::model::trace::{REvent, RLink, RScopeSpans, RSpan, RStatus};
use crate::py::{handle_poison_error, AttributesList, InstrumentationScope, Resource};
use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyclass, pymethods, Py, PyErr, PyRef, PyRefMut, PyResult, Python};
use std::sync::{Arc, Mutex};
use std::vec;

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
        let v = self.resource.lock().map_err(handle_poison_error)?;
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
        let mut inner = self.resource.lock().map_err(handle_poison_error)?;
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
pub struct ScopeSpansList(Arc<Mutex<Vec<Arc<Mutex<RScopeSpans>>>>>);

#[pymethods]
impl ScopeSpansList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<ScopeSpansListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = ScopeSpansListIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<ScopeSpans> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
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
    fn __setitem__(&self, index: usize, value: &ScopeSpans) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        inner[index] = Arc::new(Mutex::new(RScopeSpans {
            scope: value.scope.clone(),
            spans: value.spans.clone(),
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
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
}

#[pyclass]
pub struct ScopeSpansListIter {
    inner: vec::IntoIter<Arc<Mutex<RScopeSpans>>>,
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
pub struct ScopeSpans {
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
            schema_url: String::new(),
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
pub struct Spans(Arc<Mutex<Vec<Arc<Mutex<RSpan>>>>>);

#[pymethods]
impl Spans {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<SpansIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = SpansIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }
    fn __getitem__(&self, index: usize) -> PyResult<Span> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(Span {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &Span) -> PyResult<()> {
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
    fn append(&self, item: &Span) -> PyResult<()> {
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
pub struct SpansIter {
    inner: vec::IntoIter<Arc<Mutex<RSpan>>>,
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
pub struct Span {
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
    fn set_span_id(&self, new_value: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.span_id = new_value;
        Ok(())
    }
    #[getter]
    fn trace_state(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.trace_state.clone())
    }
    #[setter]
    fn set_trace_state(&self, new_value: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.trace_state = new_value;
        Ok(())
    }
    #[getter]
    fn parent_span_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.parent_span_id.clone())
    }
    #[setter]
    fn set_parent_span_id(&self, new_value: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.parent_span_id = new_value;
        Ok(())
    }
    #[getter]
    fn flags(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.flags)
    }
    #[setter]
    fn set_flags(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.flags = new_value;
        Ok(())
    }
    #[getter]
    fn name(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.name.clone())
    }
    #[setter]
    fn set_name(&self, new_value: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.name = new_value;
        Ok(())
    }
    #[getter]
    fn kind(&self) -> PyResult<i32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.kind)
    }
    #[setter]
    fn set_kind(&self, new_value: i32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.kind = new_value;
        Ok(())
    }
    #[getter]
    fn start_time_unix_nano(&self) -> PyResult<u64> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.start_time_unix_nano)
    }
    #[setter]
    fn set_start_time_unix_nano(&self, new_value: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.start_time_unix_nano = new_value;
        Ok(())
    }
    #[getter]
    fn end_time_unix_nano(&self) -> PyResult<u64> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.end_time_unix_nano)
    }
    #[setter]
    fn set_end_time_unix_nano(&self, new_value: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.end_time_unix_nano = new_value;
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
    fn set_attributes(&mut self, attrs: &AttributesList) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.dropped_attributes_count)
    }
    #[setter]
    fn set_dropped_attributes_count(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.dropped_attributes_count = new_value;
        Ok(())
    }
    #[getter]
    fn events(&self) -> PyResult<Events> {
        let mut v = self.inner.lock().unwrap();
        if v.events_arc.is_some() {
            let arc = v.events_arc.take().unwrap();
            let arc_clone = arc.clone();
            v.events_arc.replace(arc);
            return Ok(Events(arc_clone));
        }
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
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        let mut new_events = Vec::with_capacity(events.len());
        for event in events {
            new_events.push(event.inner);
        }
        v.events_arc.replace(Arc::new(Mutex::new(new_events)));
        Ok(())
    }
    #[getter]
    fn dropped_events_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.dropped_events_count)
    }
    #[setter]
    fn set_dropped_events_count(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.dropped_events_count = new_value;
        Ok(())
    }
    #[getter]
    fn links(&self) -> PyResult<Links> {
        let mut v = self.inner.lock().unwrap();
        if v.links_arc.is_some() {
            let arc = v.links_arc.take().unwrap();
            let arc_clone = arc.clone();
            v.links_arc.replace(arc);
            return Ok(Links(arc_clone));
        }
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
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        let mut new_links = Vec::with_capacity(links.len());
        for link in links {
            new_links.push(link.inner);
        }
        v.links_arc.replace(Arc::new(Mutex::new(new_links)));
        Ok(())
    }
    #[getter]
    fn dropped_links_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.dropped_links_count)
    }
    #[setter]
    fn set_dropped_links_count(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.dropped_links_count = new_value;
        Ok(())
    }
    #[getter]
    fn status(&self) -> PyResult<Option<Status>> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        {
            let status = v.status.lock().map_err(handle_poison_error)?;
            if status.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(Status(v.status.clone())))
    }
    #[setter]
    fn set_status(&self, status: Status) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
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
pub struct Events(Arc<Mutex<Vec<Arc<Mutex<REvent>>>>>);

#[pymethods]
impl Events {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<EventsIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = EventsIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }
    fn __getitem__(&self, index: usize) -> PyResult<Event> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(Event {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &Event) -> PyResult<()> {
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
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
    fn append(&self, item: &Event) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(item.inner.clone());
        Ok(())
    }
}

#[pyclass]
pub struct EventsIter {
    inner: vec::IntoIter<Arc<Mutex<REvent>>>,
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
pub struct Event {
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.time_unix_nano)
    }
    #[setter]
    fn set_time_unix_nano(&mut self, unix_nano: u64) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.time_unix_nano = unix_nano;
        Ok(())
    }
    #[getter]
    fn name(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.name.clone())
    }
    #[setter]
    fn set_name(&mut self, name: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.name = name;
        Ok(())
    }
    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let binding = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AttributesList(binding.attributes.clone()))
    }
    #[setter]
    fn set_attributes(&mut self, attrs: &AttributesList) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.attributes.lock().map_err(handle_poison_error)?;
        v.clear();
        for kv in attrs.0.lock().unwrap().iter() {
            v.push(kv.clone())
        }
        Ok(())
    }
    #[getter]
    fn dropped_attributes_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.dropped_attributes_count)
    }
    #[setter]
    fn set_dropped_attributes_count(&mut self, count: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.dropped_attributes_count = count;
        Ok(())
    }
}

#[pyclass]
pub struct Links(Arc<Mutex<Vec<Arc<Mutex<RLink>>>>>);

#[pymethods]
impl Links {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<LinksIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = LinksIter {
            inner: inner.clone().into_iter(),
        };
        // Convert to a Python-managed object
        Py::new(py, iter)
    }
    fn __getitem__(&self, index: usize) -> PyResult<Link> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(Link {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &Link) -> PyResult<()> {
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
    fn append<'py>(&self, item: &Link) -> PyResult<()> {
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
pub struct LinksIter {
    inner: vec::IntoIter<Arc<Mutex<RLink>>>,
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
pub struct Link {
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
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.trace_id.clone())
    }
    #[setter]
    fn set_trace_id(&mut self, trace_id: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.trace_id = trace_id;
        Ok(())
    }
    #[getter]
    fn span_id(&self) -> PyResult<Vec<u8>> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.span_id.clone())
    }
    #[setter]
    fn set_span_id(&mut self, span_id: Vec<u8>) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.span_id = span_id;
        Ok(())
    }
    #[getter]
    fn trace_state(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.trace_state.clone())
    }
    #[setter]
    fn set_trace_state(&mut self, trace_state: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.trace_state = trace_state;
        Ok(())
    }
    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let binding = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AttributesList(binding.attributes.clone()))
    }
    #[setter]
    fn set_attributes(&mut self, attrs: &AttributesList) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.attributes.lock().map_err(handle_poison_error)?;
        v.clear();
        for kv in attrs.0.lock().unwrap().iter() {
            v.push(kv.clone())
        }
        Ok(())
    }
    #[getter]
    fn dropped_attributes_count(&self) -> PyResult<u32> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.dropped_attributes_count)
    }
    #[setter]
    fn set_dropped_attributes_count(&self, new_value: u32) -> PyResult<()> {
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
    fn set_flags(&self, new_value: u32) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.flags = new_value;
        Ok(())
    }
}

#[pyclass]
#[derive(Clone)]
pub struct Status(Arc<Mutex<Option<RStatus>>>);

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
        let binding = self.0.lock().map_err(handle_poison_error)?;
        let v = binding
            .clone()
            .ok_or(PyErr::new::<PyRuntimeError, _>("Status is None"))?;
        Ok(v.message)
    }
    #[setter]
    fn set_message(&mut self, message: String) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(handle_poison_error)?;
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
        let binding = self.0.lock().map_err(handle_poison_error)?;
        let v = binding
            .clone()
            .ok_or(PyErr::new::<PyRuntimeError, _>("Status is None"))?;
        Ok(v.code)
    }
    #[setter]
    fn set_code(&mut self, code: StatusCode) -> PyResult<()> {
        let mut binding = self.0.lock().map_err(handle_poison_error)?;
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
