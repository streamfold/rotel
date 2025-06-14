// metrics.rs

use crate::model::common::{RInstrumentationScope, RKeyValue};
use crate::model::resource::RResource;
use crate::py::common::KeyValue;
use crate::py::{handle_poison_error, AttributesList, InstrumentationScope, Resource};
use pyo3::{pyclass, pymethods, Py, PyErr, PyRef, PyRefMut, PyResult, Python};
use std::sync::{Arc, Mutex};
use std::vec;

use crate::model::metrics::{
    RExemplar, RExemplarValue, RExponentialHistogram, RExponentialHistogramBuckets,
    RExponentialHistogramDataPoint, RGauge, RHistogram, RHistogramDataPoint, RMetric, RMetricData,
    RNumberDataPoint, RNumberDataPointValue, RScopeMetrics, RSum, RSummary, RSummaryDataPoint,
    RValueAtQuantile,
};

// --- PyO3 Bindings for RResourceMetrics ---
#[pyclass]
#[derive(Clone)]
pub struct ResourceMetrics {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_metrics: Arc<Mutex<Vec<Arc<Mutex<RScopeMetrics>>>>>,
    pub schema_url: String,
}

#[pymethods]
impl ResourceMetrics {
    #[getter]
    fn resource(&self) -> PyResult<Option<Resource>> {
        let v = self.resource.lock().map_err(handle_poison_error)?;
        if v.is_none() {
            return Ok(None);
        }
        let inner_resource = v.clone().unwrap();
        Ok(Some(Resource {
            attributes: inner_resource.attributes.clone(),
            dropped_attributes_count: inner_resource.dropped_attributes_count.clone(),
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
    fn scope_metrics(&self) -> PyResult<ScopeMetricsList> {
        Ok(ScopeMetricsList(self.scope_metrics.clone()))
    }

    #[setter]
    fn set_scope_metrics(&mut self, metrics: Vec<ScopeMetrics>) -> PyResult<()> {
        let mut inner = self.scope_metrics.lock().map_err(handle_poison_error)?;
        inner.clear();
        for sm in metrics {
            inner.push(Arc::new(Mutex::new(RScopeMetrics {
                scope: sm.scope.clone(),
                metrics: sm.metrics.clone(),
                schema_url: sm.schema_url.clone(),
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

// --- PyO3 Bindings for ScopeMetricsList (for iterating and accessing) ---
#[pyclass]
pub struct ScopeMetricsList(Arc<Mutex<Vec<Arc<Mutex<RScopeMetrics>>>>>);

#[pymethods]
impl ScopeMetricsList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<ScopeMetricsListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = ScopeMetricsListIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<ScopeMetrics> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => {
                let item = item.lock().unwrap();
                Ok(ScopeMetrics {
                    scope: item.scope.clone(),
                    metrics: item.metrics.clone(),
                    schema_url: item.schema_url.clone(),
                })
            }
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &ScopeMetrics) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        inner[index] = Arc::new(Mutex::new(RScopeMetrics {
            scope: value.scope.clone(),
            metrics: value.metrics.clone(),
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
    fn append(&self, item: &ScopeMetrics) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(Arc::new(Mutex::new(RScopeMetrics {
            scope: item.scope.clone(),
            metrics: item.metrics.clone(),
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
pub struct ScopeMetricsListIter {
    inner: vec::IntoIter<Arc<Mutex<RScopeMetrics>>>,
}

#[pymethods]
impl ScopeMetricsListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<ScopeMetrics>> {
        let item = slf.inner.next();
        if item.is_none() {
            return Ok(None);
        }
        let inner = item.unwrap();
        let inner = inner.lock().unwrap();
        Ok(Some(ScopeMetrics {
            scope: inner.scope.clone(),
            metrics: inner.metrics.clone(),
            schema_url: inner.schema_url.clone(),
        }))
    }
}

// --- PyO3 Bindings for RScopeMetrics ---
#[pyclass]
#[derive(Clone)]
pub struct ScopeMetrics {
    pub scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    pub metrics: Arc<Mutex<Vec<Arc<Mutex<RMetric>>>>>,
    pub schema_url: Arc<Mutex<String>>,
}

#[pymethods]
impl ScopeMetrics {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ScopeMetrics {
            scope: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(vec![])),
            schema_url: Arc::new(Mutex::new(String::new())),
        })
    }

    #[getter]
    fn scope(&self) -> PyResult<Option<InstrumentationScope>> {
        let v = self.scope.lock().map_err(handle_poison_error)?;
        if v.is_none() {
            return Ok(None);
        }
        Ok(Some(InstrumentationScope(self.scope.clone())))
    }

    // TODO - Fix look at trace.rs
    #[setter]
    fn set_scope(&mut self, scope: Option<InstrumentationScope>) -> PyResult<()> {
        let mut v = self.scope.lock().map_err(handle_poison_error)?;
        if let Some(s) = scope {
            let inner_s = s.0.lock().map_err(handle_poison_error)?;
            *v = inner_s.clone();
        } else {
            *v = None;
        }
        Ok(())
    }

    #[getter]
    fn metrics(&self) -> PyResult<MetricsList> {
        Ok(MetricsList(self.metrics.clone()))
    }

    #[setter]
    fn set_metrics(&mut self, metrics: Vec<Metric>) -> PyResult<()> {
        let mut inner = self.metrics.lock().map_err(handle_poison_error)?;
        inner.clear();
        for m in metrics {
            inner.push(m.inner.clone());
        }
        Ok(())
    }

    #[getter]
    fn schema_url(&self) -> PyResult<String> {
        let guard = self.schema_url.lock().map_err(handle_poison_error)?;
        Ok(guard.clone())
    }

    #[setter]
    fn set_schema_url(&mut self, schema_url: String) -> PyResult<()> {
        let mut guard = self.schema_url.lock().map_err(handle_poison_error)?;
        *guard = schema_url;
        Ok(())
    }
}

// --- PyO3 Bindings for MetricsList ---
#[pyclass]
pub struct MetricsList(Arc<Mutex<Vec<Arc<Mutex<RMetric>>>>>);

#[pymethods]
impl MetricsList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<MetricsListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = MetricsListIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<Metric> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(Metric {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &Metric) -> PyResult<()> {
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
    fn append(&self, item: &Metric) -> PyResult<()> {
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
pub struct MetricsListIter {
    inner: vec::IntoIter<Arc<Mutex<RMetric>>>,
}

#[pymethods]
impl MetricsListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Metric>> {
        let item = slf.inner.next();
        if item.is_none() {
            return Ok(None);
        }
        let inner = item.unwrap();
        Ok(Some(Metric {
            inner: inner.clone(),
        }))
    }
}

// --- PyO3 Bindings for RMetric ---
#[pyclass]
#[derive(Clone)]
pub struct Metric {
    inner: Arc<Mutex<RMetric>>,
}

#[pymethods]
impl Metric {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Metric {
            inner: Arc::new(Mutex::new(RMetric {
                name: String::new(),
                description: String::new(),
                unit: String::new(),
                metadata: Arc::new(Mutex::new(vec![])),
                data: Arc::new(Mutex::new(None)),
            })),
        })
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
    fn description(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.description.clone())
    }

    #[setter]
    fn set_description(&mut self, description: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.description = description;
        Ok(())
    }

    #[getter]
    fn unit(&self) -> PyResult<String> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.unit.clone())
    }

    #[setter]
    fn set_unit(&mut self, unit: String) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.unit = unit;
        Ok(())
    }

    #[getter]
    fn metadata(&self) -> PyResult<AttributesList> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AttributesList(v.metadata.clone()))
    }

    #[setter]
    fn set_metadata(&mut self, metadata: Vec<KeyValue>) -> PyResult<()> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        let mut inner = v.metadata.lock().map_err(handle_poison_error)?;
        inner.clear();
        for kv in metadata {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error).unwrap();
            inner.push(kv_lock.clone());
        }
        Ok(())
    }

    #[getter]
    fn data(&self) -> PyResult<Option<MetricData>> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        let mut data = v.data.lock().map_err(handle_poison_error)?;
        if data.is_none() {
            return Ok(None);
        }
        let d = data.take().unwrap();
        let x = d.clone();
        data.replace(x);
        match d {
            RMetricData::Gauge(g) => Ok(Some(MetricData::Gauge(Gauge {
                inner: Arc::new(Mutex::new(g)),
            }))),
            RMetricData::Sum(s) => Ok(Some(MetricData::Sum(Sum {
                inner: Arc::new(Mutex::new(s)),
            }))),
            RMetricData::Histogram(h) => Ok(Some(MetricData::Histogram(Histogram {
                inner: Arc::new(Mutex::new(h)),
            }))),
            RMetricData::ExponentialHistogram(e) => Ok(Some(MetricData::ExponentialHistogram(
                ExponentialHistogram {
                    inner: Arc::new(Mutex::new(e)),
                },
            ))),
            RMetricData::Summary(s) => Ok(Some(MetricData::Summary(Summary {
                inner: Arc::new(Mutex::new(s)),
            }))),
        }
    }

    #[setter]
    fn set_data(&mut self, data: Option<MetricData>) -> PyResult<()> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        let mut data_lock = v.data.lock().map_err(handle_poison_error)?;
        if data.is_none() {
            *data_lock = None
        } else {
            let new_data = data.unwrap();
            match new_data {
                MetricData::Gauge(g) => {
                    let v = g.inner.lock().map_err(handle_poison_error)?;
                    data_lock.replace(RMetricData::Gauge(v.clone()));
                }
                MetricData::Sum(s) => {
                    let v = s.inner.lock().map_err(handle_poison_error)?;
                    data_lock.replace(RMetricData::Sum(v.clone()));
                }
                MetricData::Histogram(h) => {
                    let v = h.inner.lock().map_err(handle_poison_error)?;
                    data_lock.replace(RMetricData::Histogram(v.clone()));
                }
                MetricData::ExponentialHistogram(e) => {
                    let v = e.inner.lock().map_err(handle_poison_error)?;
                    data_lock.replace(RMetricData::ExponentialHistogram(v.clone()));
                }
                MetricData::Summary(s) => {
                    let v = s.inner.lock().map_err(handle_poison_error)?;
                    data_lock.replace(RMetricData::Summary(v.clone()));
                }
            }
        }
        Ok(())
    }
}

// --- PyO3 Bindings for RMetricData (Enum) ---

#[pyclass]
#[derive(Clone)]
pub enum MetricData {
    Gauge(Gauge),
    Sum(Sum),
    Histogram(Histogram),
    ExponentialHistogram(ExponentialHistogram),
    Summary(Summary),
}

// --- PyO3 Bindings for RGauge ---
#[pyclass]
#[derive(Clone)]
pub struct Gauge {
    pub inner: Arc<Mutex<RGauge>>,
}

#[pymethods]
impl Gauge {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Gauge {
            inner: Arc::new(Mutex::new(RGauge {
                data_points: Arc::new(Default::default()),
            })),
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<NumberDataPointList> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(NumberDataPointList(v.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<NumberDataPoint>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.data_points.lock().map_err(handle_poison_error)?;
        v.clear();
        for dp in data_points {
            v.push(dp.inner.clone());
        }
        Ok(())
    }
}

// --- PyO3 Bindings for RSum ---
#[pyclass]
#[derive(Clone)]
pub struct Sum {
    inner: Arc<Mutex<RSum>>,
}

#[pymethods]
impl Sum {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Sum {
            inner: Arc::new(Mutex::new(RSum {
                data_points: Arc::new(Mutex::new(vec![])),
                aggregation_temporality: AggregationTemporality::Unspecified as i32,
                is_monotonic: false,
            })),
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<NumberDataPointList> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(NumberDataPointList(v.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<NumberDataPoint>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.data_points.lock().map_err(handle_poison_error)?;
        v.clear();
        for dp in data_points {
            v.push(dp.inner.clone());
        }
        Ok(())
    }

    #[getter]
    fn aggregation_temporality(&self) -> PyResult<AggregationTemporality> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AggregationTemporality::from(v.aggregation_temporality))
    }

    #[setter]
    fn set_aggregation_temporality(&mut self, temporality: AggregationTemporality) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.aggregation_temporality = i32::from(temporality);
        Ok(())
    }

    #[getter]
    fn is_monotonic(&self) -> PyResult<bool> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(v.is_monotonic)
    }

    #[setter]
    fn set_is_monotonic(&mut self, is_monotonic: bool) -> PyResult<()> {
        let mut v = self.inner.lock().map_err(handle_poison_error)?;
        v.is_monotonic = is_monotonic;
        Ok(())
    }
}

// --- PyO3 Bindings for RHistogram ---
#[pyclass]
#[derive(Clone)]
pub struct Histogram {
    inner: Arc<Mutex<RHistogram>>,
}

#[pymethods]
impl Histogram {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Histogram {
            inner: Arc::new(Mutex::new(RHistogram {
                data_points: Arc::new(Mutex::new(vec![])),
                aggregation_temporality: 0,
            })),
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<HistogramDataPointList> {
        let v = self.inner.lock().map_err(handle_poison_error)?;
        Ok(HistogramDataPointList(v.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<HistogramDataPoint>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.data_points.lock().map_err(handle_poison_error)?;
        v.clear();
        for dp in data_points {
            v.push(dp.inner.clone());
        }
        Ok(())
    }

    #[getter]
    fn aggregation_temporality(&self) -> PyResult<AggregationTemporality> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AggregationTemporality::from(inner.aggregation_temporality))
    }

    #[setter]
    fn set_aggregation_temporality(&mut self, temporality: AggregationTemporality) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.aggregation_temporality = i32::from(temporality);
        Ok(())
    }
}

// --- PyO3 Bindings for RExponentialHistogram ---
#[pyclass]
#[derive(Clone)]
pub struct ExponentialHistogram {
    inner: Arc<Mutex<RExponentialHistogram>>,
}

#[pymethods]
impl ExponentialHistogram {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ExponentialHistogram {
            inner: Arc::new(Mutex::new(RExponentialHistogram {
                data_points: Arc::new(Mutex::new(vec![])),
                aggregation_temporality: i32::from(AggregationTemporality::Unspecified),
            })),
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<ExponentialHistogramDataPointList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(ExponentialHistogramDataPointList(inner.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<ExponentialHistogramDataPoint>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.data_points.lock().map_err(handle_poison_error)?;
        v.clear();
        for dp in data_points {
            v.push(dp.inner.clone());
        }
        Ok(())
    }

    #[getter]
    fn aggregation_temporality(&self) -> PyResult<AggregationTemporality> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AggregationTemporality::from(inner.aggregation_temporality))
    }

    #[setter]
    fn set_aggregation_temporality(&mut self, temporality: AggregationTemporality) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.aggregation_temporality = i32::from(temporality);
        Ok(())
    }
}

// --- PyO3 Bindings for RSummary ---
#[pyclass]
#[derive(Clone)]
pub struct Summary {
    pub inner: Arc<Mutex<RSummary>>,
}

#[pymethods]
impl Summary {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Summary {
            inner: Arc::new(Mutex::new(RSummary {
                data_points: Arc::new(Mutex::new(vec![])),
            })),
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<SummaryDataPointList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(SummaryDataPointList(inner.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<SummaryDataPoint>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.data_points.lock().map_err(handle_poison_error)?;
        v.clear();
        for dp in data_points {
            v.push(Arc::new(Mutex::new(dp.into())));
        }
        Ok(())
    }
}

// --- PyO3 Bindings for NumberDataPointList ---
#[pyclass]
pub struct NumberDataPointList(Arc<Mutex<Vec<Arc<Mutex<RNumberDataPoint>>>>>);

#[pymethods]
impl NumberDataPointList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<NumberDataPointListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = NumberDataPointListIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<NumberDataPoint> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(NumberDataPoint {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &NumberDataPoint) -> PyResult<()> {
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
    fn append(&self, item: &NumberDataPoint) -> PyResult<()> {
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
pub struct NumberDataPointListIter {
    inner: vec::IntoIter<Arc<Mutex<RNumberDataPoint>>>,
}

#[pymethods]
impl NumberDataPointListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<NumberDataPoint>> {
        let item = slf.inner.next();
        if item.is_none() {
            return Ok(None);
        }
        let inner = item.unwrap();
        Ok(Some(NumberDataPoint {
            inner: inner.clone(),
        }))
    }
}

// --- PyO3 Bindings for RNumberDataPoint ---
#[pyclass]
#[derive(Clone)]
pub struct NumberDataPoint {
    inner: Arc<Mutex<RNumberDataPoint>>,
}

#[pymethods]
impl NumberDataPoint {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(NumberDataPoint {
            inner: Arc::new(Mutex::new(RNumberDataPoint {
                attributes: Arc::new(Default::default()),
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                exemplars: Arc::new(Default::default()),
                flags: 0,
                value: None,
            })),
        })
    }

    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AttributesList(inner.attributes.clone()))
    }

    #[setter]
    fn set_attributes(&mut self, attributes: Vec<KeyValue>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.attributes.lock().map_err(handle_poison_error)?;
        v.clear();
        for kv in attributes {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error).unwrap();
            v.push(kv_lock.clone());
        }
        Ok(())
    }

    #[getter]
    fn start_time_unix_nano(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.start_time_unix_nano)
    }

    #[setter]
    fn set_start_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.start_time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn time_unix_nano(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.time_unix_nano)
    }

    #[setter]
    fn set_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn exemplars(&self) -> PyResult<ExemplarList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(ExemplarList(inner.exemplars.clone()))
    }

    #[setter]
    fn set_exemplars(&mut self, exemplars: Vec<Exemplar>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.exemplars.lock().map_err(handle_poison_error)?;
        v.clear();
        for e in exemplars {
            v.push(e.inner.clone())
        }
        Ok(())
    }

    #[getter]
    fn flags(&self) -> PyResult<u32> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.flags)
    }

    #[setter]
    fn set_flags(&mut self, flags: u32) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.flags = flags;
        Ok(())
    }

    #[getter]
    fn value(&self) -> PyResult<Option<NumberDataPointValue>> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        if inner.value.is_none() {
            return Ok(None);
        }
        let v = inner.value.take().unwrap();
        let x = v.clone();
        inner.value.replace(x);
        match v {
            RNumberDataPointValue::AsDouble(f) => Ok(Some(NumberDataPointValue::AsDouble(f))),
            RNumberDataPointValue::AsInt(i) => Ok(Some(NumberDataPointValue::AsInt(i))),
        }
    }

    #[setter]
    fn set_value(&mut self, value: Option<NumberDataPointValue>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        if value.is_none() {
            inner.value = None
        }
        let x = value.unwrap();
        match x {
            NumberDataPointValue::AsDouble(f) => {
                inner.value.replace(RNumberDataPointValue::AsDouble(f))
            }
            NumberDataPointValue::AsInt(i) => inner.value.replace(RNumberDataPointValue::AsInt(i)),
        };
        Ok(())
    }
}

#[pyclass]
#[derive(Clone)]
pub enum NumberDataPointValue {
    AsDouble(f64),
    AsInt(i64),
}

// --- PyO3 Bindings for HistogramDataPointList ---
#[pyclass]
pub struct HistogramDataPointList(Arc<Mutex<Vec<Arc<Mutex<RHistogramDataPoint>>>>>);

#[pymethods]
impl HistogramDataPointList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<HistogramDataPointListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = HistogramDataPointListIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<HistogramDataPoint> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(HistogramDataPoint {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &HistogramDataPoint) -> PyResult<()> {
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
    fn append(&self, item: &HistogramDataPoint) -> PyResult<()> {
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
pub struct HistogramDataPointListIter {
    inner: vec::IntoIter<Arc<Mutex<RHistogramDataPoint>>>,
}

#[pymethods]
impl HistogramDataPointListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<HistogramDataPoint>> {
        let item = slf.inner.next();
        if item.is_none() {
            return Ok(None);
        }
        let inner = item.unwrap();
        Ok(Some(HistogramDataPoint {
            inner: inner.clone(),
        }))
    }
}

// --- PyO3 Bindings for RHistogramDataPoint ---
#[pyclass]
#[derive(Clone)]
pub struct HistogramDataPoint {
    inner: Arc<Mutex<RHistogramDataPoint>>,
}

#[pymethods]
impl HistogramDataPoint {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(HistogramDataPoint {
            inner: Arc::new(Mutex::new(RHistogramDataPoint {
                attributes: Arc::new(Default::default()),
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                count: 0,
                sum: None,
                bucket_counts: vec![],
                explicit_bounds: vec![],
                exemplars: Arc::new(Default::default()),
                flags: 0,
                min: None,
                max: None,
            })),
        })
    }

    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AttributesList(inner.attributes.clone()))
    }

    #[setter]
    fn set_attributes(&mut self, attributes: Vec<KeyValue>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.attributes.lock().map_err(handle_poison_error)?;
        v.clear();
        for kv in attributes {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error)?;
            v.push(kv_lock.clone());
        }
        Ok(())
    }

    #[getter]
    fn start_time_unix_nano(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.start_time_unix_nano)
    }

    #[setter]
    fn set_start_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.start_time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn time_unix_nano(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.time_unix_nano)
    }

    #[setter]
    fn set_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn count(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.count)
    }

    #[setter]
    fn set_count(&mut self, count: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.count = count;
        Ok(())
    }

    #[getter]
    fn sum(&self) -> PyResult<Option<f64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.sum)
    }

    #[setter]
    fn set_sum(&mut self, sum: Option<f64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.sum = sum;
        Ok(())
    }

    #[getter]
    fn bucket_counts(&self) -> PyResult<Vec<u64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.bucket_counts.clone())
    }

    #[setter]
    fn set_bucket_counts(&mut self, bucket_counts: Vec<u64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.bucket_counts = bucket_counts;
        Ok(())
    }

    #[getter]
    fn explicit_bounds(&self) -> PyResult<Vec<f64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.explicit_bounds.clone())
    }

    #[setter]
    fn set_explicit_bounds(&mut self, explicit_bounds: Vec<f64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.explicit_bounds = explicit_bounds;
        Ok(())
    }

    #[getter]
    fn exemplars(&self) -> PyResult<ExemplarList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(ExemplarList(inner.exemplars.clone()))
    }

    #[setter]
    fn set_exemplars(&mut self, exemplars: Vec<Exemplar>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.exemplars.lock().map_err(handle_poison_error)?;
        v.clear();
        for e in exemplars {
            v.push(e.inner.clone());
        }
        Ok(())
    }

    #[getter]
    fn flags(&self) -> PyResult<u32> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.flags)
    }

    #[setter]
    fn set_flags(&mut self, flags: u32) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.flags = flags;
        Ok(())
    }

    #[getter]
    fn min(&self) -> PyResult<Option<f64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.min)
    }

    #[setter]
    fn set_min(&mut self, min: Option<f64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.min = min;
        Ok(())
    }

    #[getter]
    fn max(&self) -> PyResult<Option<f64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.max)
    }

    #[setter]
    fn set_max(&mut self, max: Option<f64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.max = max;
        Ok(())
    }
}

// --- PyO3 Bindings for ExponentialHistogramDataPointList ---
#[pyclass]
pub struct ExponentialHistogramDataPointList(
    Arc<Mutex<Vec<Arc<Mutex<RExponentialHistogramDataPoint>>>>>,
);

#[pymethods]
impl ExponentialHistogramDataPointList {
    fn __iter__<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Py<ExponentialHistogramDataPointListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = ExponentialHistogramDataPointListIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<ExponentialHistogramDataPoint> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(ExponentialHistogramDataPoint {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &ExponentialHistogramDataPoint) -> PyResult<()> {
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
    fn append(&self, item: &ExponentialHistogramDataPoint) -> PyResult<()> {
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
pub struct ExponentialHistogramDataPointListIter {
    inner: vec::IntoIter<Arc<Mutex<RExponentialHistogramDataPoint>>>,
}

#[pymethods]
impl ExponentialHistogramDataPointListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<ExponentialHistogramDataPoint>> {
        let item = slf.inner.next();
        if item.is_none() {
            return Ok(None);
        }
        let inner = item.unwrap();
        Ok(Some(ExponentialHistogramDataPoint {
            inner: inner.clone(),
        }))
    }
}

// --- PyO3 Bindings for RExponentialHistogramDataPoint ---
#[pyclass]
#[derive(Clone)]
pub struct ExponentialHistogramDataPoint {
    inner: Arc<Mutex<RExponentialHistogramDataPoint>>,
}

#[pymethods]
impl ExponentialHistogramDataPoint {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ExponentialHistogramDataPoint {
            inner: Arc::new(Mutex::new(RExponentialHistogramDataPoint {
                attributes: Arc::new(Default::default()),
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                count: 0,
                sum: None,
                scale: 0,
                zero_count: 0,
                positive: Arc::new(Mutex::new(None)),
                negative: Arc::new(Mutex::new(None)),
                flags: 0,
                exemplars: Arc::new(Default::default()),
                min: None,
                max: None,
                zero_threshold: 0.0,
            })),
        })
    }

    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AttributesList(inner.attributes.clone()))
    }

    #[setter]
    fn set_attributes(&mut self, attributes: Vec<KeyValue>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.attributes.lock().map_err(handle_poison_error)?;
        v.clear();
        for kv in attributes {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error)?;
            v.push(kv_lock.clone());
        }
        Ok(())
    }

    #[getter]
    fn start_time_unix_nano(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.start_time_unix_nano)
    }

    #[setter]
    fn set_start_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.start_time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn time_unix_nano(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.time_unix_nano)
    }

    #[setter]
    fn set_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn count(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.count)
    }

    #[setter]
    fn set_count(&mut self, count: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.count = count;
        Ok(())
    }

    #[getter]
    fn sum(&self) -> PyResult<Option<f64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.sum)
    }

    #[setter]
    fn set_sum(&mut self, sum: Option<f64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.sum = sum;
        Ok(())
    }

    #[getter]
    fn scale(&self) -> PyResult<i32> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.scale)
    }

    #[setter]
    fn set_scale(&mut self, scale: i32) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.scale = scale;
        Ok(())
    }

    #[getter]
    fn zero_count(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.zero_count)
    }

    #[setter]
    fn set_zero_count(&mut self, count: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.zero_count = count;
        Ok(())
    }

    #[getter]
    fn positive(&self) -> PyResult<Option<ExponentialHistogramBuckets>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.positive.lock().map_err(handle_poison_error)?;
        if v.is_none() {
            return Ok(None);
        }
        let x = v.take().unwrap();
        let y = x.clone();
        v.replace(x);
        Ok(Some(ExponentialHistogramBuckets { inner: y }))
    }

    #[setter]
    fn set_positive(&mut self, buckets: Option<ExponentialHistogramBuckets>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.positive.lock().map_err(handle_poison_error)?;
        if buckets.is_none() {
            *v = None;
            return Ok(());
        }
        let ehb = buckets.unwrap();
        v.replace(ehb.inner.clone());
        Ok(())
    }

    #[getter]
    fn negative(&self) -> PyResult<Option<ExponentialHistogramBuckets>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.negative.lock().map_err(handle_poison_error)?;
        if v.is_none() {
            return Ok(None);
        }
        let x = v.take().unwrap();
        let y = x.clone();
        v.replace(x);
        Ok(Some(ExponentialHistogramBuckets { inner: y }))
    }

    #[setter]
    fn set_negative(&mut self, buckets: Option<ExponentialHistogramBuckets>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.negative.lock().map_err(handle_poison_error)?;
        if buckets.is_none() {
            *v = None;
            return Ok(());
        }
        let ehb = buckets.unwrap();
        v.replace(ehb.inner.clone());
        Ok(())
    }

    #[getter]
    fn flags(&self) -> PyResult<u32> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.flags)
    }

    #[setter]
    fn set_flags(&mut self, flags: u32) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.flags = flags;
        Ok(())
    }

    #[getter]
    fn exemplars(&self) -> PyResult<ExemplarList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(ExemplarList(inner.exemplars.clone()))
    }

    #[setter]
    fn set_exemplars(&mut self, exemplars: Vec<Exemplar>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner.exemplars.lock().map_err(handle_poison_error)?;
        v.clear();
        for e in exemplars {
            v.push(e.inner.clone());
        }
        Ok(())
    }

    #[getter]
    fn min(&self) -> PyResult<Option<f64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.min)
    }

    #[setter]
    fn set_min(&mut self, min: Option<f64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.min = min;
        Ok(())
    }

    #[getter]
    fn max(&self) -> PyResult<Option<f64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.max)
    }

    #[setter]
    fn set_max(&mut self, max: Option<f64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.max = max;
        Ok(())
    }

    #[getter]
    fn zero_threshold(&self) -> PyResult<f64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.zero_threshold)
    }

    #[setter]
    fn set_zero_threshold(&mut self, threshold: f64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.zero_threshold = threshold;
        Ok(())
    }
}

// --- PyO3 Bindings for RExponentialHistogramBuckets ---
#[pyclass]
#[derive(Clone)]
pub struct ExponentialHistogramBuckets {
    inner: Arc<Mutex<RExponentialHistogramBuckets>>,
}

#[pymethods]
impl ExponentialHistogramBuckets {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ExponentialHistogramBuckets {
            inner: Arc::new(Mutex::new(RExponentialHistogramBuckets {
                offset: 0,
                bucket_counts: vec![],
            })),
        })
    }

    #[getter]
    fn offset(&self) -> PyResult<i32> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.offset)
    }

    #[setter]
    fn set_offset(&mut self, offset: i32) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.offset = offset;
        Ok(())
    }

    #[getter]
    fn bucket_counts(&self) -> PyResult<Vec<u64>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.bucket_counts.clone())
    }

    #[setter]
    fn set_bucket_counts(&mut self, bucket_counts: Vec<u64>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.bucket_counts = bucket_counts;
        Ok(())
    }
}

// --- PyO3 Bindings for SummaryDataPointList ---
#[pyclass]
pub struct SummaryDataPointList(Arc<Mutex<Vec<Arc<Mutex<RSummaryDataPoint>>>>>);

#[pymethods]
impl SummaryDataPointList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<SummaryDataPointListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = SummaryDataPointListIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<SummaryDataPoint> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => {
                let item_lock = item.lock().unwrap();
                Ok(SummaryDataPoint {
                    attributes: item_lock.attributes.clone(),
                    start_time_unix_nano: item_lock.start_time_unix_nano,
                    time_unix_nano: item_lock.time_unix_nano,
                    count: item_lock.count,
                    sum: item_lock.sum,
                    quantile_values: item_lock
                        .quantile_values
                        .clone()
                        .into_iter()
                        .map(|v| v.into())
                        .collect(),
                    flags: item_lock.flags,
                })
            }
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &SummaryDataPoint) -> PyResult<()> {
        let mut inner = self.0.lock().map_err(handle_poison_error)?;
        if index >= inner.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            ));
        }
        inner[index] = Arc::new(Mutex::new(value.to_owned().into()));
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
    fn append(&self, item: &SummaryDataPoint) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(Arc::new(Mutex::new(item.to_owned().into())));
        Ok(())
    }
    fn __len__(&self) -> PyResult<usize> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        Ok(inner.len())
    }
}

#[pyclass]
pub struct SummaryDataPointListIter {
    inner: vec::IntoIter<Arc<Mutex<RSummaryDataPoint>>>,
}

#[pymethods]
impl SummaryDataPointListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<SummaryDataPoint>> {
        let item = slf.inner.next();
        if item.is_none() {
            return Ok(None);
        }
        let inner = item.unwrap();
        let inner = inner.lock().unwrap();
        Ok(Some(SummaryDataPoint {
            attributes: inner.attributes.clone(),
            start_time_unix_nano: inner.start_time_unix_nano,
            time_unix_nano: inner.time_unix_nano,
            count: inner.count,
            sum: inner.sum,
            quantile_values: inner
                .quantile_values
                .clone()
                .into_iter()
                .map(|v| v.into())
                .collect(),
            flags: inner.flags,
        }))
    }
}

// --- PyO3 Bindings for RSummaryDataPoint ---
#[pyclass]
#[derive(Clone)]
pub struct SummaryDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: f64,
    pub quantile_values: Vec<ValueAtQuantile>,
    pub flags: u32,
}

#[pymethods]
impl SummaryDataPoint {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(SummaryDataPoint {
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            count: 0,
            sum: 0.0,
            quantile_values: vec![],
            flags: 0,
        })
    }

    #[getter]
    fn attributes(&self) -> PyResult<AttributesList> {
        Ok(AttributesList(self.attributes.clone()))
    }

    #[setter]
    fn set_attributes(&mut self, attributes: Vec<KeyValue>) -> PyResult<()> {
        let mut inner = self.attributes.lock().map_err(handle_poison_error)?;
        inner.clear();
        for kv in attributes {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error).unwrap();
            inner.push(kv_lock.clone());
        }
        Ok(())
    }

    #[getter]
    fn start_time_unix_nano(&self) -> PyResult<u64> {
        Ok(self.start_time_unix_nano)
    }

    #[setter]
    fn set_start_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        self.start_time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn time_unix_nano(&self) -> PyResult<u64> {
        Ok(self.time_unix_nano)
    }

    #[setter]
    fn set_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        self.time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn count(&self) -> PyResult<u64> {
        Ok(self.count)
    }

    #[setter]
    fn set_count(&mut self, count: u64) -> PyResult<()> {
        self.count = count;
        Ok(())
    }

    #[getter]
    fn sum(&self) -> PyResult<f64> {
        Ok(self.sum)
    }

    #[setter]
    fn set_sum(&mut self, sum: f64) -> PyResult<()> {
        self.sum = sum;
        Ok(())
    }

    #[getter]
    fn quantile_values(&self) -> PyResult<Vec<ValueAtQuantile>> {
        Ok(self.quantile_values.clone())
    }

    #[setter]
    fn set_quantile_values(&mut self, values: Vec<ValueAtQuantile>) -> PyResult<()> {
        self.quantile_values = values;
        Ok(())
    }

    #[getter]
    fn flags(&self) -> PyResult<u32> {
        Ok(self.flags)
    }

    #[setter]
    fn set_flags(&mut self, flags: u32) -> PyResult<()> {
        self.flags = flags;
        Ok(())
    }
}

impl From<SummaryDataPoint> for RSummaryDataPoint {
    fn from(dp: SummaryDataPoint) -> Self {
        RSummaryDataPoint {
            attributes: dp.attributes,
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum,
            quantile_values: dp.quantile_values.into_iter().map(|v| v.into()).collect(),
            flags: dp.flags,
        }
    }
}

// --- PyO3 Bindings for RValueAtQuantile ---
#[pyclass]
#[derive(Clone)]
pub struct ValueAtQuantile {
    pub inner: RValueAtQuantile, // This one was already direct, keeping as is
}

#[pymethods]
impl ValueAtQuantile {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ValueAtQuantile {
            inner: RValueAtQuantile {
                quantile: 0.0,
                value: 0.0,
            },
        })
    }

    #[getter]
    fn quantile(&self) -> PyResult<f64> {
        Ok(self.inner.quantile)
    }

    #[setter]
    fn set_quantile(&mut self, quantile: f64) -> PyResult<()> {
        self.inner.quantile = quantile;
        Ok(())
    }

    #[getter]
    fn value(&self) -> PyResult<f64> {
        Ok(self.inner.value)
    }

    #[setter]
    fn set_value(&mut self, value: f64) -> PyResult<()> {
        self.inner.value = value;
        Ok(())
    }
}

impl From<ValueAtQuantile> for RValueAtQuantile {
    fn from(v: ValueAtQuantile) -> Self {
        v.inner
    }
}

impl From<RValueAtQuantile> for ValueAtQuantile {
    fn from(v: RValueAtQuantile) -> Self {
        ValueAtQuantile { inner: v }
    }
}

// --- PyO3 Bindings for ExemplarList ---
#[pyclass]
pub struct ExemplarList(Arc<Mutex<Vec<Arc<Mutex<RExemplar>>>>>);

#[pymethods]
impl ExemplarList {
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Py<ExemplarListIter>> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        let iter = ExemplarListIter {
            inner: inner.clone().into_iter(),
        };
        Py::new(py, iter)
    }

    fn __getitem__(&self, index: usize) -> PyResult<Exemplar> {
        let inner = self.0.lock().map_err(handle_poison_error)?;
        match inner.get(index) {
            Some(item) => Ok(Exemplar {
                inner: item.clone(),
            }),
            None => Err(PyErr::new::<pyo3::exceptions::PyIndexError, _>(
                "Index out of bounds",
            )),
        }
    }
    fn __setitem__(&self, index: usize, value: &Exemplar) -> PyResult<()> {
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
    fn append(&self, item: &Exemplar) -> PyResult<()> {
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
pub struct ExemplarListIter {
    inner: vec::IntoIter<Arc<Mutex<RExemplar>>>,
}

#[pymethods]
impl ExemplarListIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Exemplar>> {
        let item = slf.inner.next();
        if item.is_none() {
            return Ok(None);
        }
        let inner = item.unwrap();
        Ok(Some(Exemplar {
            inner: inner.clone(),
        }))
    }
}

// --- PyO3 Bindings for RExemplar ---
#[pyclass]
#[derive(Clone)]
pub struct Exemplar {
    inner: Arc<Mutex<RExemplar>>,
}

#[pymethods]
impl Exemplar {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Exemplar {
            inner: Arc::new(Mutex::new(RExemplar {
                filtered_attributes: Arc::new(Mutex::new(vec![])),
                time_unix_nano: 0,
                span_id: vec![],
                trace_id: vec![],
                value: None,
            })),
        })
    }

    #[getter]
    fn filtered_attributes(&self) -> PyResult<AttributesList> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(AttributesList(inner.filtered_attributes.clone()))
    }

    #[setter]
    fn set_filtered_attributes(&mut self, attributes: Vec<KeyValue>) -> PyResult<()> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        let mut v = inner
            .filtered_attributes
            .lock()
            .map_err(handle_poison_error)?;
        v.clear();
        for kv in attributes {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error).unwrap();
            v.push(kv_lock.clone());
        }
        Ok(())
    }

    #[getter]
    fn time_unix_nano(&self) -> PyResult<u64> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.time_unix_nano)
    }

    #[setter]
    fn set_time_unix_nano(&mut self, time: u64) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.time_unix_nano = time;
        Ok(())
    }

    #[getter]
    fn span_id(&self) -> PyResult<Vec<u8>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.span_id.clone())
    }

    #[setter]
    fn set_span_id(&mut self, id: Vec<u8>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.span_id = id;
        Ok(())
    }

    #[getter]
    fn trace_id(&self) -> PyResult<Vec<u8>> {
        let inner = self.inner.lock().map_err(handle_poison_error)?;
        Ok(inner.trace_id.clone())
    }

    #[setter]
    fn set_trace_id(&mut self, id: Vec<u8>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        inner.trace_id = id;
        Ok(())
    }

    #[getter]
    fn value(&self) -> PyResult<Option<ExemplarValue>> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        if inner.value.is_some() {
            let rv = inner.value.take().unwrap();
            let rv_copy = rv.clone();
            inner.value.replace(rv);
            match rv_copy {
                RExemplarValue::AsDouble(x) => Ok(Some(ExemplarValue::AsDouble(x))),
                RExemplarValue::AsInt(x) => Ok(Some(ExemplarValue::AsInt(x))),
            }
        } else {
            Ok(None)
        }
    }

    #[setter]
    fn set_value(&mut self, value: Option<ExemplarValue>) -> PyResult<()> {
        let mut inner = self.inner.lock().map_err(handle_poison_error)?;
        if value.is_none() {
            inner.value = None;
            return Ok(());
        }
        let v = value.unwrap();
        match v {
            ExemplarValue::AsDouble(f) => inner.value = Some(RExemplarValue::AsDouble(f)),
            ExemplarValue::AsInt(i) => inner.value = Some(RExemplarValue::AsInt(i)),
        }
        Ok(())
    }
}

// --- PyO3 Bindings for RExemplarValue (Enum) ---
#[pyclass]
#[derive(Clone)]
pub enum ExemplarValue {
    AsDouble(f64),
    AsInt(i64),
}

// --- PyO3 Bindings for RAggregationTemporality (Enum) ---
#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AggregationTemporality {
    Unspecified = 0,
    Delta = 1,
    Cumulative = 2,
}

#[pymethods]
impl AggregationTemporality {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(AggregationTemporality::Unspecified)
    }

    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "AGGREGATION_TEMPORALITY_UNSPECIFIED",
            Self::Delta => "AGGREGATION_TEMPORALITY_DELTA",
            Self::Cumulative => "AGGREGATION_TEMPORALITY_CUMULATIVE",
        }
    }
}

impl From<i32> for AggregationTemporality {
    fn from(value: i32) -> Self {
        match value {
            1 => AggregationTemporality::Delta,
            2 => AggregationTemporality::Cumulative,
            _ => AggregationTemporality::Unspecified,
        }
    }
}
impl From<AggregationTemporality> for i32 {
    fn from(value: AggregationTemporality) -> i32 {
        match value {
            AggregationTemporality::Unspecified => 3,
            AggregationTemporality::Delta => 1,
            AggregationTemporality::Cumulative => 2,
        }
    }
}

// --- PyO3 Bindings for RDataPointFlags (Enum) ---
#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataPointFlags {
    DoNotUse = 0,
    NoRecordedValueMask = 1,
}

#[pymethods]
impl DataPointFlags {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(DataPointFlags::DoNotUse)
    }

    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::DoNotUse => "DATA_POINT_FLAGS_DO_NOT_USE",
            Self::NoRecordedValueMask => "DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK",
        }
    }

    #[getter]
    fn value(&self) -> PyResult<u32> {
        match self {
            DataPointFlags::DoNotUse => Ok(0),
            DataPointFlags::NoRecordedValueMask => Ok(1),
        }
    }
}

impl From<u32> for DataPointFlags {
    fn from(value: u32) -> Self {
        match value {
            1 => DataPointFlags::NoRecordedValueMask,
            _ => DataPointFlags::DoNotUse,
        }
    }
}

impl From<DataPointFlags> for u32 {
    fn from(value: DataPointFlags) -> Self {
        match value {
            DataPointFlags::DoNotUse => 0,
            DataPointFlags::NoRecordedValueMask => 1,
        }
    }
}
