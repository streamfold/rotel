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
    pub schema_url: String,
}

#[pymethods]
impl ScopeMetrics {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ScopeMetrics {
            scope: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(vec![])),
            schema_url: String::new(),
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
            inner.push(Arc::new(Mutex::new(RMetric {
                name: m.name.clone(),
                description: m.description.clone(),
                unit: m.unit.clone(),
                metadata: m.metadata.clone(),
                data: m.data.clone().map(|data| match data {
                    MetricData::Gauge(g) => RMetricData::Gauge(g.into()),
                    MetricData::Sum(s) => RMetricData::Sum(s.into()),
                    MetricData::Histogram(h) => RMetricData::Histogram(h.into()),
                    MetricData::ExponentialHistogram(eh) => {
                        RMetricData::ExponentialHistogram(eh.into())
                    }
                    MetricData::Summary(s) => RMetricData::Summary(s.into()),
                }),
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
            Some(item) => {
                let item_lock = item.lock().unwrap();
                Ok(Metric {
                    name: item_lock.name.clone(),
                    description: item_lock.description.clone(),
                    unit: item_lock.unit.clone(),
                    metadata: item_lock.metadata.clone(),
                    data: item_lock.data.clone().map(|data| match data {
                        RMetricData::Gauge(g) => MetricData::Gauge(Gauge {
                            data_points: g.data_points.clone(),
                        }),
                        RMetricData::Sum(s) => MetricData::Sum(Sum {
                            data_points: s.data_points.clone(),
                            aggregation_temporality: s.aggregation_temporality.into(),
                            is_monotonic: s.is_monotonic,
                        }),
                        RMetricData::Histogram(h) => MetricData::Histogram(Histogram {
                            data_points: h.data_points.clone(),
                            aggregation_temporality: h.aggregation_temporality.into(),
                        }),
                        RMetricData::ExponentialHistogram(eh) => {
                            MetricData::ExponentialHistogram(ExponentialHistogram {
                                data_points: eh.data_points.clone(),
                                aggregation_temporality: eh.aggregation_temporality.into(),
                            })
                        }
                        RMetricData::Summary(s) => MetricData::Summary(Summary {
                            data_points: s.data_points.clone(),
                        }),
                    }),
                })
            }
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
        inner[index] = Arc::new(Mutex::new(RMetric {
            name: value.name.clone(),
            description: value.description.clone(),
            unit: value.unit.clone(),
            metadata: value.metadata.clone(),
            data: value.data.clone().map(|data| match data {
                MetricData::Gauge(g) => RMetricData::Gauge(g.into()),
                MetricData::Sum(s) => RMetricData::Sum(s.into()),
                MetricData::Histogram(h) => RMetricData::Histogram(h.into()),
                MetricData::ExponentialHistogram(eh) => {
                    RMetricData::ExponentialHistogram(eh.into())
                }
                MetricData::Summary(s) => RMetricData::Summary(s.into()),
            }),
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
    fn append(&self, item: &Metric) -> PyResult<()> {
        let mut k = self.0.lock().map_err(handle_poison_error)?;
        k.push(Arc::new(Mutex::new(RMetric {
            name: item.name.clone(),
            description: item.description.clone(),
            unit: item.unit.clone(),
            metadata: item.metadata.clone(),
            data: item.data.clone().map(|data| match data {
                MetricData::Gauge(g) => RMetricData::Gauge(g.into()),
                MetricData::Sum(s) => RMetricData::Sum(s.into()),
                MetricData::Histogram(h) => RMetricData::Histogram(h.into()),
                MetricData::ExponentialHistogram(eh) => {
                    RMetricData::ExponentialHistogram(eh.into())
                }
                MetricData::Summary(s) => RMetricData::Summary(s.into()),
            }),
        })));
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
        let inner = inner.lock().unwrap();
        Ok(Some(Metric {
            name: inner.name.clone(),
            description: inner.description.clone(),
            unit: inner.unit.clone(),
            metadata: inner.metadata.clone(),
            data: inner.data.clone().map(|data| match data {
                RMetricData::Gauge(g) => MetricData::Gauge(Gauge {
                    data_points: g.data_points.clone(),
                }),
                RMetricData::Sum(s) => MetricData::Sum(Sum {
                    data_points: s.data_points.clone(),
                    aggregation_temporality: s.aggregation_temporality.into(),
                    is_monotonic: s.is_monotonic,
                }),
                RMetricData::Histogram(h) => MetricData::Histogram(Histogram {
                    data_points: h.data_points.clone(),
                    aggregation_temporality: h.aggregation_temporality.into(),
                }),
                RMetricData::ExponentialHistogram(eh) => {
                    MetricData::ExponentialHistogram(ExponentialHistogram {
                        data_points: eh.data_points.clone(),
                        aggregation_temporality: eh.aggregation_temporality.into(),
                    })
                }
                RMetricData::Summary(s) => MetricData::Summary(Summary {
                    data_points: s.data_points.clone(),
                }),
            }),
        }))
    }
}

// --- PyO3 Bindings for RMetric ---
#[pyclass]
#[derive(Clone)]
pub struct Metric {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub metadata: Arc<Mutex<Vec<RKeyValue>>>,
    pub data: Option<MetricData>,
}

#[pymethods]
impl Metric {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Metric {
            name: String::new(),
            description: String::new(),
            unit: String::new(),
            metadata: Arc::new(Mutex::new(vec![])),
            data: None,
        })
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.name.clone())
    }

    #[setter]
    fn set_name(&mut self, name: String) -> PyResult<()> {
        self.name = name;
        Ok(())
    }

    #[getter]
    fn description(&self) -> PyResult<String> {
        Ok(self.description.clone())
    }

    #[setter]
    fn set_description(&mut self, description: String) -> PyResult<()> {
        self.description = description;
        Ok(())
    }

    #[getter]
    fn unit(&self) -> PyResult<String> {
        Ok(self.unit.clone())
    }

    #[setter]
    fn set_unit(&mut self, unit: String) -> PyResult<()> {
        self.unit = unit;
        Ok(())
    }

    #[getter]
    fn metadata(&self) -> PyResult<AttributesList> {
        Ok(AttributesList(self.metadata.clone()))
    }

    #[setter]
    fn set_metadata(&mut self, metadata: Vec<KeyValue>) -> PyResult<()> {
        let mut inner = self.metadata.lock().map_err(handle_poison_error)?;
        inner.clear();
        for kv in metadata {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error).unwrap();
            inner.push(kv_lock.clone());
        }
        Ok(())
    }

    #[getter]
    fn data(&self) -> PyResult<Option<MetricData>> {
        Ok(self.data.clone())
    }

    #[setter]
    fn set_data(&mut self, data: Option<MetricData>) -> PyResult<()> {
        self.data = data;
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

impl From<Gauge> for RGauge {
    fn from(g: Gauge) -> Self {
        RGauge {
            data_points: g.data_points,
        }
    }
}

impl From<Sum> for RSum {
    fn from(s: Sum) -> Self {
        RSum {
            data_points: s.data_points,
            aggregation_temporality: s.aggregation_temporality as i32,
            is_monotonic: s.is_monotonic,
        }
    }
}

impl From<Histogram> for RHistogram {
    fn from(h: Histogram) -> Self {
        RHistogram {
            data_points: h.data_points,
            aggregation_temporality: h.aggregation_temporality as i32,
        }
    }
}

impl From<ExponentialHistogram> for RExponentialHistogram {
    fn from(eh: ExponentialHistogram) -> Self {
        RExponentialHistogram {
            data_points: eh.data_points,
            aggregation_temporality: eh.aggregation_temporality as i32,
        }
    }
}

impl From<Summary> for RSummary {
    fn from(s: Summary) -> Self {
        RSummary {
            data_points: s.data_points,
        }
    }
}

// --- PyO3 Bindings for RGauge ---
#[pyclass]
#[derive(Clone)]
pub struct Gauge {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RNumberDataPoint>>>>>,
}

#[pymethods]
impl Gauge {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Gauge {
            data_points: Arc::new(Mutex::new(vec![])),
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<NumberDataPointList> {
        Ok(NumberDataPointList(self.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<NumberDataPoint>) -> PyResult<()> {
        let mut inner = self.data_points.lock().map_err(handle_poison_error)?;
        inner.clear();
        for dp in data_points {
            inner.push(Arc::new(Mutex::new(RNumberDataPoint {
                attributes: dp.attributes.clone(),
                start_time_unix_nano: dp.start_time_unix_nano,
                time_unix_nano: dp.time_unix_nano,
                exemplars: dp.exemplars.clone(),
                flags: dp.flags,
                value: dp.value.map(|v| match v {
                    NumberDataPointValue::AsDouble(d) => RNumberDataPointValue::AsDouble(d),
                    NumberDataPointValue::AsInt(i) => RNumberDataPointValue::AsInt(i),
                }),
            })));
        }
        Ok(())
    }
}

// --- PyO3 Bindings for RSum ---
#[pyclass]
#[derive(Clone)]
pub struct Sum {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RNumberDataPoint>>>>>,
    pub aggregation_temporality: AggregationTemporality,
    pub is_monotonic: bool,
}

#[pymethods]
impl Sum {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Sum {
            data_points: Arc::new(Mutex::new(vec![])),
            aggregation_temporality: AggregationTemporality::Unspecified,
            is_monotonic: false,
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<NumberDataPointList> {
        Ok(NumberDataPointList(self.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<NumberDataPoint>) -> PyResult<()> {
        let mut inner = self.data_points.lock().map_err(handle_poison_error)?;
        inner.clear();
        for dp in data_points {
            inner.push(Arc::new(Mutex::new(dp.into())));
        }
        Ok(())
    }

    #[getter]
    fn aggregation_temporality(&self) -> PyResult<AggregationTemporality> {
        Ok(self.aggregation_temporality)
    }

    #[setter]
    fn set_aggregation_temporality(&mut self, temporality: AggregationTemporality) -> PyResult<()> {
        self.aggregation_temporality = temporality;
        Ok(())
    }

    #[getter]
    fn is_monotonic(&self) -> PyResult<bool> {
        Ok(self.is_monotonic)
    }

    #[setter]
    fn set_is_monotonic(&mut self, is_monotonic: bool) -> PyResult<()> {
        self.is_monotonic = is_monotonic;
        Ok(())
    }
}

// --- PyO3 Bindings for RHistogram ---
#[pyclass]
#[derive(Clone)]
pub struct Histogram {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RHistogramDataPoint>>>>>,
    pub aggregation_temporality: AggregationTemporality,
}

#[pymethods]
impl Histogram {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Histogram {
            data_points: Arc::new(Mutex::new(vec![])),
            aggregation_temporality: AggregationTemporality::Unspecified,
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<HistogramDataPointList> {
        Ok(HistogramDataPointList(self.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<HistogramDataPoint>) -> PyResult<()> {
        let mut inner = self.data_points.lock().map_err(handle_poison_error)?;
        inner.clear();
        for dp in data_points {
            inner.push(Arc::new(Mutex::new(dp.into())));
        }
        Ok(())
    }

    #[getter]
    fn aggregation_temporality(&self) -> PyResult<AggregationTemporality> {
        Ok(self.aggregation_temporality)
    }

    #[setter]
    fn set_aggregation_temporality(&mut self, temporality: AggregationTemporality) -> PyResult<()> {
        self.aggregation_temporality = temporality;
        Ok(())
    }
}

// --- PyO3 Bindings for RExponentialHistogram ---
#[pyclass]
#[derive(Clone)]
pub struct ExponentialHistogram {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RExponentialHistogramDataPoint>>>>>,
    pub aggregation_temporality: AggregationTemporality,
}

#[pymethods]
impl ExponentialHistogram {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ExponentialHistogram {
            data_points: Arc::new(Mutex::new(vec![])),
            aggregation_temporality: AggregationTemporality::Unspecified,
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<ExponentialHistogramDataPointList> {
        Ok(ExponentialHistogramDataPointList(self.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<ExponentialHistogramDataPoint>) -> PyResult<()> {
        let mut inner = self.data_points.lock().map_err(handle_poison_error)?;
        inner.clear();
        for dp in data_points {
            inner.push(Arc::new(Mutex::new(dp.into())));
        }
        Ok(())
    }

    #[getter]
    fn aggregation_temporality(&self) -> PyResult<AggregationTemporality> {
        Ok(self.aggregation_temporality)
    }

    #[setter]
    fn set_aggregation_temporality(&mut self, temporality: AggregationTemporality) -> PyResult<()> {
        self.aggregation_temporality = temporality;
        Ok(())
    }
}

// --- PyO3 Bindings for RSummary ---
#[pyclass]
#[derive(Clone)]
pub struct Summary {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RSummaryDataPoint>>>>>,
}

#[pymethods]
impl Summary {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Summary {
            data_points: Arc::new(Mutex::new(vec![])),
        })
    }

    #[getter]
    fn data_points(&self) -> PyResult<SummaryDataPointList> {
        Ok(SummaryDataPointList(self.data_points.clone()))
    }

    #[setter]
    fn set_data_points(&mut self, data_points: Vec<SummaryDataPoint>) -> PyResult<()> {
        let mut inner = self.data_points.lock().map_err(handle_poison_error)?;
        inner.clear();
        for dp in data_points {
            inner.push(Arc::new(Mutex::new(dp.into())));
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
            Some(item) => {
                let item_lock = item.lock().unwrap();
                Ok(NumberDataPoint {
                    attributes: item_lock.attributes.clone(),
                    start_time_unix_nano: item_lock.start_time_unix_nano,
                    time_unix_nano: item_lock.time_unix_nano,
                    exemplars: item_lock.exemplars.clone(),
                    flags: item_lock.flags,
                    value: item_lock.value.clone().map(|v| match v {
                        RNumberDataPointValue::AsDouble(d) => NumberDataPointValue::AsDouble(d),
                        RNumberDataPointValue::AsInt(i) => NumberDataPointValue::AsInt(i),
                    }),
                })
            }
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

        inner[index] = Arc::new(Mutex::new(RNumberDataPoint {
            attributes: value.attributes.clone(),
            start_time_unix_nano: value.start_time_unix_nano,
            time_unix_nano: value.time_unix_nano,
            exemplars: value.exemplars.clone(),
            flags: value.flags,
            value: value.value.clone().map(|v| match v {
                NumberDataPointValue::AsDouble(d) => RNumberDataPointValue::AsDouble(d),
                NumberDataPointValue::AsInt(i) => RNumberDataPointValue::AsInt(i),
            }),
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
    fn append(&self, item: &NumberDataPoint) -> PyResult<()> {
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
        let inner = inner.lock().unwrap();
        Ok(Some(NumberDataPoint {
            attributes: inner.attributes.clone(),
            start_time_unix_nano: inner.start_time_unix_nano,
            time_unix_nano: inner.time_unix_nano,
            exemplars: inner.exemplars.clone(),
            flags: inner.flags,
            value: inner.value.clone().map(|v| match v {
                RNumberDataPointValue::AsDouble(d) => NumberDataPointValue::AsDouble(d),
                RNumberDataPointValue::AsInt(i) => NumberDataPointValue::AsInt(i),
            }),
        }))
    }
}

// --- PyO3 Bindings for RNumberDataPoint ---
#[pyclass]
#[derive(Clone)]
pub struct NumberDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub exemplars: Arc<Mutex<Vec<Arc<Mutex<RExemplar>>>>>,
    pub flags: u32,
    pub value: Option<NumberDataPointValue>,
}

#[pymethods]
impl NumberDataPoint {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(NumberDataPoint {
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            exemplars: Arc::new(Mutex::new(vec![])),
            flags: 0,
            value: None,
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
    fn exemplars(&self) -> PyResult<ExemplarList> {
        Ok(ExemplarList(self.exemplars.clone()))
    }

    #[setter]
    fn set_exemplars(&mut self, exemplars: Vec<Exemplar>) -> PyResult<()> {
        let mut inner = self.exemplars.lock().map_err(handle_poison_error)?;
        inner.clear();
        for e in exemplars {
            inner.push(Arc::new(Mutex::new(e.into())));
        }
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

    #[getter]
    fn value(&self) -> PyResult<Option<NumberDataPointValue>> {
        Ok(self.value.clone())
    }

    #[setter]
    fn set_value(&mut self, value: Option<NumberDataPointValue>) -> PyResult<()> {
        self.value = value;
        Ok(())
    }
}

impl From<NumberDataPoint> for RNumberDataPoint {
    fn from(dp: NumberDataPoint) -> Self {
        RNumberDataPoint {
            attributes: dp.attributes,
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            exemplars: dp.exemplars,
            flags: dp.flags,
            value: dp.value.map(|v| match v {
                NumberDataPointValue::AsDouble(d) => RNumberDataPointValue::AsDouble(d),
                NumberDataPointValue::AsInt(i) => RNumberDataPointValue::AsInt(i),
            }),
        }
    }
}

// --- PyO3 Bindings for RNumberDataPointValue (Enum) ---
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
            Some(item) => {
                let item_lock = item.lock().unwrap();
                Ok(HistogramDataPoint {
                    attributes: item_lock.attributes.clone(),
                    start_time_unix_nano: item_lock.start_time_unix_nano,
                    time_unix_nano: item_lock.time_unix_nano,
                    count: item_lock.count,
                    sum: item_lock.sum,
                    bucket_counts: item_lock.bucket_counts.clone(),
                    explicit_bounds: item_lock.explicit_bounds.clone(),
                    exemplars: item_lock.exemplars.clone(),
                    flags: item_lock.flags,
                    min: item_lock.min,
                    max: item_lock.max,
                })
            }
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
    fn append(&self, item: &HistogramDataPoint) -> PyResult<()> {
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
        let inner = inner.lock().unwrap();
        Ok(Some(HistogramDataPoint {
            attributes: inner.attributes.clone(),
            start_time_unix_nano: inner.start_time_unix_nano,
            time_unix_nano: inner.time_unix_nano,
            count: inner.count,
            sum: inner.sum,
            bucket_counts: inner.bucket_counts.clone(),
            explicit_bounds: inner.explicit_bounds.clone(),
            exemplars: inner.exemplars.clone(),
            flags: inner.flags,
            min: inner.min,
            max: inner.max,
        }))
    }
}

// --- PyO3 Bindings for RHistogramDataPoint ---
#[pyclass]
#[derive(Clone)]
pub struct HistogramDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: Option<f64>,
    pub bucket_counts: Vec<u64>,
    pub explicit_bounds: Vec<f64>,
    pub exemplars: Arc<Mutex<Vec<Arc<Mutex<RExemplar>>>>>,
    pub flags: u32,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[pymethods]
impl HistogramDataPoint {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(HistogramDataPoint {
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            count: 0,
            sum: None,
            bucket_counts: vec![],
            explicit_bounds: vec![],
            exemplars: Arc::new(Mutex::new(vec![])),
            flags: 0,
            min: None,
            max: None,
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
    fn sum(&self) -> PyResult<Option<f64>> {
        Ok(self.sum)
    }

    #[setter]
    fn set_sum(&mut self, sum: Option<f64>) -> PyResult<()> {
        self.sum = sum;
        Ok(())
    }

    #[getter]
    fn bucket_counts(&self) -> PyResult<Vec<u64>> {
        Ok(self.bucket_counts.clone())
    }

    #[setter]
    fn set_bucket_counts(&mut self, bucket_counts: Vec<u64>) -> PyResult<()> {
        self.bucket_counts = bucket_counts;
        Ok(())
    }

    #[getter]
    fn explicit_bounds(&self) -> PyResult<Vec<f64>> {
        Ok(self.explicit_bounds.clone())
    }

    #[setter]
    fn set_explicit_bounds(&mut self, explicit_bounds: Vec<f64>) -> PyResult<()> {
        self.explicit_bounds = explicit_bounds;
        Ok(())
    }

    #[getter]
    fn exemplars(&self) -> PyResult<ExemplarList> {
        Ok(ExemplarList(self.exemplars.clone()))
    }

    #[setter]
    fn set_exemplars(&mut self, exemplars: Vec<Exemplar>) -> PyResult<()> {
        let mut inner = self.exemplars.lock().map_err(handle_poison_error)?;
        inner.clear();
        for e in exemplars {
            inner.push(Arc::new(Mutex::new(e.into())));
        }
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

    #[getter]
    fn min(&self) -> PyResult<Option<f64>> {
        Ok(self.min)
    }

    #[setter]
    fn set_min(&mut self, min: Option<f64>) -> PyResult<()> {
        self.min = min;
        Ok(())
    }

    #[getter]
    fn max(&self) -> PyResult<Option<f64>> {
        Ok(self.max)
    }

    #[setter]
    fn set_max(&mut self, max: Option<f64>) -> PyResult<()> {
        self.max = max;
        Ok(())
    }
}

impl From<HistogramDataPoint> for RHistogramDataPoint {
    fn from(dp: HistogramDataPoint) -> Self {
        RHistogramDataPoint {
            attributes: dp.attributes,
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum,
            bucket_counts: dp.bucket_counts,
            explicit_bounds: dp.explicit_bounds,
            exemplars: dp.exemplars,
            flags: dp.flags,
            min: dp.min,
            max: dp.max,
        }
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
            Some(item) => {
                let item_lock = item.lock().unwrap();
                Ok(ExponentialHistogramDataPoint {
                    attributes: item_lock.attributes.clone(),
                    start_time_unix_nano: item_lock.start_time_unix_nano,
                    time_unix_nano: item_lock.time_unix_nano,
                    count: item_lock.count,
                    sum: item_lock.sum,
                    scale: item_lock.scale,
                    zero_count: item_lock.zero_count,
                    positive: item_lock.positive.clone().map(|p| p.to_owned().into()),
                    negative: item_lock.negative.clone().map(|n| n.to_owned().into()),
                    flags: item_lock.flags,
                    exemplars: item_lock.exemplars.clone(),
                    min: item_lock.min,
                    max: item_lock.max,
                    zero_threshold: item_lock.zero_threshold,
                })
            }
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
    fn append(&self, item: &ExponentialHistogramDataPoint) -> PyResult<()> {
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
        let inner = inner.lock().unwrap();
        Ok(Some(ExponentialHistogramDataPoint {
            attributes: inner.attributes.clone(),
            start_time_unix_nano: inner.start_time_unix_nano,
            time_unix_nano: inner.time_unix_nano,
            count: inner.count,
            sum: inner.sum,
            scale: inner.scale,
            zero_count: inner.zero_count,
            positive: inner.positive.clone().map(|p| p.into()),
            negative: inner.negative.clone().map(|n| n.into()),
            flags: inner.flags,
            exemplars: inner.exemplars.clone(),
            min: inner.min,
            max: inner.max,
            zero_threshold: inner.zero_threshold,
        }))
    }
}

// --- PyO3 Bindings for RExponentialHistogramDataPoint ---
#[pyclass]
#[derive(Clone)]
pub struct ExponentialHistogramDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: Option<f64>,
    pub scale: i32,
    pub zero_count: u64,
    pub positive: Option<ExponentialHistogramBuckets>,
    pub negative: Option<ExponentialHistogramBuckets>,
    pub flags: u32,
    pub exemplars: Arc<Mutex<Vec<Arc<Mutex<RExemplar>>>>>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub zero_threshold: f64,
}

#[pymethods]
impl ExponentialHistogramDataPoint {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ExponentialHistogramDataPoint {
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            count: 0,
            sum: None,
            scale: 0,
            zero_count: 0,
            positive: None,
            negative: None,
            flags: 0,
            exemplars: Arc::new(Mutex::new(vec![])),
            min: None,
            max: None,
            zero_threshold: 0.0,
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
    fn sum(&self) -> PyResult<Option<f64>> {
        Ok(self.sum)
    }

    #[setter]
    fn set_sum(&mut self, sum: Option<f64>) -> PyResult<()> {
        self.sum = sum;
        Ok(())
    }

    #[getter]
    fn scale(&self) -> PyResult<i32> {
        Ok(self.scale)
    }

    #[setter]
    fn set_scale(&mut self, scale: i32) -> PyResult<()> {
        self.scale = scale;
        Ok(())
    }

    #[getter]
    fn zero_count(&self) -> PyResult<u64> {
        Ok(self.zero_count)
    }

    #[setter]
    fn set_zero_count(&mut self, count: u64) -> PyResult<()> {
        self.zero_count = count;
        Ok(())
    }

    #[getter]
    fn positive(&self) -> PyResult<Option<ExponentialHistogramBuckets>> {
        Ok(self.positive.clone())
    }

    #[setter]
    fn set_positive(&mut self, buckets: Option<ExponentialHistogramBuckets>) -> PyResult<()> {
        self.positive = buckets;
        Ok(())
    }

    #[getter]
    fn negative(&self) -> PyResult<Option<ExponentialHistogramBuckets>> {
        Ok(self.negative.clone())
    }

    #[setter]
    fn set_negative(&mut self, buckets: Option<ExponentialHistogramBuckets>) -> PyResult<()> {
        self.negative = buckets;
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

    #[getter]
    fn exemplars(&self) -> PyResult<ExemplarList> {
        Ok(ExemplarList(self.exemplars.clone()))
    }

    #[setter]
    fn set_exemplars(&mut self, exemplars: Vec<Exemplar>) -> PyResult<()> {
        let mut inner = self.exemplars.lock().map_err(handle_poison_error)?;
        inner.clear();
        for e in exemplars {
            inner.push(Arc::new(Mutex::new(e.into())));
        }
        Ok(())
    }

    #[getter]
    fn min(&self) -> PyResult<Option<f64>> {
        Ok(self.min)
    }

    #[setter]
    fn set_min(&mut self, min: Option<f64>) -> PyResult<()> {
        self.min = min;
        Ok(())
    }

    #[getter]
    fn max(&self) -> PyResult<Option<f64>> {
        Ok(self.max)
    }

    #[setter]
    fn set_max(&mut self, max: Option<f64>) -> PyResult<()> {
        self.max = max;
        Ok(())
    }

    #[getter]
    fn zero_threshold(&self) -> PyResult<f64> {
        Ok(self.zero_threshold)
    }

    #[setter]
    fn set_zero_threshold(&mut self, threshold: f64) -> PyResult<()> {
        self.zero_threshold = threshold;
        Ok(())
    }
}

impl From<ExponentialHistogramDataPoint> for RExponentialHistogramDataPoint {
    fn from(dp: ExponentialHistogramDataPoint) -> Self {
        RExponentialHistogramDataPoint {
            attributes: dp.attributes,
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum,
            scale: dp.scale,
            zero_count: dp.zero_count,
            positive: dp.positive.map(|p| p.into()),
            negative: dp.negative.map(|n| n.into()),
            flags: dp.flags,
            exemplars: dp.exemplars,
            min: dp.min,
            max: dp.max,
            zero_threshold: dp.zero_threshold,
        }
    }
}

impl From<RExponentialHistogramDataPoint> for ExponentialHistogramDataPoint {
    fn from(dp: RExponentialHistogramDataPoint) -> Self {
        ExponentialHistogramDataPoint {
            attributes: dp.attributes.clone(),
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum,
            scale: dp.scale,
            zero_count: dp.zero_count,
            positive: dp.positive.map(|p| p.into()),
            negative: dp.negative.map(|n| n.into()),
            flags: dp.flags,
            exemplars: dp.exemplars.clone(),
            min: dp.min,
            max: dp.max,
            zero_threshold: dp.zero_threshold,
        }
    }
}

// --- PyO3 Bindings for RExponentialHistogramBuckets ---
#[pyclass]
#[derive(Clone)]
pub struct ExponentialHistogramBuckets {
    pub offset: i32,
    pub bucket_counts: Vec<u64>,
}

#[pymethods]
impl ExponentialHistogramBuckets {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(ExponentialHistogramBuckets {
            offset: 0,
            bucket_counts: vec![],
        })
    }

    #[getter]
    fn offset(&self) -> PyResult<i32> {
        Ok(self.offset)
    }

    #[setter]
    fn set_offset(&mut self, offset: i32) -> PyResult<()> {
        self.offset = offset;
        Ok(())
    }

    #[getter]
    fn bucket_counts(&self) -> PyResult<Vec<u64>> {
        Ok(self.bucket_counts.clone())
    }

    #[setter]
    fn set_bucket_counts(&mut self, bucket_counts: Vec<u64>) -> PyResult<()> {
        self.bucket_counts = bucket_counts;
        Ok(())
    }
}

impl From<ExponentialHistogramBuckets> for RExponentialHistogramBuckets {
    fn from(b: ExponentialHistogramBuckets) -> Self {
        RExponentialHistogramBuckets {
            offset: b.offset,
            bucket_counts: b.bucket_counts,
        }
    }
}

impl From<RExponentialHistogramBuckets> for ExponentialHistogramBuckets {
    fn from(b: RExponentialHistogramBuckets) -> Self {
        ExponentialHistogramBuckets {
            offset: b.offset,
            bucket_counts: b.bucket_counts,
        }
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
            Some(item) => {
                let item_lock = item.lock().unwrap();
                Ok(Exemplar {
                    filtered_attributes: item_lock.filtered_attributes.clone(),
                    time_unix_nano: item_lock.time_unix_nano,
                    span_id: item_lock.span_id.clone(),
                    trace_id: item_lock.trace_id.clone(),
                    value: item_lock.value.clone().map(|v| match v {
                        RExemplarValue::AsDouble(d) => ExemplarValue::AsDouble(d),
                        RExemplarValue::AsInt(i) => ExemplarValue::AsInt(i),
                    }),
                })
            }
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
    fn append(&self, item: &Exemplar) -> PyResult<()> {
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
        let inner = inner.lock().unwrap();
        Ok(Some(Exemplar {
            filtered_attributes: inner.filtered_attributes.clone(),
            time_unix_nano: inner.time_unix_nano,
            span_id: inner.span_id.clone(),
            trace_id: inner.trace_id.clone(),
            value: inner.value.clone().map(|v| match v {
                RExemplarValue::AsDouble(d) => ExemplarValue::AsDouble(d),
                RExemplarValue::AsInt(i) => ExemplarValue::AsInt(i),
            }),
        }))
    }
}

// --- PyO3 Bindings for RExemplar ---
#[pyclass]
#[derive(Clone)]
pub struct Exemplar {
    pub filtered_attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub time_unix_nano: u64,
    pub span_id: Vec<u8>,
    pub trace_id: Vec<u8>,
    pub value: Option<ExemplarValue>,
}

#[pymethods]
impl Exemplar {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Exemplar {
            filtered_attributes: Arc::new(Mutex::new(vec![])),
            time_unix_nano: 0,
            span_id: vec![],
            trace_id: vec![],
            value: None,
        })
    }

    #[getter]
    fn filtered_attributes(&self) -> PyResult<AttributesList> {
        Ok(AttributesList(self.filtered_attributes.clone()))
    }

    #[setter]
    fn set_filtered_attributes(&mut self, attributes: Vec<KeyValue>) -> PyResult<()> {
        let mut inner = self
            .filtered_attributes
            .lock()
            .map_err(handle_poison_error)?;
        inner.clear();
        for kv in attributes {
            let kv_lock = kv.inner.lock().map_err(handle_poison_error).unwrap();
            inner.push(kv_lock.clone());
        }
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
    fn span_id(&self) -> PyResult<Vec<u8>> {
        Ok(self.span_id.clone())
    }

    #[setter]
    fn set_span_id(&mut self, id: Vec<u8>) -> PyResult<()> {
        self.span_id = id;
        Ok(())
    }

    #[getter]
    fn trace_id(&self) -> PyResult<Vec<u8>> {
        Ok(self.trace_id.clone())
    }

    #[setter]
    fn set_trace_id(&mut self, id: Vec<u8>) -> PyResult<()> {
        self.trace_id = id;
        Ok(())
    }

    #[getter]
    fn value(&self) -> PyResult<Option<ExemplarValue>> {
        Ok(self.value.clone())
    }

    #[setter]
    fn set_value(&mut self, value: Option<ExemplarValue>) -> PyResult<()> {
        self.value = value;
        Ok(())
    }
}

impl From<Exemplar> for RExemplar {
    fn from(e: Exemplar) -> Self {
        RExemplar {
            filtered_attributes: e.filtered_attributes,
            time_unix_nano: e.time_unix_nano,
            span_id: e.span_id,
            trace_id: e.trace_id,
            value: e.value.map(|v| match v {
                ExemplarValue::AsDouble(d) => RExemplarValue::AsDouble(d),
                ExemplarValue::AsInt(i) => RExemplarValue::AsInt(i),
            }),
        }
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
