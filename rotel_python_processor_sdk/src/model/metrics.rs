// metrics.rs

use crate::model::common::*;
use crate::model::resource::RResource;
use std::sync::{Arc, Mutex};

/// A collection of RScopeMetrics from a RResource.
#[derive(Debug, Clone)]
pub struct RResourceMetrics {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_metrics: Arc<Mutex<Vec<Arc<Mutex<RScopeMetrics>>>>>,
    pub schema_url: String,
}

/// A collection of Metrics produced by an RInstrumentationScope.
#[derive(Debug, Clone)]
pub struct RScopeMetrics {
    pub scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    pub metrics: Arc<Mutex<Vec<Arc<Mutex<RMetric>>>>>,
    pub schema_url: Arc<Mutex<String>>,
}

/// Defines a RMetric which has one or more timeseries.
#[derive(Debug, Clone)]
pub struct RMetric {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub metadata: Arc<Mutex<Vec<RKeyValue>>>,
    pub data: Arc<Mutex<Option<RMetricData>>>,
}

/// RMetricData determines the aggregation type (if any) of the metric.
#[derive(Debug, Clone)]
pub enum RMetricData {
    Gauge(RGauge),
    Sum(RSum),
    Histogram(RHistogram),
    ExponentialHistogram(RExponentialHistogram),
    Summary(RSummary),
}

/// RGauge represents the type of a scalar metric that always exports the
/// "current value" for every data point.
#[derive(Debug, Clone)]
pub struct RGauge {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RNumberDataPoint>>>>>,
}

/// RSum represents the type of a scalar metric that is calculated as a sum of all
/// reported measurements over a time interval.
#[derive(Debug, Clone)]
pub struct RSum {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RNumberDataPoint>>>>>,
    pub aggregation_temporality: i32,
    pub is_monotonic: bool,
}

/// RHistogram represents the type of a metric that is calculated by aggregating
/// as a Histogram of all reported measurements over a time interval.
#[derive(Debug, Clone)]
pub struct RHistogram {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RHistogramDataPoint>>>>>,
    pub aggregation_temporality: i32,
}

/// RExponentialHistogram represents the type of a metric that is calculated by aggregating
/// as a ExponentialHistogram of all reported double measurements over a time interval.
#[derive(Debug, Clone)]
pub struct RExponentialHistogram {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RExponentialHistogramDataPoint>>>>>,
    pub aggregation_temporality: i32,
}

/// RSummary metric data are used to convey quantile summaries.
#[derive(Debug, Clone)]
pub struct RSummary {
    pub data_points: Arc<Mutex<Vec<Arc<Mutex<RSummaryDataPoint>>>>>,
}

/// RNumberDataPoint is a single data point in a timeseries that describes the
/// time-varying scalar value of a metric.
#[derive(Debug, Clone)]
pub struct RNumberDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub exemplars: Arc<Mutex<Vec<Arc<Mutex<RExemplar>>>>>,
    pub flags: u32,
    pub value: Option<RNumberDataPointValue>,
}

/// RNumberDataPointValue is the value itself for a NumberDataPoint.
#[derive(Debug, Clone)]
pub enum RNumberDataPointValue {
    AsDouble(f64),
    AsInt(i64),
}

/// RHistogramDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a Histogram.
#[derive(Debug, Clone)]
pub struct RHistogramDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: Option<f64>,
    // TODO these should be an ARC?
    pub bucket_counts: Vec<u64>,
    pub explicit_bounds: Vec<f64>,
    pub exemplars: Arc<Mutex<Vec<Arc<Mutex<RExemplar>>>>>,
    pub flags: u32,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// RExponentialHistogramDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a ExponentialHistogram of double values.
#[derive(Debug, Clone)]
pub struct RExponentialHistogramDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: Option<f64>,
    pub scale: i32,
    pub zero_count: u64,
    pub positive: Arc<Mutex<Option<Arc<Mutex<RExponentialHistogramBuckets>>>>>,
    pub negative: Arc<Mutex<Option<Arc<Mutex<RExponentialHistogramBuckets>>>>>,
    pub flags: u32,
    pub exemplars: Arc<Mutex<Vec<Arc<Mutex<RExemplar>>>>>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub zero_threshold: f64,
}

/// RExtendedExponentialHistogramBuckets are a set of bucket counts.
#[derive(Debug, Clone)]
pub struct RExponentialHistogramBuckets {
    pub offset: i32,
    pub bucket_counts: Vec<u64>,
}

/// RSummaryDataPoint is a single data point in a timeseries that describes the
/// time-varying values of a Summary metric.
#[derive(Debug, Clone)]
pub struct RSummaryDataPoint {
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: f64,
    pub quantile_values: Vec<RValueAtQuantile>,
    pub flags: u32,
}

/// RValueAtQuantile represents the value at a given quantile of a distribution.
#[derive(Debug, Clone)]
pub struct RValueAtQuantile {
    pub quantile: f64,
    pub value: f64,
}

/// RExemplar is a representation of an exemplar, which is a sample input measurement.
#[derive(Debug, Clone)]
pub struct RExemplar {
    pub filtered_attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub time_unix_nano: u64,
    pub span_id: Vec<u8>,
    pub trace_id: Vec<u8>,
    pub value: Option<RExemplarValue>,
}

/// RExemplarValue is the value of the measurement that was recorded.
#[derive(Debug, Clone)]
pub enum RExemplarValue {
    AsDouble(f64),
    AsInt(i64),
}

/// RAggregationTemporality defines how a metric aggregator reports aggregated values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum RAggregationTemporality {
    Unspecified = 0,
    Delta = 1,
    Cumulative = 2,
}

/// RDataPointFlags is defined as a protobuf 'uint32' type and is to be used as a
/// bit-field representing 32 distinct boolean flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum RDataPointFlags {
    DoNotUse = 0,
    NoRecordedValueMask = 1,
}
