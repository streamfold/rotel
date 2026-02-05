// SPDX-License-Identifier: Apache-2.0

//! FFI-safe metric types.

use abi_stable::std_types::{ROption, RString, RVec};
use abi_stable::StableAbi;

use super::common::{RInstrumentationScope, RKeyValue, RResource};

/// FFI-safe equivalent of opentelemetry_proto ResourceMetrics
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RResourceMetrics {
    pub resource: ROption<RResource>,
    pub scope_metrics: RVec<RScopeMetrics>,
    pub schema_url: RString,
}

/// FFI-safe equivalent of opentelemetry_proto ScopeMetrics
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RScopeMetrics {
    pub scope: ROption<RInstrumentationScope>,
    pub metrics: RVec<RMetric>,
    pub schema_url: RString,
}

/// FFI-safe equivalent of opentelemetry_proto Metric
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RMetric {
    pub name: RString,
    pub description: RString,
    pub unit: RString,
    pub metadata: RVec<RKeyValue>,
    pub data: ROption<RMetricData>,
}

/// FFI-safe equivalent of opentelemetry_proto metric data variants
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub enum RMetricData {
    Gauge(RGauge),
    Sum(RSum),
    Histogram(RHistogram),
    ExponentialHistogram(RExponentialHistogram),
    Summary(RSummary),
}

/// FFI-safe equivalent of opentelemetry_proto Gauge
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RGauge {
    pub data_points: RVec<RNumberDataPoint>,
}

/// FFI-safe equivalent of opentelemetry_proto Sum
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RSum {
    pub data_points: RVec<RNumberDataPoint>,
    pub aggregation_temporality: i32,
    pub is_monotonic: bool,
}

/// FFI-safe equivalent of opentelemetry_proto Histogram
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RHistogram {
    pub data_points: RVec<RHistogramDataPoint>,
    pub aggregation_temporality: i32,
}

/// FFI-safe equivalent of opentelemetry_proto ExponentialHistogram
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RExponentialHistogram {
    pub data_points: RVec<RExponentialHistogramDataPoint>,
    pub aggregation_temporality: i32,
}

/// FFI-safe equivalent of opentelemetry_proto Summary
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RSummary {
    pub data_points: RVec<RSummaryDataPoint>,
}

/// FFI-safe equivalent of opentelemetry_proto NumberDataPoint
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RNumberDataPoint {
    pub attributes: RVec<RKeyValue>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub value: RNumberValue,
    pub exemplars: RVec<RExemplar>,
    pub flags: u32,
}

/// FFI-safe number value (either i64 or f64)
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub enum RNumberValue {
    Int(i64),
    Double(f64),
}

/// FFI-safe equivalent of opentelemetry_proto HistogramDataPoint
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RHistogramDataPoint {
    pub attributes: RVec<RKeyValue>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: ROption<f64>,
    pub bucket_counts: RVec<u64>,
    pub explicit_bounds: RVec<f64>,
    pub exemplars: RVec<RExemplar>,
    pub flags: u32,
    pub min: ROption<f64>,
    pub max: ROption<f64>,
}

/// FFI-safe equivalent of opentelemetry_proto ExponentialHistogramDataPoint
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RExponentialHistogramDataPoint {
    pub attributes: RVec<RKeyValue>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: ROption<f64>,
    pub scale: i32,
    pub zero_count: u64,
    pub positive: RBuckets,
    pub negative: RBuckets,
    pub flags: u32,
    pub exemplars: RVec<RExemplar>,
    pub min: ROption<f64>,
    pub max: ROption<f64>,
    pub zero_threshold: f64,
}

/// FFI-safe equivalent of opentelemetry_proto Buckets
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RBuckets {
    pub offset: i32,
    pub bucket_counts: RVec<u64>,
}

/// FFI-safe equivalent of opentelemetry_proto SummaryDataPoint
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RSummaryDataPoint {
    pub attributes: RVec<RKeyValue>,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    pub count: u64,
    pub sum: f64,
    pub quantile_values: RVec<RQuantileValue>,
    pub flags: u32,
}

/// FFI-safe equivalent of opentelemetry_proto ValueAtQuantile
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RQuantileValue {
    pub quantile: f64,
    pub value: f64,
}

/// FFI-safe equivalent of opentelemetry_proto Exemplar
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RExemplar {
    pub filtered_attributes: RVec<RKeyValue>,
    pub time_unix_nano: u64,
    pub value: RNumberValue,
    pub span_id: RVec<u8>,
    pub trace_id: RVec<u8>,
}
