from typing import Optional

from rotel_sdk.open_telemetry.common.v1 import InstrumentationScope, KeyValue
from rotel_sdk.open_telemetry.resource.v1 import Resource


class ResourceMetrics:
    """
    A collection of ScopeMetrics from a Resource.
    """
    resource: Optional[Resource]
    """
    The resource for the metrics in this message.
    If this field is not set then no resource info is known.
    """
    scope_metrics: list[ScopeMetrics]
    """
    A list of metrics that originate from a resource.
    """
    schema_url: str
    """
    The Schema URL, if known. This is the identifier of the Schema that the resource data
    is recorded in. Notably, the last part of the URL path is the version number of the
    schema: http[s]://server[:port]/path/<version>. To learn more about Schema URL see
    https://opentelemetry.io/docs/specs/otel/schemas/#schema-url
    This schema_url applies to the data in the "resource" field. It does not apply
    to the data in the "scope_metrics" field which have their own schema_url field.
    """


class ScopeMetrics:
    """
    A collection of Metrics produced by a Scope.
    """
    scope: Optional[InstrumentationScope]
    """
    The instrumentation scope information for the metrics in this message.
    Semantically when InstrumentationScope isn't set, it is equivalent with
    an empty instrumentation scope name (unknown).
    """
    metrics: list[Metric]
    """
    A list of metrics that originate from an instrumentation library.
    """
    schema_url: str
    """
    The Schema URL, if known. This is the identifier of the Schema that the metric data
    is recorded in. Notably, the last part of the URL path is the version number of the
    schema: http[s]://server[:port]/path/<version>. To learn more about Schema URL see
    https://opentelemetry.io/docs/specs/otel/schemas/#schema-url
    This schema_url applies to all metrics in the "metrics" field.
    """


class Metric:
    """
    Defines a Metric which has one or more timeseries. The following is a
    brief summary of the Metric data model. For more details, see:

    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md
    """
    name: str
    """
    name of the metric.
    """
    description: str
    """
    description of the metric, which can be used in documentation.
    """
    unit: str
    """
    unit in which the metric value is reported. Follows the format
    described by https://unitsofmeasure.org/ucum.html.
    """
    metadata: list[KeyValue]
    """
    Additional metadata attributes that describe the metric. [Optional].
    Attributes are non-identifying.
    Consumers SHOULD NOT need to be aware of these attributes.
    These attributes MAY be used to encode information allowing
    for lossless roundtrip translation to / from another data model.
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    data: Optional[MetricData]
    """
    Data determines the aggregation type (if any) of the metric, what is the
    reported value type for the data points, as well as the relatationship to
    the time interval over which they are reported.
    """


class MetricData:
    """
    Union type representing different metric data types.
    """
    Gauge = Gauge
    Sum = Sum
    Histogram = Histogram
    ExponentialHistogram = ExponentialHistogram
    Summary = Summary


class Gauge:
    """
    Gauge represents the type of a scalar metric that always exports the
    "current value" for every data point. It should be used for an "unknown"
    aggregation.

    A Gauge does not support different aggregation temporalities. Given the
    aggregation is unknown, points cannot be combined using the same
    aggregation, regardless of aggregation temporalities. Therefore,
    AggregationTemporality is not included. Consequently, this also means
    "StartTimeUnixNano" is ignored for all data points.
    """
    data_points: list[NumberDataPoint]


class Sum:
    """
    Sum represents the type of a scalar metric that is calculated as a sum of all
    reported measurements over a time interval.
    """
    data_points: list[NumberDataPoint]
    aggregation_temporality: AggregationTemporality
    """
    aggregation_temporality describes if the aggregator reports delta changes
    since last report time, or cumulative changes since a fixed start time.
    """
    is_monotonic: bool
    """
    If "true" means that the sum is monotonic.
    """


class Histogram:
    """
    Histogram represents the type of a metric that is calculated by aggregating
    as a Histogram of all reported measurements over a time interval.
    """
    data_points: list[HistogramDataPoint]
    aggregation_temporality: AggregationTemporality
    """
    aggregation_temporality describes if the aggregator reports delta changes
    since last report time, or cumulative changes since a fixed start time.
    """


class ExponentialHistogram:
    """
    ExponentialHistogram represents the type of a metric that is calculated by aggregating
    as a ExponentialHistogram of all reported double measurements over a time interval.
    """
    data_points: list[ExponentialHistogramDataPoint]
    aggregation_temporality: AggregationTemporality
    """
    aggregation_temporality describes if the aggregator reports delta changes
    since last report time, or cumulative changes since a fixed start time.
    """


class Summary:
    """
    Summary metric data are used to convey quantile summaries,
    a Prometheus (see: https://prometheus.io/docs/concepts/metric_types/#summary)
    and OpenMetrics (see: https://github.com/prometheus/OpenMetrics/blob/4dbf6075567ab43296eed941037c12951faafb92/protos/prometheus.proto#L45)
    data type. These data points cannot always be merged in a meaningful way.
    While they can be useful in some applications, histogram data points are
    recommended for new applications.
    Summary metrics do not have an aggregation temporality field. This is
    because the count and sum fields of a SummaryDataPoint are assumed to be
    cumulative values.
    """
    data_points: list[SummaryDataPoint]


class NumberDataPoint:
    """
    NumberDataPoint is a single data point in a timeseries that describes the
    time-varying scalar value of a metric.
    """
    attributes: list[KeyValue]
    """
    The set of key/value pairs that uniquely identify the timeseries from
    where this point belongs. The list may be empty (may contain 0 elements).
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    start_time_unix_nano: int
    """
    StartTimeUnixNano is optional but strongly encouraged, see the
    the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    time_unix_nano: int
    """
    TimeUnixNano is required, see the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    exemplars: list[Exemplar]
    """
    (Optional) List of exemplars collected from
    measurements that were used to form the data point
    """
    flags: int
    """
    Flags that apply to this specific data point. See DataPointFlags
    for the available flags and their meaning.
    """
    value: Optional[NumberDataPointValue]
    """
    The value itself. A point is considered invalid when one of the recognized
    value fields is not present inside this oneof.
    """


class NumberDataPointValue:
    """
    Union type for NumberDataPoint values.
    """

    @staticmethod
    def AsDouble(value: float) -> NumberDataPointValue: ...

    @staticmethod
    def AsInt(value: int) -> NumberDataPointValue: ...


class HistogramDataPoint:
    """
    HistogramDataPoint is a single data point in a timeseries that describes the
    time-varying values of a Histogram. A Histogram contains summary statistics
    for a population of values, it may optionally contain the distribution of
    those values across a set of buckets.

    If the histogram contains the distribution of values, then both
    "explicit_bounds" and "bucket counts" fields must be defined.
    If the histogram does not contain the distribution of values, then both
    "explicit_bounds" and "bucket_counts" must be omitted and only "count" and
    "sum" are known.
    """
    attributes: list[KeyValue]
    """
    The set of key/value pairs that uniquely identify the timeseries from
    where this point belongs. The list may be empty (may contain 0 elements).
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    start_time_unix_nano: int
    """
    StartTimeUnixNano is optional but strongly encouraged, see the
    the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    time_unix_nano: int
    """
    TimeUnixNano is required, see the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    count: int
    """
    count is the number of values in the population. Must be non-negative. This
    value must be equal to the sum of the "count" fields in buckets if a
    histogram is provided.
    """
    sum: Optional[float]
    """
    sum of the values in the population. If count is zero then this field
    must be zero.

    Note: Sum should only be filled out when measuring non-negative discrete
    events, and is assumed to be monotonic over the values of these events.
    Negative events *can* be recorded, but sum should not be filled out when
    doing so. This is specifically to enforce compatibility w/ OpenMetrics,
    see: https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#histogram
    """
    bucket_counts: list[int]
    """
    bucket_counts is an optional field contains the count values of histogram
    for each bucket.

    The sum of the bucket_counts must equal the value in the count field.

    The number of elements in bucket_counts array must be by one greater than
    the number of elements in explicit_bounds array. The exception to this rule
    is when the length of bucket_counts is 0, then the length of explicit_bounds
    must also be 0.
    """
    explicit_bounds: list[float]
    """
    explicit_bounds specifies buckets with explicitly defined bounds for values.

    The boundaries for bucket at index i are:

    (-infinity, explicit_bounds[i]] for i == 0
    (explicit_bounds[i-1], explicit_bounds[i]] for 0 < i < size(explicit_bounds)
    (explicit_bounds[i-1], +infinity) for i == size(explicit_bounds)

    The values in the explicit_bounds array must be strictly increasing.

    Histogram buckets are inclusive of their upper boundary, except the last
    bucket where the boundary is at infinity. This format is intentionally
    compatible with the OpenMetrics histogram definition.

    If bucket_counts length is 0 then explicit_bounds length must also be 0,
    otherwise the data point is invalid.
    """
    exemplars: list[Exemplar]
    """
    (Optional) List of exemplars collected from
    measurements that were used to form the data point
    """
    flags: int
    """
    Flags that apply to this specific data point. See DataPointFlags
    for the available flags and their meaning.
    """
    min: Optional[float]
    """
    min is the minimum value over (start_time, end_time].
    """
    max: Optional[float]
    """
    max is the maximum value over (start_time, end_time].
    """


class ExponentialHistogramDataPoint:
    """
    ExponentialHistogramDataPoint is a single data point in a timeseries that describes the
    time-varying values of a ExponentialHistogram of double values. A ExponentialHistogram contains
    summary statistics for a population of values, it may optionally contain the
    distribution of those values across a set of buckets.
    """
    attributes: list[KeyValue]
    """
    The set of key/value pairs that uniquely identify the timeseries from
    where this point belongs. The list may be empty (may contain 0 elements).
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    start_time_unix_nano: int
    """
    StartTimeUnixNano is optional but strongly encouraged, see the
    the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    time_unix_nano: int
    """
    TimeUnixNano is required, see the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    count: int
    """
    count is the number of values in the population. Must be
    non-negative. This value must be equal to the sum of the "bucket_counts"
    values in the positive and negative Buckets plus the "zero_count" field.
    """
    sum: Optional[float]
    """
    sum of the values in the population. If count is zero then this field
    must be zero.

    Note: Sum should only be filled out when measuring non-negative discrete
    events, and is assumed to be monotonic over the values of these events.
    Negative events *can* be recorded, but sum should not be filled out when
    doing so. This is specifically to enforce compatibility w/ OpenMetrics,
    see: https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#histogram
    """
    scale: int
    """
    scale describes the resolution of the histogram. Boundaries are
    located at powers of the base, where:

      base = (2^(2^-scale))

    The histogram bucket identified by `index`, a signed integer,
    contains values that are greater than (base^index) and
    less than or equal to (base^(index+1)).

    The positive and negative ranges of the histogram are expressed
    separately. Negative values are mapped by their absolute value
    into the negative range using the same scale as the positive range.

    scale is not restricted by the protocol, as the permissible
    values depend on the range of the data.
    """
    zero_count: int
    """
    zero_count is the count of values that are either exactly zero or
    within the region considered zero by the instrumentation at the
    tolerated degree of precision. This bucket stores values that
    cannot be expressed using the standard exponential formula as
    well as values that have been rounded to zero.

    Implementations MAY consider the zero bucket to have probability
    mass equal to (zero_count / count).
    """
    positive: Optional[ExponentialHistogramBuckets]
    """
    positive carries the positive range of exponential bucket counts.
    """
    negative: Optional[ExponentialHistogramBuckets]
    """
    negative carries the negative range of exponential bucket counts.
    """
    flags: int
    """
    Flags that apply to this specific data point. See DataPointFlags
    for the available flags and their meaning.
    """
    exemplars: list[Exemplar]
    """
    (Optional) List of exemplars collected from
    measurements that were used to form the data point
    """
    min: Optional[float]
    """
    min is the minimum value over (start_time, end_time].
    """
    max: Optional[float]
    """
    max is the maximum value over (start_time, end_time].
    """
    zero_threshold: float
    """
    ZeroThreshold may be optionally set to convey the width of the zero
    region. Where the zero region is defined as the closed interval
    [-ZeroThreshold, ZeroThreshold].
    When ZeroThreshold is 0, zero count bucket stores values that cannot be
    expressed using the standard exponential formula as well as values that
    have been rounded to zero.
    """


class ExponentialHistogramBuckets:
    """
    Buckets are a set of bucket counts, encoded in a contiguous array
    of counts.
    """
    offset: int
    """
    Offset is the bucket index of the first entry in the bucket_counts array.

    Note: This uses a varint encoding as a simple form of compression.
    """
    bucket_counts: list[int]
    """
    bucket_counts is an array of count values, where bucket_counts[i] carries
    the count of the bucket at index (offset+i). bucket_counts[i] is the count
    of values greater than base^(offset+i) and less than or equal to
    base^(offset+i+1).

    Note: By contrast, the explicit HistogramDataPoint uses
    fixed64. This field is expected to have many buckets,
    especially zeros, so uint64 has been selected to ensure
    varint encoding.
    """


class SummaryDataPoint:
    """
    SummaryDataPoint is a single data point in a timeseries that describes the
    time-varying values of a Summary metric. The count and sum fields represent
    cumulative values.
    """
    attributes: list[KeyValue]
    """
    The set of key/value pairs that uniquely identify the timeseries from
    where this point belongs. The list may be empty (may contain 0 elements).
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    start_time_unix_nano: int
    """
    StartTimeUnixNano is optional but strongly encouraged, see the
    the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    time_unix_nano: int
    """
    TimeUnixNano is required, see the detailed comments above Metric.

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    count: int
    """
    count is the number of values in the population. Must be non-negative.
    """
    sum: float
    """
    sum of the values in the population. If count is zero then this field
    must be zero.

    Note: Sum should only be filled out when measuring non-negative discrete
    events, and is assumed to be monotonic over the values of these events.
    Negative events *can* be recorded, but sum should not be filled out when
    doing so. This is specifically to enforce compatibility w/ OpenMetrics,
    see: https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#summary
    """
    quantile_values: list[ValueAtQuantile]
    """
    (Optional) list of values at different quantiles of the distribution calculated
    from the current snapshot. The quantiles must be strictly increasing.
    """
    flags: int
    """
    Flags that apply to this specific data point. See DataPointFlags
    for the available flags and their meaning.
    """


class ValueAtQuantile:
    """
    Represents the value at a given quantile of a distribution.

    To record Min and Max values following conventions are used:
    - The 1.0 quantile is equivalent to the maximum value observed.
    - The 0.0 quantile is equivalent to the minimum value observed.

    See the following issue for more context:
    https://github.com/open-telemetry/opentelemetry-proto/issues/125
    """
    quantile: float
    """
    The quantile of a distribution. Must be in the interval
    [0.0, 1.0].
    """
    value: float
    """
    The value at the given quantile of a distribution.

    Quantile values must NOT be negative.
    """


class Exemplar:
    """
    A representation of an exemplar, which is a sample input measurement.
    Exemplars also hold information about the environment when the measurement
    was recorded, for example the span and trace ID of the active span when the
    exemplar was recorded.
    """
    filtered_attributes: list[KeyValue]
    """
    The set of key/value pairs that were filtered out by the aggregator, but
    recorded alongside the original measurement. Only key/value pairs that were
    filtered out by the aggregator should be included
    """
    time_unix_nano: int
    """
    time_unix_nano is the exact time when this exemplar was recorded

    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January
    1970.
    """
    span_id: bytes
    """
    (Optional) Span ID of the exemplar trace.
    span_id may be missing if the measurement is not recorded inside a trace
    or if the trace is not sampled.
    """
    trace_id: bytes
    """
    (Optional) Trace ID of the exemplar trace.
    trace_id may be missing if the measurement is not recorded inside a trace
    or if the trace is not sampled.
    """
    value: Optional[ExemplarValue]
    """
    The value of the measurement that was recorded. An exemplar is
    considered invalid when one of the recognized value fields is not present
    inside this oneof.
    """


class ExemplarValue:
    """
    Union type for Exemplar values.
    """

    @staticmethod
    def AsDouble(value: float) -> ExemplarValue: ...

    @staticmethod
    def AsInt(value: int) -> ExemplarValue: ...


class AggregationTemporality:
    """
    AggregationTemporality defines how a metric aggregator reports aggregated
    values. It describes how those values relate to the time interval over
    which they are aggregated.
    """
    Unspecified: int
    """
    UNSPECIFIED is the default AggregationTemporality, it MUST not be used.
    """
    Delta: int
    """
    DELTA is an AggregationTemporality for a metric aggregator which reports
    changes since last report time. Successive metrics contain aggregation of
    values from continuous and non-overlapping intervals.

    The values for a DELTA metric are based only on the time interval
    associated with one measurement cycle. There is no dependency on
    previous measurements like is the case for CUMULATIVE metrics.
    """
    Cumulative: int
    """
    CUMULATIVE is an AggregationTemporality for a metric aggregator which
    reports changes since a fixed start time. This means that current values
    of a CUMULATIVE metric depend on all previous measurements since the
    start time. Because of this, the sender is required to retain this state
    in some form. If this state is lost or invalidated, the CUMULATIVE metric
    values MUST be reset and a new fixed start time following the last
    reported measurement time sent MUST be used.
    """

    def as_str_name(self) -> str: ...


class DataPointFlags:
    """
    DataPointFlags is defined as a protobuf 'uint32' type and is to be used as a
    bit-field representing 32 distinct boolean flags. Each flag defined in this
    enum is a bit-mask. To test the presence of a single flag in the flags of
    a data point, for example, use an expression like:

      (point.flags & DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK) == DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK
    """
    DoNotUse: int
    """
    The zero value for the enum. Should not be used for comparisons.
    Instead use bitwise "and" with the appropriate mask as shown above.
    """
    NoRecordedValueMask: int
    """
    This DataPoint is valid but has no recorded value. This value
    SHOULD be used to reflect explicitly missing data in a series, as
    for an equivalent to the Prometheus "staleness marker".
    """

    def as_str_name(self) -> str: ...

    @property
    def value(self) -> int: ...
