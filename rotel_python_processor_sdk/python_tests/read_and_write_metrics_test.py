# read_and_write_metrics_test.py

from rotel_sdk.open_telemetry.metrics.v1 import (
    Metric,
    MetricData,
    Sum,
    Histogram,
    NumberDataPoint,
    HistogramDataPoint,
    Exemplar,
    AggregationTemporality,
    DataPointFlags,
    NumberDataPointValue,
    ExemplarValue
)

from rotel_sdk.open_telemetry.common.v1 import KeyValue


def process_metrics(resource_metrics):
    """
    Processes and mutates a ResourceMetrics object received from Rust.
    This function asserts initial states and then modifies the metrics data.
    """

    # Assert initial state of ResourceMetrics (similar to how it would come from Rust)
    assert resource_metrics.schema_url == "initial_schema_url_resource"
    assert resource_metrics.resource is not None
    assert resource_metrics.resource.dropped_attributes_count == 0
    assert len(resource_metrics.scope_metrics) == 1

    scope_metrics = resource_metrics.scope_metrics[0]
    assert scope_metrics.schema_url == "initial_schema_url_scope"
    assert scope_metrics.scope is not None
    assert scope_metrics.scope.name == "initial_scope_name"
    assert len(scope_metrics.metrics) == 1

    metric = scope_metrics.metrics[0]
    assert metric.name == "initial_metric_name"
    assert metric.description == "initial_description"
    assert metric.unit == "initial_unit"
    assert len(metric.metadata) == 1
    assert metric.metadata[0].key == "initial_metadata_key"
    assert metric.metadata[0].value.value == "initial_metadata_value"
    assert metric.data is not None

    # Assume it's a Gauge for initial state
    assert isinstance(metric.data, MetricData.Gauge)
    gauge = metric.data[0]
    assert len(gauge.data_points) == 1

    num_dp = gauge.data_points[0]
    assert num_dp.start_time_unix_nano == 1000
    assert num_dp.time_unix_nano == 2000
    assert num_dp.value[0] == 123.45
    assert len(num_dp.attributes) == 1
    assert num_dp.attributes[0].key == "dp_key"
    assert num_dp.attributes[0].value.value == "dp_value"
    assert num_dp.flags == 0
    assert len(num_dp.exemplars) == 0

    # let's change the start_time_unix_nano
    num_dp.start_time_unix_nano = 2000
    num_dp.time_unix_nano = 3000
    num_dp.attributes.append(KeyValue.new_string_value("dp_key2", "dp_value2"))
    exemplar = Exemplar()
    exemplar.time_unix_nano = 3500
    exemplar.span_id = b"\x01\x02\x03\x04\x05\x06\x07\x08"
    exemplar.trace_id = b"\x08\x07\x06\x05\x04\x03\x02\x01\x08\x07\x06\x05\x04\x03\x02\x01"
    exemplar.value = ExemplarValue.AsDouble(10.5)
    exemplar.filtered_attributes.append(KeyValue.new_string_value("filtered_key", "filtered_value"))
    num_dp.exemplars.append(exemplar)
    num_dp = gauge.data_points[0]
    num_dp.flags = 1
    num_dp.value = NumberDataPointValue.AsInt(111)
    assert num_dp.start_time_unix_nano == 2000
    assert num_dp.time_unix_nano == 3000
    assert len(num_dp.attributes) == 2
    del (num_dp.attributes[0])
    assert len(num_dp.attributes) == 1
    assert num_dp.attributes[0].key == "dp_key2"
    assert num_dp.attributes[0].value.value == "dp_value2"
    assert len(num_dp.exemplars) == 1
    assert num_dp.flags == 1
    assert num_dp.value[0] == 111

    # --- Mutate the metrics data ---

    # Mutate ResourceMetrics
    resource_metrics.schema_url = "py_processed_schema_url_resource"
    resource_metrics.resource.dropped_attributes_count = 5

    # Mutate ScopeMetrics
    scope_metrics.schema_url = "py_processed_schema_url_scope"
    scope_metrics.scope.name = "py_processed_scope_name"
    scope_metrics.scope.version = "py_processed_scope_version"
    scope_metrics.scope.attributes.append(KeyValue.new_bool_value("scope_attr_py", True))
    scope_metrics.scope.dropped_attributes_count = 1

    # Mutate Metric
    metric.name = "py_processed_metric_name"
    metric.description = "py_processed_description"
    metric.unit = "py_processed_unit"
    metric.metadata.append(KeyValue.new_int_value("metadata_py", 999))

    # Change metric type to Sum and add new data points
    new_sum = Sum()
    new_sum.aggregation_temporality = AggregationTemporality.Cumulative
    new_sum.is_monotonic = True

    sum_dp1 = NumberDataPoint()
    sum_dp1.start_time_unix_nano = 3000
    sum_dp1.time_unix_nano = 4000
    sum_dp1.value = NumberDataPointValue.AsInt(10)
    sum_dp1.attributes.append(KeyValue.new_string_value("sum_dp_key", "sum_dp_value"))

    sum_dp2 = NumberDataPoint()
    sum_dp2.start_time_unix_nano = 5000
    sum_dp2.time_unix_nano = 6000
    sum_dp2.value = NumberDataPointValue.AsInt(20)
    sum_dp2.flags = DataPointFlags.NoRecordedValueMask.value

    new_sum.data_points.append(sum_dp1)
    new_sum.data_points.append(sum_dp2)

    # Add an exemplar to sum_dp1
    exemplar_sum = Exemplar()
    exemplar_sum.time_unix_nano = 3500
    exemplar_sum.span_id = b"\x01\x02\x03\x04\x05\x06\x07\x08"
    exemplar_sum.trace_id = b"\x08\x07\x06\x05\x04\x03\x02\x01\x08\x07\x06\x05\x04\x03\x02\x01"
    exemplar_sum.value = ExemplarValue.AsDouble(10.5)
    exemplar_sum.filtered_attributes.append(KeyValue.new_string_value("filtered_key", "filtered_value"))
    sum_dp1.exemplars.append(exemplar_sum)

    metric.data = MetricData.Sum(new_sum)

    # Add a new metric (Histogram) to the scope_metrics list
    new_metric_histogram = Metric()
    new_metric_histogram.name = "py_processed_histogram"
    new_metric_histogram.description = "A histogram from Python"
    new_metric_histogram.unit = "ms"

    hist_data = Histogram()
    hist_data.aggregation_temporality = AggregationTemporality.Delta

    hist_dp = HistogramDataPoint()
    hist_dp.start_time_unix_nano = 7000
    hist_dp.time_unix_nano = 8000
    hist_dp.count = 100
    hist_dp.sum = 1000.0
    hist_dp.bucket_counts = [10, 20, 30, 40]
    hist_dp.explicit_bounds = [1.0, 5.0, 10.0]
    hist_dp.min = 0.5
    hist_dp.max = 12.0
    hist_dp.attributes.append(KeyValue.new_string_value("hist_attr", "hist_value"))

    hist_exemplar = Exemplar()
    hist_exemplar.time_unix_nano = 7500
    hist_exemplar.value = ExemplarValue.AsInt(75)
    hist_dp.exemplars.append(hist_exemplar)

    hist_data.data_points.append(hist_dp)
    new_metric_histogram.data = MetricData.Histogram(hist_data)
    scope_metrics.metrics.append(new_metric_histogram)

    # Verify mutations
    assert resource_metrics.schema_url == "py_processed_schema_url_resource"
    assert resource_metrics.resource.dropped_attributes_count == 5
    assert scope_metrics.schema_url == "py_processed_schema_url_scope"
    assert scope_metrics.scope.name == "py_processed_scope_name"
    assert scope_metrics.scope.version == "py_processed_scope_version"
    assert len(scope_metrics.scope.attributes) == 2
    assert scope_metrics.scope.attributes[1].key == "scope_attr_py"
    assert scope_metrics.scope.attributes[1].value.value is True
    assert scope_metrics.scope.dropped_attributes_count == 1

    assert metric.name == "py_processed_metric_name"
    assert metric.description == "py_processed_description"
    assert metric.unit == "py_processed_unit"
    assert len(metric.metadata) == 2
    assert metric.metadata[1].key == "metadata_py"
    assert metric.metadata[1].value.value == 999

    assert isinstance(metric.data, MetricData.Sum)
    processed_sum = metric.data[0]
    assert int(processed_sum.aggregation_temporality) == int(AggregationTemporality.Cumulative)
    assert processed_sum.is_monotonic is True
    assert len(processed_sum.data_points) == 2
    assert processed_sum.data_points[0].value[0] == 10
    assert processed_sum.data_points[1].value[0] == 20
    assert processed_sum.data_points[0].exemplars[0].value[0] == 10.5
    assert processed_sum.data_points[0].exemplars[0].filtered_attributes[0].value.value == "filtered_value"

    assert len(scope_metrics.metrics) == 2
    processed_hist_metric = scope_metrics.metrics[1]
    assert processed_hist_metric.name == "py_processed_histogram"
    assert isinstance(processed_hist_metric.data, MetricData.Histogram)
    processed_hist_data = processed_hist_metric.data[0]
    assert int(processed_hist_data.aggregation_temporality) == int(AggregationTemporality.Delta)
    assert len(processed_hist_data.data_points) == 1
    assert processed_hist_data.data_points[0].count == 100
    assert processed_hist_data.data_points[0].sum == 1000.0
    assert processed_hist_data.data_points[0].bucket_counts == [10, 20, 30, 40]
    assert processed_hist_data.data_points[0].explicit_bounds == [1.0, 5.0, 10.0]
    assert processed_hist_data.data_points[0].min == 0.5
    assert processed_hist_data.data_points[0].max == 12.0
    assert processed_hist_data.data_points[0].exemplars[0].value[0] == 75
