# read_and_write_metrics_test.py

from rotel_sdk.open_telemetry.metrics.v1 import (
    MetricData,
    ExponentialHistogramBuckets,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    SummaryDataPoint,
    ValueAtQuantile,
    Exemplar,
    AggregationTemporality,
    NumberDataPointValue,
    ExemplarValue
)

from rotel_sdk.open_telemetry.common.v1 import KeyValue


def process_metrics(resource_metrics):
    """
    1. Verifies ALL initial metric values created by Rust
    2. Mutates ALL metric types and their properties
    3. Verifies all mutations were successful
    """

    print("=== STEP 1: VERIFY ALL INITIAL VALUES FROM RUST ===")

    # --- Verify ResourceMetrics initial state ---
    assert resource_metrics.schema_url == "initial_schema_url_resource"
    assert resource_metrics.resource is not None
    assert resource_metrics.resource.dropped_attributes_count == 10
    assert len(resource_metrics.resource.attributes) == 1
    assert resource_metrics.resource.attributes[0].key == "initial_resource_key"
    assert resource_metrics.resource.attributes[0].value.value == "initial_resource_value"

    # --- Verify ScopeMetrics initial state ---
    assert len(resource_metrics.scope_metrics) == 1
    scope_metrics = resource_metrics.scope_metrics[0]
    assert scope_metrics.schema_url == "initial_schema_url_scope"
    assert scope_metrics.scope is not None
    assert scope_metrics.scope.name == "initial_scope_name"
    assert scope_metrics.scope.version == "1.0.0"
    assert scope_metrics.scope.dropped_attributes_count == 5
    assert len(scope_metrics.scope.attributes) == 1
    assert scope_metrics.scope.attributes[0].key == "initial_scope_key"
    assert scope_metrics.scope.attributes[0].value.value is True

    # Should have 5 metrics: Gauge, Sum, Histogram, ExponentialHistogram, Summary
    assert len(scope_metrics.metrics) == 5

    # --- Verify Gauge metric initial state ---
    gauge_metric = scope_metrics.metrics[0]
    assert gauge_metric.name == "initial_gauge_metric"
    assert gauge_metric.description == "initial_gauge_description"
    assert gauge_metric.unit == "initial_gauge_unit"
    assert len(gauge_metric.metadata) == 1
    assert gauge_metric.metadata[0].key == "gauge_metadata_key"
    assert gauge_metric.metadata[0].value.value == "gauge_metadata_value"
    assert isinstance(gauge_metric.data, MetricData.Gauge)

    gauge_data = gauge_metric.data[0]
    assert len(gauge_data.data_points) == 1
    gauge_dp = gauge_data.data_points[0]
    assert gauge_dp.start_time_unix_nano == 1000
    assert gauge_dp.time_unix_nano == 2000
    assert gauge_dp.flags == 0
    assert gauge_dp.value[0] == 123.45
    assert len(gauge_dp.attributes) == 1
    assert gauge_dp.attributes[0].key == "gauge_dp_key"
    assert gauge_dp.attributes[0].value.value == "gauge_dp_value"
    assert len(gauge_dp.exemplars) == 1
    assert gauge_dp.exemplars[0].time_unix_nano == 1500
    assert gauge_dp.exemplars[0].value[0] == 15.5
    assert len(gauge_dp.exemplars[0].filtered_attributes) == 1
    assert gauge_dp.exemplars[0].filtered_attributes[0].key == "gauge_exemplar_key"

    # --- Verify Sum metric initial state ---
    sum_metric = scope_metrics.metrics[1]
    assert sum_metric.name == "initial_sum_metric"
    assert sum_metric.description == "initial_sum_description"
    assert sum_metric.unit == "initial_sum_unit"
    assert len(sum_metric.metadata) == 1
    assert sum_metric.metadata[0].key == "sum_metadata_key"
    assert sum_metric.metadata[0].value.value == 42
    assert isinstance(sum_metric.data, MetricData.Sum)

    sum_data = sum_metric.data[0]
    assert int(sum_data.aggregation_temporality) == int(AggregationTemporality.Delta)
    assert sum_data.is_monotonic is True
    assert len(sum_data.data_points) == 1
    sum_dp = sum_data.data_points[0]
    assert sum_dp.start_time_unix_nano == 3000
    assert sum_dp.time_unix_nano == 4000
    assert sum_dp.flags == 0
    assert sum_dp.value[0] == 100
    assert len(sum_dp.attributes) == 1
    assert sum_dp.attributes[0].key == "sum_dp_key"
    assert len(sum_dp.exemplars) == 0

    # --- Verify Histogram metric initial state ---
    hist_metric = scope_metrics.metrics[2]
    assert hist_metric.name == "initial_histogram_metric"
    assert hist_metric.description == "initial_histogram_description"
    assert hist_metric.unit == "initial_histogram_unit"
    assert len(hist_metric.metadata) == 1
    assert hist_metric.metadata[0].key == "histogram_metadata_key"
    assert hist_metric.metadata[0].value.value is False
    assert isinstance(hist_metric.data, MetricData.Histogram)

    hist_data = hist_metric.data[0]
    assert int(hist_data.aggregation_temporality) == int(AggregationTemporality.Cumulative)
    assert len(hist_data.data_points) == 1
    hist_dp = hist_data.data_points[0]
    assert hist_dp.start_time_unix_nano == 5000
    assert hist_dp.time_unix_nano == 6000
    assert hist_dp.count == 50
    assert hist_dp.sum == 500.0
    assert hist_dp.bucket_counts == [5, 10, 15, 20]
    assert hist_dp.explicit_bounds == [1.0, 5.0, 10.0]
    assert hist_dp.flags == 1
    assert hist_dp.min == 0.1
    assert hist_dp.max == 20.0
    assert len(hist_dp.attributes) == 1
    assert hist_dp.attributes[0].key == "histogram_dp_key"
    assert len(hist_dp.exemplars) == 1
    assert hist_dp.exemplars[0].time_unix_nano == 5500
    assert hist_dp.exemplars[0].value[0] == 25

    # --- Verify ExponentialHistogram metric initial state ---
    exp_hist_metric = scope_metrics.metrics[3]
    assert exp_hist_metric.name == "initial_exp_histogram_metric"
    assert exp_hist_metric.description == "initial_exp_histogram_description"
    assert exp_hist_metric.unit == "initial_exp_histogram_unit"
    assert len(exp_hist_metric.metadata) == 1
    assert exp_hist_metric.metadata[0].key == "exp_histogram_metadata_key"
    assert exp_hist_metric.metadata[0].value.value == 3.14
    assert isinstance(exp_hist_metric.data, MetricData.ExponentialHistogram)

    exp_hist_data = exp_hist_metric.data[0]
    assert int(exp_hist_data.aggregation_temporality) == int(AggregationTemporality.Delta)
    assert len(exp_hist_data.data_points) == 1
    exp_hist_dp = exp_hist_data.data_points[0]
    assert exp_hist_dp.start_time_unix_nano == 7000
    assert exp_hist_dp.time_unix_nano == 8000
    assert exp_hist_dp.count == 75
    assert exp_hist_dp.sum == 750.0
    assert exp_hist_dp.scale == 1
    assert exp_hist_dp.zero_count == 3
    assert exp_hist_dp.zero_threshold == 0.01
    assert exp_hist_dp.flags == 0
    assert exp_hist_dp.min == 0.5
    assert exp_hist_dp.max == 50.0
    assert len(exp_hist_dp.attributes) == 1
    assert exp_hist_dp.attributes[0].key == "exp_histogram_dp_key"
    assert len(exp_hist_dp.exemplars) == 1
    assert exp_hist_dp.exemplars[0].time_unix_nano == 7500
    assert exp_hist_dp.exemplars[0].value[0] == 37.5

    # Verify positive buckets
    assert exp_hist_dp.positive is not None
    assert exp_hist_dp.positive.offset == 2
    assert exp_hist_dp.positive.bucket_counts == [2, 4, 6, 8]

    # Verify negative buckets
    assert exp_hist_dp.negative is not None
    assert exp_hist_dp.negative.offset == -1
    assert exp_hist_dp.negative.bucket_counts == [1, 2, 3]

    # --- Verify Summary metric initial state ---
    summary_metric = scope_metrics.metrics[4]
    assert summary_metric.name == "initial_summary_metric"
    assert summary_metric.description == "initial_summary_description"
    assert summary_metric.unit == "initial_summary_unit"
    assert len(summary_metric.metadata) == 1
    assert summary_metric.metadata[0].key == "summary_metadata_key"
    assert bytes(summary_metric.metadata[0].value.value) == b"summary_bytes"
    assert isinstance(summary_metric.data, MetricData.Summary)

    summary_data = summary_metric.data[0]
    assert len(summary_data.data_points) == 1
    summary_dp = summary_data.data_points[0]
    assert summary_dp.start_time_unix_nano == 9000
    assert summary_dp.time_unix_nano == 10000
    assert summary_dp.count == 25
    assert summary_dp.sum == 250.0
    assert summary_dp.flags == 1
    assert len(summary_dp.attributes) == 1
    assert summary_dp.attributes[0].key == "summary_dp_key"
    assert len(summary_dp.quantile_values) == 3
    assert summary_dp.quantile_values[0].quantile == 0.5
    assert summary_dp.quantile_values[0].value == 10.0
    assert summary_dp.quantile_values[1].quantile == 0.95
    assert summary_dp.quantile_values[1].value == 19.0
    assert summary_dp.quantile_values[2].quantile == 0.99
    assert summary_dp.quantile_values[2].value == 19.8

    print("✓ All initial values verified successfully!")

    print("\n=== STEP 2: MUTATE ALL METRIC TYPES ===")

    # --- Mutate ResourceMetrics ---
    resource_metrics.schema_url = "python_modified_schema_url_resource"
    resource_metrics.resource.dropped_attributes_count = 25
    resource_metrics.resource.attributes.append(
        KeyValue.new_string_value("python_added_resource_key", "python_added_resource_value"))

    # --- Mutate ScopeMetrics ---
    scope_metrics.schema_url = "python_modified_schema_url_scope"
    scope_metrics.scope.name = "python_modified_scope_name"
    scope_metrics.scope.version = "python_modified_scope_version"
    scope_metrics.scope.dropped_attributes_count = 15
    scope_metrics.scope.attributes.append(KeyValue.new_bool_value("python_added_scope_key", False))

    # --- Mutate Gauge metric ---
    gauge_metric.name = "python_modified_gauge_metric"
    gauge_metric.description = "python_modified_gauge_description"
    gauge_metric.unit = "python_modified_gauge_unit"
    gauge_metric.metadata.append(KeyValue.new_double_value("python_added_gauge_metadata", 99.99))

    # Mutate existing gauge data point
    gauge_dp.start_time_unix_nano = 1100
    gauge_dp.time_unix_nano = 2200
    gauge_dp.flags = 1
    gauge_dp.value = NumberDataPointValue.AsInt(200)
    gauge_dp.attributes.append(KeyValue.new_string_value("python_added_gauge_dp_attr", "python_value"))

    # Add exemplar to gauge
    new_gauge_exemplar = Exemplar()
    new_gauge_exemplar.time_unix_nano = 1750
    new_gauge_exemplar.span_id = b"\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
    new_gauge_exemplar.trace_id = b"\x10\x0f\x0e\x0d\x0c\x0b\x0a\x09\x10\x0f\x0e\x0d\x0c\x0b\x0a\x09"
    new_gauge_exemplar.value = ExemplarValue.AsInt(175)
    new_gauge_exemplar.filtered_attributes.append(
        KeyValue.new_string_value("new_gauge_exemplar_key", "new_gauge_exemplar_value"))
    gauge_dp.exemplars.append(new_gauge_exemplar)

    # Add new gauge data point
    new_gauge_dp = NumberDataPoint()
    new_gauge_dp.start_time_unix_nano = 2000
    new_gauge_dp.time_unix_nano = 3000
    new_gauge_dp.value = NumberDataPointValue.AsDouble(300.0)
    new_gauge_dp.flags = 0
    new_gauge_dp.attributes.append(KeyValue.new_string_value("new_gauge_dp_key", "new_gauge_dp_value"))
    gauge_data.data_points.append(new_gauge_dp)

    # --- Mutate Sum metric ---
    sum_metric.name = "python_modified_sum_metric"
    sum_metric.description = "python_modified_sum_description"
    sum_metric.unit = "python_modified_sum_unit"
    sum_metric.metadata.append(KeyValue.new_int_value("python_added_sum_metadata", 777))

    # Change sum properties
    sum_data.aggregation_temporality = AggregationTemporality.Cumulative
    sum_data.is_monotonic = False

    # Mutate existing sum data point
    sum_dp.start_time_unix_nano = 3300
    sum_dp.time_unix_nano = 4400
    sum_dp.value = NumberDataPointValue.AsInt(150)
    sum_dp.flags = 1
    sum_dp.attributes.append(KeyValue.new_string_value("python_added_sum_dp_attr", "python_sum_value"))

    # Add exemplar to sum
    sum_exemplar = Exemplar()
    sum_exemplar.time_unix_nano = 3700
    sum_exemplar.value = ExemplarValue.AsDouble(125.5)
    sum_exemplar.span_id = b"\x31\x32\x33\x34\x35\x36\x37\x38"
    sum_exemplar.trace_id = b"\x38\x37\x36\x35\x34\x33\x32\x31\x38\x37\x36\x35\x34\x33\x32\x31"
    sum_exemplar.filtered_attributes.append(KeyValue.new_string_value("sum_exemplar_key", "sum_exemplar_value"))
    sum_dp.exemplars.append(sum_exemplar)

    # Add new sum data point
    new_sum_dp = NumberDataPoint()
    new_sum_dp.start_time_unix_nano = 4000
    new_sum_dp.time_unix_nano = 5000
    new_sum_dp.value = NumberDataPointValue.AsDouble(250.5)
    new_sum_dp.flags = 0
    new_sum_dp.attributes.append(KeyValue.new_string_value("new_sum_dp_key", "new_sum_dp_value"))
    sum_data.data_points.append(new_sum_dp)

    # --- Mutate Histogram metric ---
    hist_metric.name = "python_modified_histogram_metric"
    hist_metric.description = "python_modified_histogram_description"
    hist_metric.unit = "python_modified_histogram_unit"
    hist_metric.metadata.append(KeyValue.new_bool_value("python_added_histogram_metadata", True))

    # Change histogram properties
    hist_data.aggregation_temporality = AggregationTemporality.Delta

    # Mutate existing histogram data point
    hist_dp.start_time_unix_nano = 5500
    hist_dp.time_unix_nano = 6600
    hist_dp.count = 75
    hist_dp.sum = 750.0
    hist_dp.bucket_counts = [10, 20, 30, 40]
    hist_dp.explicit_bounds = [2.0, 10.0, 20.0]
    hist_dp.flags = 0
    hist_dp.min = 0.2
    hist_dp.max = 25.0
    hist_dp.attributes.append(KeyValue.new_string_value("python_added_histogram_dp_attr", "python_hist_value"))

    # Add exemplar to histogram
    hist_exemplar = Exemplar()
    hist_exemplar.time_unix_nano = 6000
    hist_exemplar.value = ExemplarValue.AsInt(30)
    hist_exemplar.span_id = b"\x41\x42\x43\x44\x45\x46\x47\x48"
    hist_exemplar.trace_id = b"\x48\x47\x46\x45\x44\x43\x42\x41\x48\x47\x46\x45\x44\x43\x42\x41"
    hist_exemplar.filtered_attributes.append(KeyValue.new_string_value("hist_exemplar_key", "hist_exemplar_value"))
    hist_dp.exemplars.append(hist_exemplar)

    # Add new histogram data point
    new_hist_dp = HistogramDataPoint()
    new_hist_dp.start_time_unix_nano = 6000
    new_hist_dp.time_unix_nano = 7000
    new_hist_dp.count = 100
    new_hist_dp.sum = 1000.0
    new_hist_dp.bucket_counts = [15, 25, 35, 45]
    new_hist_dp.explicit_bounds = [3.0, 15.0, 30.0]
    new_hist_dp.flags = 1
    new_hist_dp.min = 0.3
    new_hist_dp.max = 35.0
    new_hist_dp.attributes.append(KeyValue.new_string_value("new_hist_dp_key", "new_hist_dp_value"))
    hist_data.data_points.append(new_hist_dp)

    # --- Mutate ExponentialHistogram metric ---
    exp_hist_metric.name = "python_modified_exp_histogram_metric"
    exp_hist_metric.description = "python_modified_exp_histogram_description"
    exp_hist_metric.unit = "python_modified_exp_histogram_unit"
    exp_hist_metric.metadata.append(KeyValue.new_bytes_value("python_added_exp_histogram_metadata", b"exp_hist_bytes"))

    # Change exp histogram properties
    exp_hist_data.aggregation_temporality = AggregationTemporality.Cumulative

    # Mutate existing exp histogram data point
    exp_hist_dp.start_time_unix_nano = 7700
    exp_hist_dp.time_unix_nano = 8800
    exp_hist_dp.count = 100
    exp_hist_dp.sum = 1000.0
    exp_hist_dp.scale = 2
    exp_hist_dp.zero_count = 5
    exp_hist_dp.zero_threshold = 0.02
    exp_hist_dp.flags = 1
    exp_hist_dp.min = 0.6
    exp_hist_dp.max = 60.0
    exp_hist_dp.attributes.append(
        KeyValue.new_string_value("python_added_exp_histogram_dp_attr", "python_exp_hist_value"))

    # Mutate positive buckets
    exp_hist_dp.positive.offset = 3
    exp_hist_dp.positive.bucket_counts = [4, 8, 12, 16]

    # Mutate negative buckets
    exp_hist_dp.negative.offset = -2
    exp_hist_dp.negative.bucket_counts = [2, 4, 6]

    # Add exemplar to exp histogram
    exp_hist_exemplar = Exemplar()
    exp_hist_exemplar.time_unix_nano = 8000
    exp_hist_exemplar.value = ExemplarValue.AsDouble(50.0)
    exp_hist_exemplar.span_id = b"\x51\x52\x53\x54\x55\x56\x57\x58"
    exp_hist_exemplar.trace_id = b"\x58\x57\x56\x55\x54\x53\x52\x51\x58\x57\x56\x55\x54\x53\x52\x51"
    exp_hist_exemplar.filtered_attributes.append(
        KeyValue.new_string_value("exp_hist_exemplar_key", "exp_hist_exemplar_value"))
    exp_hist_dp.exemplars.append(exp_hist_exemplar)

    # Add new exp histogram data point
    new_exp_hist_dp = ExponentialHistogramDataPoint()
    new_exp_hist_dp.start_time_unix_nano = 8000
    new_exp_hist_dp.time_unix_nano = 9000
    new_exp_hist_dp.count = 125
    new_exp_hist_dp.sum = 1250.0
    new_exp_hist_dp.scale = 3
    new_exp_hist_dp.zero_count = 7
    new_exp_hist_dp.zero_threshold = 0.03
    new_exp_hist_dp.flags = 0
    new_exp_hist_dp.min = 0.7
    new_exp_hist_dp.max = 70.0
    new_exp_hist_dp.attributes.append(KeyValue.new_string_value("new_exp_hist_dp_key", "new_exp_hist_dp_value"))

    # Create new buckets for new data point
    new_positive_buckets = ExponentialHistogramBuckets()
    new_positive_buckets.offset = 4
    new_positive_buckets.bucket_counts = [5, 10, 15, 20, 25]
    new_exp_hist_dp.positive = new_positive_buckets

    new_negative_buckets = ExponentialHistogramBuckets()
    new_negative_buckets.offset = -3
    new_negative_buckets.bucket_counts = [3, 6, 9, 12]
    new_exp_hist_dp.negative = new_negative_buckets

    exp_hist_data.data_points.append(new_exp_hist_dp)

    # --- Mutate Summary metric ---
    summary_metric.name = "python_modified_summary_metric"
    summary_metric.description = "python_modified_summary_description"
    summary_metric.unit = "python_modified_summary_unit"
    summary_metric.metadata.append(KeyValue.new_string_value("python_added_summary_metadata", "summary_metadata_value"))

    # Mutate existing summary data point
    summary_dp.start_time_unix_nano = 9900
    summary_dp.time_unix_nano = 11000
    summary_dp.count = 50
    summary_dp.sum = 500.0
    summary_dp.flags = 0
    summary_dp.attributes.append(KeyValue.new_string_value("python_added_summary_dp_attr", "python_summary_value"))

    # Mutate quantile values
    summary_dp.quantile_values[0].value = 20.0  # Changed from 10.0
    summary_dp.quantile_values[1].value = 38.0  # Changed from 19.0
    summary_dp.quantile_values[2].value = 39.6  # Changed from 19.8

    # Add new quantile
    new_quantile = ValueAtQuantile()
    new_quantile.quantile = 0.999
    new_quantile.value = 39.96
    summary_dp.quantile_values.append(new_quantile)

    # Add new summary data point
    new_summary_dp = SummaryDataPoint()
    new_summary_dp.start_time_unix_nano = 10000
    new_summary_dp.time_unix_nano = 11000
    new_summary_dp.count = 75
    new_summary_dp.sum = 750.0
    new_summary_dp.flags = 1
    new_summary_dp.attributes.append(KeyValue.new_string_value("new_summary_dp_key", "new_summary_dp_value"))

    # Add quantiles for new data point
    q50 = ValueAtQuantile()
    q50.quantile = 0.5
    q50.value = 30.0

    q95 = ValueAtQuantile()
    q95.quantile = 0.95
    q95.value = 57.0

    new_summary_dp.quantile_values = [q50, q95]
    summary_data.data_points.append(new_summary_dp)

    print("✓ All metrics mutated successfully!")

    print("\n=== STEP 3: VERIFY ALL MUTATIONS ===")

    # --- Verify ResourceMetrics mutations ---
    assert resource_metrics.schema_url == "python_modified_schema_url_resource"
    assert resource_metrics.resource.dropped_attributes_count == 25
    assert len(resource_metrics.resource.attributes) == 2
    assert resource_metrics.resource.attributes[1].key == "python_added_resource_key"

    # --- Verify ScopeMetrics mutations ---
    assert scope_metrics.schema_url == "python_modified_schema_url_scope"
    assert scope_metrics.scope.name == "python_modified_scope_name"
    assert scope_metrics.scope.version == "python_modified_scope_version"
    assert scope_metrics.scope.dropped_attributes_count == 15
    assert len(scope_metrics.scope.attributes) == 2
    assert scope_metrics.scope.attributes[1].key == "python_added_scope_key"

    # --- Verify Gauge mutations ---
    assert gauge_metric.name == "python_modified_gauge_metric"
    assert len(gauge_metric.metadata) == 2
    assert len(gauge_data.data_points) == 2
    assert gauge_data.data_points[0].start_time_unix_nano == 1100
    assert gauge_data.data_points[0].time_unix_nano == 2200
    assert gauge_data.data_points[0].flags == 1
    assert gauge_data.data_points[0].value[0] == 200
    assert len(gauge_data.data_points[0].attributes) == 2
    assert len(gauge_data.data_points[0].exemplars) == 2

    # --- Verify Sum mutations ---
    assert sum_metric.name == "python_modified_sum_metric"
    assert int(sum_data.aggregation_temporality) == int(AggregationTemporality.Cumulative)
    assert sum_data.is_monotonic is False
    assert len(sum_data.data_points) == 2
    assert sum_data.data_points[0].start_time_unix_nano == 3300
    assert sum_data.data_points[0].time_unix_nano == 4400
    assert len(sum_data.data_points[0].exemplars) == 1

    # --- Verify Histogram mutations ---
    assert hist_metric.name == "python_modified_histogram_metric"
    assert int(hist_data.aggregation_temporality) == int(AggregationTemporality.Delta)
    assert len(hist_data.data_points) == 2
    assert hist_data.data_points[0].count == 75
    assert hist_data.data_points[0].sum == 750.0
    assert hist_data.data_points[0].bucket_counts == [10, 20, 30, 40]
    assert hist_data.data_points[0].explicit_bounds == [2.0, 10.0, 20.0]
    assert len(hist_data.data_points[0].exemplars) == 2

    # --- Verify ExponentialHistogram mutations ---
    assert exp_hist_metric.name == "python_modified_exp_histogram_metric"
    assert int(exp_hist_data.aggregation_temporality) == int(AggregationTemporality.Cumulative)
    assert len(exp_hist_data.data_points) == 2
    assert exp_hist_data.data_points[0].count == 100
    assert exp_hist_data.data_points[0].sum == 1000.0
    assert exp_hist_data.data_points[0].scale == 2
    assert exp_hist_data.data_points[0].zero_count == 5
    assert exp_hist_data.data_points[0].positive.offset == 3
    assert exp_hist_data.data_points[0].positive.bucket_counts == [4, 8, 12, 16]
    assert exp_hist_data.data_points[0].negative.offset == -2
    assert exp_hist_data.data_points[0].negative.bucket_counts == [2, 4, 6]
    assert len(exp_hist_data.data_points[0].exemplars) == 2

    # --- Verify Summary mutations ---
    assert summary_metric.name == "python_modified_summary_metric"
    assert len(summary_data.data_points) == 2
    assert summary_data.data_points[0].count == 50
    assert summary_data.data_points[0].sum == 500.0
    assert summary_data.data_points[0].flags == 0
    assert len(summary_data.data_points[0].quantile_values) == 4
    assert summary_data.data_points[0].quantile_values[0].value == 20.0
    assert summary_data.data_points[0].quantile_values[1].value == 38.0
    assert summary_data.data_points[0].quantile_values[2].value == 39.6
    assert summary_data.data_points[0].quantile_values[3].quantile == 0.999

    print("✓ All mutations verified successfully!")
    print("\n=== PYTHON PROCESSING COMPLETE ===")
    print("Rust will now verify all mutations...")
