import itertools
import sys

from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs
from rotel_sdk.open_telemetry.metrics.v1 import ResourceMetrics, MetricData, Gauge, Sum, Histogram, \
    ExponentialHistogram, Summary
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans

sys.path.insert(0, './processors')

from attributes_processor import Config, ActionKeyValue, Action, AttributeProcessor

processor_config = Config(
    actions=[
        # INSERT: Add 'host.name' if not exists
        ActionKeyValue(key="host.name", action=Action.INSERT, value="my-server-1"),
        # UPDATE: Change 'http.status_code' to string type
        ActionKeyValue(key="http.status_code", action=Action.UPDATE, value="OK"),
        # UPSERT: Add or update 'env' attribute
        ActionKeyValue(key="env", action=Action.UPSERT, value="production"),
        # UPSERT with from_attribute: Copy 'user.email' to 'email'
        ActionKeyValue(key="email", action=Action.UPSERT, from_attribute="user.email"),
        # HASH: Hash 'user.id'
        ActionKeyValue(key="user.id", action=Action.HASH),
        # HASH with regex: Hash any attribute starting with 'trace'
        ActionKeyValue(key="", action=Action.HASH, regex_pattern=r"^trace.*"),
        # EXTRACT: Extract 'id' and 'name' from 'raw_data'
        ActionKeyValue(key="raw_data", action=Action.EXTRACT,
                       regex_pattern=r"id:(?P<extracted_id>\d+),name:(?P<extracted_name>\w+)"),
        # CONVERT: Convert 'temp_str_int' to int
        ActionKeyValue(key="temp_str_int", action=Action.CONVERT, converted_type="int"),
        # CONVERT: Convert 'temp_str_bool' to bool
        ActionKeyValue(key="temp_str_bool", action=Action.CONVERT, converted_type="bool"),
        # CONVERT: Convert 'temp_int_float' to float
        ActionKeyValue(key="temp_int_float", action=Action.CONVERT, converted_type="double"),
        # DELETE: Remove 'path' attribute
        ActionKeyValue(key="path", action=Action.DELETE),
        # DELETE with regex: Remove any attribute ending with '.secret'
        ActionKeyValue(key="", action=Action.DELETE, regex_pattern=r".*\.secret$"),
    ]
)

processor = AttributeProcessor(processor_config)


def process_logs(resource_logs: ResourceLogs):
    for log_record in itertools.chain.from_iterable(
            scope_log.log_records for scope_log in resource_logs.scope_logs
    ):
        attrs = processor.process_attributes(log_record.attributes)
        log_record.attributes = attrs


def process_spans(resource_spans: ResourceSpans):
    for span in itertools.chain.from_iterable(
            scope_span.spans for scope_span in resource_spans.scope_spans
    ):
        attrs = processor.process_attributes(span.attributes)
        span.attributes = attrs


def process_metrics(resource_metrics: ResourceMetrics):
    for metric in itertools.chain.from_iterable(
            scope_metric.metrics for scope_metric in resource_metrics.scope_metrics
    ):
        if metric.data is not None:
            if isinstance(metric.data, MetricData.Gauge):
                gauge: Gauge = metric.data[0]
                for dp in gauge.data_points:
                    dp.attributes = processor.process_attributes(dp.attributes)
            if isinstance(metric.data, MetricData.Sum):
                sum: Sum = metric.data[0]
                for dp in sum.data_points:
                    dp.attributes = processor.process_attributes(dp.attributes)
            if isinstance(metric.data, MetricData.Histogram):
                histo: Histogram = metric.data[0]
                for dp in histo.data_points:
                    dp.attributes = processor.process_attributes(dp.attributes)
            if isinstance(metric.data, MetricData.ExponentialHistogram):
                exp_histo: ExponentialHistogram = metric.data[0]
                for dp in exp_histo.data_points:
                    dp.attributes = processor.process_attributes(dp.attributes)
            if isinstance(metric.data, Summary):
                summary: Summary = metric.data[0]
                for dp in summary.data_points:
                    dp.attributes = processor.process_attributes(dp.attributes)
