import sys

from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs
from rotel_sdk.open_telemetry.metrics.v1 import ResourceMetrics
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans

sys.path.insert(0, './processors')

from context_processor import ContextProcessor

processor = ContextProcessor()


def process_logs(resource_logs: ResourceLogs):
    processor.process_logs(resource_logs)


def process_spans(resource_spans: ResourceSpans):
    processor.process_spans(resource_spans)


def process_metrics(resource_metrics: ResourceMetrics):
    processor.process_metrics(resource_metrics)
