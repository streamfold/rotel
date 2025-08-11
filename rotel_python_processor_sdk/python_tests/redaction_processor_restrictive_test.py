import sys

from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs
from rotel_sdk.open_telemetry.metrics.v1 import ResourceMetrics
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans

sys.path.insert(0, './processors')

from redaction_processor import RedactionProcessorConfig, RedactionProcessor

config = RedactionProcessorConfig(
    allow_all_keys=False,  # Deny by default, only allowed_keys pass
    # Explicitly allowed for resources, spans, metrics, logs
    allowed_keys=["description", "group", "id", "name", "user_id", "event_type", "source",
                  "ip_address", "status", "path", "endpoint", "region", "operation", "service.name",
                  "host.arch", "os.type", "env", "deployment.environment"],
    # These keys will always be kept, even if in blocked lists or not in allowed_keys
    ignored_keys=["safe_attribute"],
    hash_function="md5",  # Example: "sha256" or None
    summary="debug"  # "info", "silent"
)

processor = RedactionProcessor(config)


def process_logs(resource_logs: ResourceLogs):
    processor.process_logs(resource_logs)


def process_spans(resource_spans: ResourceSpans):
    processor.process_spans(resource_spans)


def process_metrics(resource_metrics: ResourceMetrics):
    processor.process_metrics(resource_metrics)
