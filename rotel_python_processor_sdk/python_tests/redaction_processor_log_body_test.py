import sys

from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans

sys.path.insert(0, './processors')

from redaction_processor import RedactionProcessorConfig, RedactionProcessor

config = RedactionProcessorConfig(
    allow_all_keys=True,  # Deny by default, only allowed_keys pass
    blocked_values=[
        ".*.password=.*.",  # Log body password
    ],
    hash_function="md5",  # Example: "sha256" or None
    summary="debug"  # "info", "silent"
)

processor = RedactionProcessor(config)


def process_logs(resource_logs: ResourceLogs):
    processor.process_logs(resource_logs)


def process_spans(resource_spans: ResourceSpans):
    processor.process_spans(resource_spans)
