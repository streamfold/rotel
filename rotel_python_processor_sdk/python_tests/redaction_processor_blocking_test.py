import sys

from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans

sys.path.insert(0, './processors')

from redaction_processor import RedactionProcessorConfig, RedactionProcessor

config = RedactionProcessorConfig(
    allow_all_keys=True,  # Deny by default, only allowed_keys pass
    blocked_key_patterns=[".*token.*", ".*api_key.*", ".*password.*"],
    blocked_values=[
        "4[0-9]{12}(?:[0-9]{3})?",  # Visa credit card number
        "(5[1-5][0-9]{14})",  # MasterCard number
        ".*@.*\\.com",  # block and emails
        "https://example.com/sensitive/path",  # specific URL
        "SELECT.*FROM.*",  # SQL statement
        "123-45-6789",  # SSN for logs (value itself)
        "192\\.168\\.\\d+\\.\\d+",  # IP address pattern
        "10\\.0\\.\\d+\\.\\d+",  # Another IP pattern
        "password=abcde",  # Log body password
        "another@example.com"  # Email in log body
    ],
    allowed_values=[".+@mycompany.com"],  # This overrides blocked_values if matched
    hash_function="md5",  # Example: "sha256" or None
    summary="debug"  # "info", "silent"
)

processor = RedactionProcessor(config)


def process_logs(resource_logs: ResourceLogs):
    processor.process_logs(resource_logs)


def process_spans(resource_spans: ResourceSpans):
    processor.process_spans(resource_spans)
