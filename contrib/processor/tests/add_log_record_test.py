from rotel_sdk.open_telemetry.common.v1 import KeyValue, AnyValue
from rotel_sdk.open_telemetry.logs.v1 import LogRecord


def process(resource_logs):
    # Ensure there's at least one scope_logs entry to append to
    if not resource_logs.scope_logs:
        print("Error: No ScopeLogs found to append to.")
        return

    # Get the first ScopeLogs object
    scope_logs = resource_logs.scope_logs[0]

    # Create a new LogRecord
    new_log = LogRecord()
    new_log.time_unix_nano = 9876543210
    new_log.observed_time_unix_nano = 9876543211
    new_log.severity_number = 17  # FATAL
    new_log.severity_text = "FATAL"

    new_body = AnyValue()
    new_body.string_value = "This is a newly added log message."
    new_log.body = new_body

    new_log.attributes.append(KeyValue.new_string_value("new_log_key", "new_log_value"))
    new_log.dropped_attributes_count = 2
    new_log.flags = 4
    new_log.trace_id = b"fedcba9876543210"
    new_log.span_id = b"fedcba98"

    # Append the new LogRecord to the list
    scope_logs.log_records.append(new_log)
