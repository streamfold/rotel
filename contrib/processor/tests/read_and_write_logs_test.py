from rotel_sdk.open_telemetry.common.v1 import KeyValue, AnyValue


def process(resource_logs):
    # Assert expected initial state of the log record
    log_record = resource_logs.scope_logs[0].log_records[0]

    assert log_record.time_unix_nano == 1000000000  # Example initial value
    assert log_record.observed_time_unix_nano == 1000000001  # Example initial value
    assert log_record.severity_number == 9  # INFO severity number
    assert log_record.severity_text == "INFO"
    assert log_record.body.value == "initial log message"  # Example initial value
    assert len(log_record.attributes) == 2
    for attr in log_record.attributes:
        if attr.key == "log.source":
            assert attr.value.value == "my_app"
        elif attr.key == "component":
            assert attr.value.value == "backend"
    assert log_record.dropped_attributes_count == 0
    assert log_record.flags == 0
    assert log_record.trace_id == b'\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10'
    assert log_record.span_id == b'\x11\x12\x13\x14\x15\x16\x17\x18'

    # Mutate log record fields
    log_record.time_unix_nano = 2000000000
    log_record.observed_time_unix_nano = 2000000001
    log_record.severity_number = 13  # ERROR severity number
    log_record.severity_text = "ERROR"

    new_body = AnyValue()
    new_body.string_value = "processed log message"
    log_record.body = new_body

    log_record.dropped_attributes_count = 5
    log_record.flags = 1

    log_record.trace_id = b"abcdefghijklmnop"
    log_record.span_id = b"qrstuvwx"

    # Add a new attribute
    log_record.attributes.append(KeyValue.new_string_value("new_log_attr", "new_log_value"))
    assert len(log_record.attributes) == 3

    # Modify an existing attribute
    # Assuming "component" is at index 1 based on initial state assertion
    av = AnyValue()
    av.string_value = "modified_component_value"
    log_record.attributes[1].value = av
    assert log_record.attributes[1].key == "component"
    assert log_record.attributes[1].value.value == "modified_component_value"
