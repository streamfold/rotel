from rotel_python_processor_sdk import PyStatus, PyStatusCode, PyKeyValue


def process(resource_spans):
    span = resource_spans.scope_spans[0].spans[0]
    span.trace_id = b"5555555555"
    span.span_id = b"6666666666"
    span.trace_state = "test=1234567890"
    span.parent_span_id = b"7777777777"
    span.flags = 1
    span.name = "py_processed_span"
    span.kind = 4  # producer
    span.start_time_unix_nano = 1234567890
    span.end_time_unix_nano = 1234567890
    # TODO add attributes
    # span.attributes
    span.dropped_attributes_count = 100
    # TODO add events
    # span.events
    span.dropped_events_count = 200
    # TODO add links
    # span.links
    span.dropped_links_count = 300
    status = PyStatus()
    status.code = PyStatusCode.Error
    status.message = "error message"
    span.status = status

    event = span.events[0]
    event.time_unix_nano = 1234567890
    event.name = "py_processed_event"
    event.attributes.append(PyKeyValue.new_string_value("event_attr_key", "event_attr_value"))
    event.dropped_attributes_count = 400
