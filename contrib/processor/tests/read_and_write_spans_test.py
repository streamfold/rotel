from rotel_sdk.open_telemetry.common.v1 import KeyValue
from rotel_sdk.open_telemetry.trace.v1 import Status, StatusCode, Link


def process(resource_spans):
    # assert expected initial state
    span = resource_spans.scope_spans[0].spans[0]
    assert resource_spans.resource.dropped_attributes_count == 0
    assert span.trace_id == b'\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01'
    assert span.span_id == b'\x02\x02\x02\x02\x02\x02\x02\x02'
    assert span.trace_state == "rojo=00f067aa0ba902b7"
    assert span.parent_span_id == b'\x01\x01\x01\x01\x01\x01\x01\x01'
    assert span.flags == 0
    assert span.name == "foo"
    assert span.kind == 1
    assert span.dropped_attributes_count == 0
    assert len(span.attributes) == 2
    for attr in span.attributes:
        if attr.key == "http.method":
            assert attr.value.value == "POST"
        elif attr.key == "http.request.path":
            assert attr.value.value == "/items"
    for event in span.events:
        assert event.name == "a test event"
        assert len(event.attributes) == 0
        assert event.dropped_attributes_count == 0
    assert span.dropped_events_count == 0
    assert len(span.links) == 1
    link = span.links[0]
    assert link.trace_id == b'\x03\x03\x03\x03\x03\x03\x03\x03'
    assert link.span_id == b'\x04\x04\x04\x04\x04\x04\x04\x04'
    assert link.trace_state == "mojo=00f067aa0ba902b7"
    assert len(link.attributes) == 2
    for attr in link.attributes:
        if attr.key == "http.method":
            assert attr.value.value == "GET"
        elif attr.key == "http.request.path":
            assert attr.value.value == "/item/0"
    assert link.dropped_attributes_count == 10
    assert link.flags == 0

    # mutate resource
    resource_spans.resource.dropped_attributes_count = 15
    # mutate span
    span.trace_id = b"5555555555"
    span.span_id = b"6666666666"
    span.trace_state = "test=1234567890"
    span.parent_span_id = b"7777777777"
    span.flags = 1
    span.name = "py_processed_span"
    span.kind = 4  # producer
    span.start_time_unix_nano = 1234567890
    span.end_time_unix_nano = 1234567890

    span.attributes.append(KeyValue.new_string_value("span_attr_key", "span_attr_value"))
    span.dropped_attributes_count = 100
    # TODO add events
    # span.events
    span.dropped_links_count = 300
    span.dropped_events_count = 200
    # Append a new link
    new_link = Link()
    new_link.trace_id = b"88888888"
    new_link.span_id = b"99999999"
    new_link.trace_state = "test=1234567890"
    new_link.attributes.append(KeyValue.new_string_value("link_attr_key", "link_attr_value"))
    new_link.dropped_attributes_count = 300
    new_link.flags = 1
    span.links.append(new_link)

    status = Status()
    status.code = StatusCode.Error
    status.message = "error message"
    span.status = status

    event = span.events[0]
    event.time_unix_nano = 1234567890
    event.name = "py_processed_event"
    event.attributes.append(KeyValue.new_string_value("event_attr_key", "event_attr_value"))
    event.dropped_attributes_count = 400
