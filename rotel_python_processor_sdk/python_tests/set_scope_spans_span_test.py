from rotel_sdk.open_telemetry.common.v1 import KeyValue
from rotel_sdk.open_telemetry.trace.v1 import Span, Status, StatusCode, Event, Link


def process(resource_spans):
    span = Span()
    span.trace_id = b"5555555555"
    span.span_id = b"6666666666"
    span.trace_state = "test=1234567890"
    span.parent_span_id = b"7777777777"
    span.flags = 1
    span.name = "py_processed_span"
    span.kind = 4  # producer
    span.start_time_unix_nano = 1234567890
    span.end_time_unix_nano = 1234567890
    span.dropped_attributes_count = 100
    span.dropped_events_count = 200
    span.dropped_links_count = 300
    span.attributes.append(KeyValue.new_string_value("span_attr_key", "span_attr_value"))

    status = Status()
    status.code = StatusCode.Error
    status.message = "error message"
    span.status = status

    event = Event()
    event.time_unix_nano = 1234567890
    event.name = "py_processed_event"
    event.attributes.append(KeyValue.new_string_value("event_attr_key", "event_attr_value"))
    event.dropped_attributes_count = 400
    # Push a dummy event and test setting our new event by index.
    # We need to append here as current len of events is 0
    span.events.append(Event())
    span.events[0] = event

    new_link = Link()
    new_link.trace_id = b"88888888"
    new_link.span_id = b"99999999"
    new_link.trace_state = "test=1234567890"
    new_link.attributes.append(KeyValue.new_string_value("link_attr_key", "link_attr_value"))
    new_link.dropped_attributes_count = 300
    new_link.flags = 1
    # Same thing here, push a dummy and then test setting our new link by index.
    span.links.append(Link())
    span.links[0] = new_link

    resource_spans.scope_spans[0].spans[0] = span
