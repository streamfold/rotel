from rotel_sdk.open_telemetry.common.v1 import KeyValue
from rotel_sdk.open_telemetry.trace.v1 import Event


def process(resource_spans):
    # assert expected initial state
    span = resource_spans.scope_spans[0].spans[0]
    for event in span.events:
        assert event.name == "a test event"
        assert len(event.attributes) == 0
        assert event.dropped_attributes_count == 0

    # Create a new Events type and append events to it.
    # Then set on the spans event list.

    events = []
    first_event = Event()
    first_event.name = "first_event"
    first_event.time_unix_nano = 123
    first_event.attributes.append(
        KeyValue.new_string_value("first_event_attr_key", "first_event_attr_value")
    )
    first_event.dropped_attributes_count = 1
    events.append(first_event)

    second_event = Event()
    second_event.name = "second_event"
    second_event.time_unix_nano = 456
    second_event.attributes.append(
        KeyValue.new_string_value("second_event_attr_key", "second_event_attr_value")
    )
    second_event.dropped_attributes_count = 2
    events.append(second_event)

    span.events = events
