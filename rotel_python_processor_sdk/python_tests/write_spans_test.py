from rotel_sdk.open_telemetry.common.v1 import KeyValue
from rotel_sdk.open_telemetry.trace.v1 import Span


def process(resource_spans):
    spans = []
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
    span.attributes.append(KeyValue.new_string_value("span_attr_key", "span_attr_value"))
    spans.append(span)

    resource_spans.scope_spans[0].spans = spans
