from rotel_sdk.open_telemetry.common.v1 import KeyValue, InstrumentationScope
from rotel_sdk.open_telemetry.trace.v1 import ScopeSpans, Span


def process(resource_spans):
    # assert expected initial state
    scope_spans_list = []
    scope_spans = ScopeSpans()
    scope_spans.schema_url = "https://github.com/streamfold/rotel"
    inst_scope = InstrumentationScope()
    inst_scope.name = "rotel-sdk"
    inst_scope.version = "v1.0.0"
    inst_scope.attributes.append(KeyValue.new_string_value("rotel-sdk", "v1.0.0"))
    scope_spans.scope = inst_scope

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
    scope_spans.spans.append(span)

    scope_spans_list.append(scope_spans)
    resource_spans.scope_spans = scope_spans_list
