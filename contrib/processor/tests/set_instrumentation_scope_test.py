from rotel.open_telemetry.common.v1 import InstrumentationScope, KeyValue


def process(resource_spans):
    scope_spans = resource_spans.scope_spans[0]
    scope = scope_spans.scope
    # assert "scope" == scope.name
    # assert "0.0.1" == scope.version
    # assert 0 == scope.dropped_attributes_count

    # Now create a new InstrumentationScope
    scope = InstrumentationScope()
    scope.name = "name_changed"
    scope.version = "0.0.2"
    scope.dropped_attributes_count = 100

    scope_spans.scope = scope
    # Retrieve the scope span and scope again and verify updates

    scope_spans = resource_spans.scope_spans[0]
    assert "name_changed" == scope_spans.scope.name
    assert "0.0.2" == scope_spans.scope.version
    assert 100 == scope_spans.scope.dropped_attributes_count

    # Append a new attribute
    kv = KeyValue.new_string_value("severity", "WARN")
    resource_spans.scope_spans[0].scope.attributes.append(kv)

    assert len(resource_spans.scope_spans[0].scope.attributes) == 1
