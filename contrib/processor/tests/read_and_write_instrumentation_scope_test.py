from rotel_python_processor_sdk import PyKeyValue


def process(resource_spans):
    scope_spans = resource_spans.scope_spans[0]
    scope = scope_spans.scope
    assert "scope" == scope.name
    assert "0.0.1" == scope.version
    assert 0 == scope.dropped_attributes_count

    # Now update attributes on the scope
    scope.name = "name_changed"
    scope.version = "0.0.2"
    scope.dropped_attributes_count = 100

    # Retrieve the scope span and scope again and verify updates

    scope_spans = resource_spans.scope_spans[0]
    assert "name_changed" == scope_spans.scope.name
    assert "0.0.2" == scope_spans.scope.version
    assert 100 == scope_spans.scope.dropped_attributes_count

    for attr in scope_spans.scope.attributes:
        assert "module" == attr.key
        assert "api" == attr.value.value

    # Now mutate it on an iteration
    for attr in scope_spans.scope.attributes:
        attr.key = "key_changed"
        attr.value.int_value = 200

    for attr in resource_spans.scope_spans[0].scope.attributes:
        assert "key_changed" == attr.key
        assert 200 == attr.value.value

    # Append a new attribute
    kv = PyKeyValue.new_string_value("severity", "WARN")
    resource_spans.scope_spans[0].scope.attributes.append(kv)

    assert len(resource_spans.scope_spans[0].scope.attributes) == 2
