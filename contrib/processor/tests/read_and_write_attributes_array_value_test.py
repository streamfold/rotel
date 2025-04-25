from rotel_sdk.open_telemetry.common.v1 import AnyValue, ArrayValue, Attributes, KeyValue


def process(resource):
    # Access the AnyValue by subscript
    av = resource.attributes[0].value.value[0]
    assert av.value == "foo"

    # Change the value of
    av.string_value = "bar"
    # Now verify it's updated by fetching again
    av = resource.attributes[0].value.value[0]
    assert av.value == "bar"

    # Iterate the ArrayValue
    for av in resource.attributes[0].value.value:
        assert av.value == "bar"

    # Create a new ArrayValue and update resource.attributes
    new_array_value = ArrayValue()
    new_any_value = AnyValue()
    new_any_value.string_value = "baz"
    new_array_value.append(new_any_value)

    kv = KeyValue.new_array_value("my_array", new_array_value)
    new_attributes = Attributes()
    new_attributes.append(kv)
    resource.attributes = new_attributes

    assert resource.attributes[0].key == "my_array"
    av = resource.attributes[0].value.value[0]
    assert av.value == "baz"

    # Check len support
    assert 1 == len(resource.attributes[0].value.value)
