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

    # Now let's set and item directly on an index of the ArrayValue with __setitem__
    new_any_value = AnyValue()
    new_any_value.string_value = "qux"
    resource.attributes[0].value.value.__setitem__(0, new_any_value)
    assert resource.attributes[0].value.value[0].value == "qux"
    # Set it again via index
    new_any_value.int_value = 123456789
    resource.attributes[0].value.value[0] = new_any_value
    assert resource.attributes[0].value.value[0].value == 123456789
