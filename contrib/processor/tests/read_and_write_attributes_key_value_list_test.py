from rotel.open_telemetry.common.v1 import KeyValueList, KeyValue, Attributes


def process(resource):
    # Access the KeyValueList first value by subscript
    kv = resource.attributes[0].value.value[0]
    assert kv.value.value == "foo"

    # Change the value of
    kv.value.string_value = "bar"
    # Now verify it's updated by fetching again
    kv = resource.attributes[0].value.value[0]
    assert kv.value.value == "bar"

    # Iterate the KeyValueList and verify the value is updated
    kvl = resource.attributes[0].value.value
    for kv in kvl:
        assert kv.value.value == "bar"

    # Create a new KeyValueList and update resource.attributes
    new_key_value_list = KeyValueList()
    new_kv = KeyValue.new_string_value("new_key", "baz")
    new_key_value_list.append(new_kv)

    kvl = KeyValue.new_kv_list("my_kv_list", new_key_value_list)
    new_attributes = Attributes()
    new_attributes.append(kvl)
    resource.attributes = new_attributes

    assert resource.attributes[0].key == "my_kv_list"
    av = resource.attributes[0].value.value[0]
    assert av.value.value == "baz"

    # Check len support
    assert 1 == len(resource.attributes[0].value.value)
