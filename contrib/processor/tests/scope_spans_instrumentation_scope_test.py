def process(resource_spans):
    scope_spans = resource_spans.scope_spans[0]
    print(f"scope_span: {scope_spans}")
    print(f"scope_span.scope: {scope_spans.scope}")
    scope = scope_spans.scope
    print(f"scope.name: {scope.name}")
    print(f"scope.version: {scope.version}")
    print(f"scope.dropped_attributes_count: {scope.dropped_attributes_count}")
    scope.name = "foo"
    print(f"scope.name: {scope.name}")
    print(f"scope.name: {scope_spans.scope.name}")

    scope_spans = resource_spans.scope_spans[0]
    print(f"scope.name: {scope_spans.scope.name}")

    for scope_span in resource_spans.scope_spans:
        scope_span.scope.name = "bar"

    for scope_span in resource_spans.scope_spans:
        print(f"scope_span.scope.name: {scope_span.scope.name}")

    # Access the KeyValueList first value by subscript
# kv = resource.attributes[0].value.value[0]
# assert kv.value.value == "foo"
#
# # Change the value of
# kv.value.string_value = "bar"
# # Now verify it's updated by fetching again
# kv = resource.attributes[0].value.value[0]
# assert kv.value.value == "bar"
#
# # Iterate the KeyValueList and verify the value is updated
# kvl = resource.attributes[0].value.value
# for kv in kvl:
#     assert kv.value.value == "bar"
#
# # Create a new KeyValueList and update resource.attributes
# new_key_value_list = PyKeyValueList()
# new_kv = PyKeyValue.new_string_value("new_key", "baz")
# new_key_value_list.append(new_kv)
#
# kvl = PyKeyValue.new_kv_list("my_kv_list", new_key_value_list)
# new_attributes = PyAttributes()
# new_attributes.append(kvl)
# resource.attributes = new_attributes
#
# assert resource.attributes[0].key == "my_kv_list"
# av = resource.attributes[0].value.value[0]
# assert av.value.value == "baz"
#
# # Check len support
# assert 1 == len(resource.attributes[0].value.value)
