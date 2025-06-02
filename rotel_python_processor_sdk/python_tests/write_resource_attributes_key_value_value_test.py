def process(resource):
    attributes = resource.attributes
    attributes[0].value.string_value = "changed"

    for kv in resource.attributes:
        print(f"{kv.key}")
        print(f"{kv.value.value}")
