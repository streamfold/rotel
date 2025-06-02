def process(resource):
    attributes = resource.attributes
    print(f"resource.attributes: {attributes}")
    print(f"resource.attributes.len(): {len(attributes)}")

    print(f"resource.attributes[0].key: {attributes[0].key}")
    print(f"resource.attributes[0].value: {attributes[0].value.value}")

    for kv in resource.attributes:
        print(f"{kv.key}")
        print(f"{kv.value.value}")
