from rotel_python_processor_sdk import PyAnyValue

def process(resource):
    attributes = resource.attributes
    attributes[0].key = "new_key"

    for kv in resource.attributes:
        print(f"{kv.key}")
        print(f"{kv.value.value}")
