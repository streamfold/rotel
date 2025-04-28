from rotel_sdk.open_telemetry.common.v1 import KeyValue
from rotel_sdk.open_telemetry.resource.v1 import Resource


def process(resource_spans):
    # Create a new resource with some attributes.
    resource = Resource()
    resource.attributes.append(KeyValue.new_string_value("key", "value"))
    resource.attributes.append(KeyValue.new_bool_value("boolean", True))
    resource.dropped_attributes_count = 35

    resource_spans.resource = resource

    # Verify our setter worked
    resource = resource_spans.resource
    assert 2 == len(resource.attributes)
    assert 35 == resource.dropped_attributes_count
    for attr in resource.attributes:
        if attr.key == "key":
            assert attr.value.value == "value"
        elif attr.key == "boolean":
            assert attr.value.value
