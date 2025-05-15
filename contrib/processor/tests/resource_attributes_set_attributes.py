import platform

from rotel_sdk.open_telemetry.common.v1 import KeyValue, Attributes


def process(resource):
    new_attributes = Attributes()
    os_name = platform.system()
    os_version = platform.release()
    # Add individual attributes
    new_attributes.append(KeyValue.new_string_value("os.name", os_name))
    new_attributes.append(KeyValue.new_string_value("os.version", os_version))
    resource.attributes = new_attributes
    # Get the attributes and check their values
    assert len(resource.attributes) == 2
    first_attr = resource.attributes[0]
    assert first_attr.key == "os.name"
    assert first_attr.value.value != ""
    second_attr = resource.attributes[1]
    assert second_attr.key == "os.version"
    assert second_attr.value.value != ""
    # Now perform a set operation on the attributes
    kv = KeyValue.new_double_value("double.value", 3.14)
    resource.attributes[0] = kv
    get_kv = resource.attributes[0]
    assert get_kv.key == "double.value"
    assert get_kv.value.value == 3.14
