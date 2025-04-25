import platform
from datetime import datetime
from rotel_sdk.open_telemetry.common.v1 import KeyValue


def process(resource):
    print("Hello from python attributes before mutating")
    for kv in resource.attributes:
        print(f"{kv.key}")
        print(f"{kv.value.value}")

    # mutate while iterating
    for kv in resource.attributes:
        kv.key = "new key"
        kv.value.string_value = "new value"

    print("Hello from python, again here's the attributes after mutating")
    for kv in resource.attributes:
        print(f"{kv.key}")
        print(f"{kv.value.value}")

    os_name = platform.system()
    os_version = platform.release()
    # Add individual attributes
    resource.attributes.append(KeyValue.new_string_value("os.name", os_name))
    resource.attributes.append(KeyValue.new_string_value("os.version", os_version))
    resource.attributes.append_attributes(get_attrs_list())

    print("Goodbye from Python, here's the final attributes")
    for kv in resource.attributes:
        print(f"{kv.key}")
        print(f"{kv.value.value}")


def get_attrs_list():
    new_attrs = []
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_attrs.append(KeyValue.new_string_value("observed_time", current_time))
    new_attrs.append(KeyValue.new_string_value("service.name", "rotel"))
    new_attrs.append(KeyValue.new_bool_value("observed", True))

    return new_attrs
