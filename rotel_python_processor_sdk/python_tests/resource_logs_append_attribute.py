import platform
from datetime import datetime
from rotel_sdk.open_telemetry.common.v1 import KeyValue


def process(resource_logs):
    os_name = platform.system()
    os_version = platform.release()
    # Add individual attributes
    resource_logs.resource.attributes.append(KeyValue.new_string_value("os.name", os_name))
    resource_logs.resource.attributes.append(KeyValue.new_string_value("os.version", os_version))
    resource_logs.resource.attributes.append_attributes(get_attrs_list())


def get_attrs_list():
    new_attrs = []
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_attrs.append(KeyValue.new_string_value("observed_time", current_time))
    new_attrs.append(KeyValue.new_string_value("service.name", "rotel"))
    new_attrs.append(KeyValue.new_bool_value("observed", True))

    return new_attrs
