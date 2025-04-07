import platform
from rotel_python_processor_sdk import PyKeyValue, PyAttributes


def process(resource):
    new_attributes = PyAttributes()
    os_name = platform.system()
    os_version = platform.release()
    # Add individual attributes
    new_attributes.append(PyKeyValue.new_string_value("os.name", os_name))
    new_attributes.append(PyKeyValue.new_string_value("os.version", os_version))
    resource.attributes = new_attributes
