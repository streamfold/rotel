from rotel_python_processor_sdk import PyAnyValue

def process(any_value):
    print(f"any_value.value: {any_value.value}")
    any_value.string_value = "changed"
    print(f"any_value.value: {any_value.value}")
