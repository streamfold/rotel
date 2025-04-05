from rotel_python_processor_sdk import PyKeyValue

def process(key_value):
    print(f"key_value.key: {key_value.key}")
    key_value.key = "new_key"