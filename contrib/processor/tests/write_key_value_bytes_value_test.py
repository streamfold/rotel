def process(key_value):
    assert key_value.value.value == "foo"
    key_value.value.bytes_value = b"111111"
