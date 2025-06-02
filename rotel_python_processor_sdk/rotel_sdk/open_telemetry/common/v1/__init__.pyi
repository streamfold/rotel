from typing import Optional, Any


class KeyValue:
    """
    KeyValue is a key-value pair that is used to store Span attributes, Link attributes, etc.
    """
    key: str
    """
    The key of the key-value pair.
    """
    value: Optional[AnyValue]
    """
    The Optional value of the key-value pair.
    """

    @staticmethod
    def new_string_value(key: str, value: str) -> KeyValue: ...

    """
    Creates a new KeyValue object with a string value. 
    """

    @staticmethod
    def new_bool_value(key: str, value: bool) -> KeyValue: ...

    """
    Creates a new KeyValue object with a boolean value. 
    """

    @staticmethod
    def new_int_value(key: str, value: int) -> KeyValue: ...

    """
    Creates a new KeyValue object with an integer value. 
    """

    @staticmethod
    def new_double_value(key: str, value: float) -> KeyValue: ...

    """
    Creates a new KeyValue object with a double value. 
    """

    @staticmethod
    def new_bytes_value(key: str, value: bytes) -> KeyValue: ...

    """
    Creates a new KeyValue object with a byte array value.
    """

    @staticmethod
    def new_array_value(key: str, value: ArrayValue) -> KeyValue: ...

    """
    Creates a new KeyValue object with an array value.
    """

    @staticmethod
    def new_kv_list(key: str, value: KeyValueList) -> KeyValue: ...

    """
    Creates a new KeyValue object with a KeyValueList value.
    """


class ArrayValue:
    """
    ArrayValue represents a list of values.
    """

    def append(self, value: AnyValue) -> None: ...

    """
    Appends a value to this array.
    """

    def __delitem__(self, index: int) -> None: ...

    """
    Removes an item from this list.
    """

    def __getitem__(self, index: int) -> AnyValue: ...

    """
    Retrieves an item from this list.
    """

    def __setitem__(self, index: int, value: AnyValue) -> None: ...

    """
    Sets an item in this list.
    """


class KeyValueList:
    """
    A list of key-value pairs.
    """

    def append(self, value: KeyValue) -> None: ...

    """
    Appends a KeyValue object to this list.
    """

    def __delitem__(self, index: int) -> None: ...

    """
    Removes an item from this list.
    """

    def __getitem__(self, index: int) -> KeyValue: ...

    """
    Retrieves an item from this list.
    """

    def __setitem__(self, index: int, value: KeyValue) -> None: ...

    """
    Sets an item in this list.
    """


class AnyValue:
    """
    AnyValue is used to represent any type of attribute value. AnyValue may contain a primitive value such as a string or integer or it may contain an arbitrary nested object containing arrays, key-value lists and primitives.
    """
    value: Optional[Any]


class InstrumentationScope:
    """
    InstrumentationScope is a message representing the instrumentation scope information such as the fully qualified name and version.
    """
    name: str
    """
    An empty instrumentation scope name means the name is unknown.
    """
    version: str
    attributes: List[KeyValue]
    """
    Additional attributes that describe the scope. [Optional]. Attribute keys MUST be unique (it is not allowed to have more than one attribute with the same key).
    """
    dropped_attributes_count: int
