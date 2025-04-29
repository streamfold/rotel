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


class AnyValue:
    """
    AnyValue is used to represent any type of attribute value. AnyValue may contain a primitive value such as a string or integer or it may contain an arbitrary nested object containing arrays, key-value lists and primitives.
    """

    def value(self) -> Optional[Any]: ...

    """
    :return An Optional Any object.
    """


class InstrumentationScope:
    name: str
    version: str
    attributes: List[KeyValue]
    dropped_attributes_count: int
