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

    def value(self) -> Optional[Any]:
        """
        :returns An Optional Any object.
        """


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
