from typing import Any


class KeyValue:
    key: str
    value: AnyValue


class AnyValue:
    def value(self) -> Any: ...


class InstrumentationScope:
    name: str
    version: str
    attributes: List[KeyValue]
    dropped_attributes_count: int
