from rotel_sdk.open_telemetry.common.v1 import KeyValue


class Resource:
    attributes: list[KeyValue]
    dropped_attributes_count: int
