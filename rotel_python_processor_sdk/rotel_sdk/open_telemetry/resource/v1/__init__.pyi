from rotel_sdk.open_telemetry.common.v1 import KeyValue


class Resource:
    """
    Resource information.
    """
    attributes: list[KeyValue]
    """
    Set of attributes that describe the resource.
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    dropped_attributes_count: int
    """
    dropped_attributes_count is the number of dropped attributes. If the value is 0, then
    no attributes were dropped.
    """
