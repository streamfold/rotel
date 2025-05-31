from rotel_sdk.open_telemetry.common.v1 import KeyValueList, ArrayValue
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans
from typing import cast


def process(resource_spans: ResourceSpans):
    if resource_spans.resource is not None:
        resource = resource_spans.resource
        del resource.attributes[2]

        # Remove from the KeyValueList which is the first element
        if resource.attributes[0].value is not None:
            kvlist = resource.attributes[0].value.value
            if kvlist is not None:
                kvlist = cast(KeyValueList, kvlist)
                del kvlist[0]
            
        # Remove from the ArrayList which is the second element
        if resource.attributes[1].value is not None:
            arlist = resource.attributes[1].value.value
            if arlist is not None:
                arlist = cast(ArrayValue, arlist)
                del arlist[1]
