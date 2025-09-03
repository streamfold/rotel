from rotel_sdk.open_telemetry.common.v1 import EntityRef, EntityRefs


def process(resource):
    er = resource.entity_refs[0]
    
    assert er.schema_url == "http://example.com/schema/v1.0"
    assert er.type_ == "host"
    assert er.id_keys[0] == "host.id"
    assert er.description_keys[0] == "host.arch"
    assert er.description_keys[1] == "host.name"

    new_er = EntityRef()
    new_er.schema_url = "http://example.com/schema/v2.0"
    new_er.type_ = "container"
    new_er.id_keys = ["container.id"]
    new_er.description_keys = ["container.image.id", "container.image.name"]

    new_ers = EntityRefs()
    new_ers.append(new_er)

    resource.entity_refs = new_ers

    # Check len support
    assert 1 == len(resource.entity_refs)
