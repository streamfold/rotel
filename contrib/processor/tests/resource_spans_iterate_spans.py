def process(resource_spans):
    print("Hello from python attributes before mutating")
    for scope_spans in resource_spans.scope_spans:
        print(f"{scope_spans}")
        for span in scope_spans.spans:
            print(f"trace_id:  {span.trace_id}")
            span.trace_id = b"5555555555"
            print(f"trace_id:  {span.trace_id}")

    for scope_spans in resource_spans.scope_spans:
        for span in scope_spans.spans:
            print(f"trace_id:  {span.trace_id}")
            print(f"span_id:  {span.span_id}")
            print(f"name_id:  {span.name}")
