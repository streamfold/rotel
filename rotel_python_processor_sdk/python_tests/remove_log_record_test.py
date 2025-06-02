def process(resource_logs):
    # Ensure there's at least one scope_logs entry to modify
    if not resource_logs.scope_logs:
        print("Error: No ScopeLogs found to modify.")
        return

    # Get the first ScopeLogs object
    scope_logs = resource_logs.scope_logs[0]

    # Assert initial state: two log records
    assert len(scope_logs.log_records) == 2
    assert scope_logs.log_records[0].body.value == "first log message"
    assert scope_logs.log_records[1].body.value == "second log message"

    # Remove the second log record
    # Python lists support 'del' or 'pop'
    del scope_logs.log_records[1]

    # Assert that only one log record remains
    assert len(scope_logs.log_records) == 1
    assert scope_logs.log_records[0].body.value == "first log message"
