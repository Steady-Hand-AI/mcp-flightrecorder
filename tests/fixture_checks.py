"""Small independent SQLite/JSON checks shared by the two example fixtures."""

import json


def same_typed_value(actual, expected):
    """Keep nested JSON types distinct, including booleans and numbers."""
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            same_typed_value(actual[key], expected[key]) for key in expected
        )
    if isinstance(expected, (list, tuple)):
        return len(actual) == len(expected) and all(
            same_typed_value(left, right) for left, right in zip(actual, expected)
        )
    return actual == expected


def body(connection, artifact_id, name="referenced artifact"):
    row = connection.execute("SELECT * FROM artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()
    assert row is not None and row["body_text"] is not None, f"{name} must have a retained body"
    return json.loads(row["body_text"])


def set_pointer(value, pointer, replacement):
    parts = [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]
    target = value
    for part in parts[:-1]:
        target = target[int(part) if isinstance(target, list) else part]
    target[int(parts[-1]) if isinstance(target, list) else parts[-1]] = replacement


def reconstruct(connection, manifest, name="request"):
    value = body(connection, manifest["template_ref"], f"{name} template")
    members = [body(connection, item, f"{name} message {index}")
               for index, item in enumerate(manifest["message_refs"])]
    set_pointer(value, manifest["message_path"], members)
    for field in manifest["field_refs"]:
        set_pointer(value, field["pointer"], body(connection, field["artifact_id"], f"{name} extracted field"))
    return value


def query(connection, sql_dir, name, **params):
    sql = (sql_dir / f"{name}.sql").read_text(encoding="utf-8")
    return [dict(row) for row in connection.execute(sql, params)]


def check_token(token, scope, projection, name):
    assert isinstance(token, dict), f"{name} must expose its original-comparison token"
    for key, expected in (("basis", "observed_original"), ("projection", projection), ("scope", scope)):
        assert same_typed_value(token.get(key), expected), f"{name} must have compatible {key}"
    value = token.get("value")
    assert (isinstance(value, str) and len(value) == 64
            and all(char in "0123456789abcdef" for char in value)), f"{name} must have a 256-bit comparison value"
    return token
