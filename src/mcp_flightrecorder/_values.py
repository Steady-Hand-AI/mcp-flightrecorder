"""Small, provider-independent rules for frozen JSON and original comparisons."""


import hashlib
import hmac
import json
import math


class _Missing:
    __slots__ = ()

    def __repr__(self):
        return "MISSING"


MISSING = _Missing()


class InvalidInput(ValueError):
    """An exposed input does not meet the recorder's JSON/control contract."""


BASIS = "observed_original"
ARGS_PROJECTION = "args_json_python/1"
TEXT_PROJECTION = "text_blocks_json/1"
MESSAGE_PROJECTION = "message_json/1"
PROJECTION_LIMIT = 1_048_576


def _validate_keys(value):
    if any(type(key) is not str for key in value):
        raise InvalidInput("JSON object keys must be strings")


def _validate_number(value):
    if type(value) is float and not math.isfinite(value):
        raise InvalidInput("JSON numbers must be finite")


def _freeze_container(value, active):
    identity = id(value)
    if identity in active:
        raise InvalidInput("JSON containers cannot contain cycles")
    active.add(identity)
    try:
        if type(value) is dict:
            _validate_keys(value)
            return {key: _freeze(child, active) for key, child in value.items()}
        return [_freeze(child, active) for child in value]
    finally:
        active.remove(identity)


def _freeze(value, active):
    kind = type(value)
    if kind in (dict, list):
        return _freeze_container(value, active)
    if kind not in (str, int, float, bool, type(None)):
        raise InvalidInput("unsupported JSON value")
    _validate_number(value)
    return value


def freeze(value):
    """Detach ordinary JSON without invoking SDK or custom serialization hooks.

    MISSING is permitted at the exposed top level, never inside a JSON value.
    This traversal validates supported values; it is not a serialized-size
    estimator or a bound on caller-owned objects.
    """
    if value is MISSING:
        return value
    try:
        return _freeze(value, set())
    except RecursionError as error:
        raise InvalidInput("JSON nesting exceeds the capture boundary") from error


def json_text(value):
    """The unchanged unsorted reference representation, including key order."""
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise InvalidInput("JSON encoding is unavailable") from error


def canonical_bytes(value):
    """Typed Python JSON encoding used by declared destination comparisons."""
    frozen = freeze(value)
    if frozen is MISSING:
        raise InvalidInput("unexposed JSON has no canonical encoding")
    try:
        return json.dumps(frozen, sort_keys=True, ensure_ascii=True,
                          separators=(",", ":"), allow_nan=False).encode("ascii")
    except (TypeError, ValueError, RecursionError) as error:
        raise InvalidInput("canonical JSON encoding is unavailable") from error


def validate_control(value, name, *, nullable=False, nonempty=False, max_bytes=256):
    """Validate one bounded UTF-8 control string without coercing its type."""
    if nullable and value is None:
        return value
    if type(value) is not str or (nonempty and not value):
        raise InvalidInput(name + " must be a string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise InvalidInput(name + " must be valid UTF-8") from error
    if len(encoded) > max_bytes:
        raise InvalidInput(name + " exceeds its encoded bound")
    return value


def validate_pointer(pointer, *, allow_root=True):
    validate_control(pointer, "JSON Pointer", max_bytes=1024)
    if pointer == "" and allow_root:
        return pointer
    if not pointer.startswith("/"):
        raise InvalidInput("JSON Pointer must start with slash")
    for token in pointer[1:].split("/"):
        index = 0
        while index < len(token):
            if token[index] == "~":
                if index + 1 == len(token) or token[index + 1] not in "01":
                    raise InvalidInput("invalid JSON Pointer escape")
                index += 1
            index += 1
    return pointer


def pointer_tokens(pointer):
    validate_pointer(pointer)
    if pointer == "":
        return []
    return [token.replace("~1", "/").replace("~0", "~")
            for token in pointer[1:].split("/")]


def _pointer_index(parent, token):
    if type(parent) is dict:
        return token
    if type(parent) is not list:
        raise InvalidInput("JSON Pointer has no such container")
    if not token.isascii() or not token.isdigit() or (len(token) > 1 and token[0] == "0"):
        raise InvalidInput("invalid JSON Pointer array index")
    return int(token)


def pointer_get(value, pointer):
    current = value
    try:
        for token in pointer_tokens(pointer):
            current = current[_pointer_index(current, token)]
        return current
    except (KeyError, IndexError, TypeError, ValueError) as error:
        raise InvalidInput("JSON Pointer is absent from exposed JSON") from error


def pointer_replace(value, pointer, replacement):
    """Replace one existing nonroot location in a private frozen JSON copy."""
    tokens = pointer_tokens(pointer)
    if not tokens:
        raise InvalidInput("an extracted path must be a nonroot JSON Pointer")
    parent = value
    try:
        for token in tokens[:-1]:
            parent = parent[_pointer_index(parent, token)]
        index = _pointer_index(parent, tokens[-1])
        previous = parent[index]
        parent[index] = replacement
        return previous
    except (KeyError, IndexError, TypeError, ValueError) as error:
        raise InvalidInput("JSON Pointer is absent from exposed JSON") from error


def unavailable_digest(run_id, projection, reason):
    return {"value": None, "scope": run_id, "projection": projection,
            "basis": BASIS, "reason": reason}


def _projection_reason(value, projection, complete, enabled):
    if not enabled:
        return "disabled"
    if not complete:
        return "original_incomplete"
    if value is MISSING:
        return "unexposed"
    if projection == TEXT_PROJECTION and (
            type(value) is not list or any(type(item) is not str for item in value)):
        return "unsupported_projection"
    return None


def _encoded_digest(value, key, projection, *, utf8_size=None):
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"),
                               ensure_ascii=True, allow_nan=False)
    digest = hmac.new(key, b"mcp-flightrecorder\0" + BASIS.encode() +
                      b"\0" + projection.encode("ascii") + b"\0", hashlib.sha256)
    # Sorting keys preserves JSON length; ASCII escaping expands valid UTF-8
    # by at most six (including DEL). Encode only an already bounded value.
    if utf8_size is not None and utf8_size * 6 <= PROJECTION_LIMIT:
        encoded = encoder.encode(value).encode("ascii")
        if len(encoded) > PROJECTION_LIMIT:
            return None
        digest.update(encoded)
        return digest.hexdigest()
    encoded_bytes = 0
    for chunk in encoder.iterencode(value):
        encoded_bytes += len(chunk)
        if encoded_bytes > PROJECTION_LIMIT:
            return None
        digest.update(chunk.encode("ascii"))
    return digest.hexdigest()


def original_digest(value, *, key, run_id, projection, complete, enabled, utf8_size=None):
    """Hash one bounded exposed projection before any retention transformation."""
    reason = _projection_reason(value, projection, complete, enabled)
    if reason is not None:
        return unavailable_digest(run_id, projection, reason)
    try:
        observed = freeze(value)
        result = _encoded_digest(observed, key, projection, utf8_size=utf8_size)
        if result is None:
            return unavailable_digest(run_id, projection, "capture_limit")
        return {"value": result, "scope": run_id, "projection": projection, "basis": BASIS}
    except InvalidInput:
        return unavailable_digest(run_id, projection, "unsupported_value")
    except (RecursionError, ValueError, TypeError):
        return unavailable_digest(run_id, projection, "encoding_unavailable")


def _unique_members(items):
    result = {}
    for name, value in items:
        if name in result:
            raise InvalidInput("duplicate JSON member in raw arguments")
        result[name] = value
    return result


def _reject_constant(_):
    raise InvalidInput("nonfinite constant in raw arguments")


def _finite_float(text):
    value = float(text)
    _validate_number(value)
    return value


def parse_original_arguments(raw):
    if type(raw) is not str:
        raise InvalidInput("raw arguments must be a string")
    return json.loads(raw, object_pairs_hook=_unique_members,
                      parse_constant=_reject_constant, parse_float=_finite_float)


def _argument_value(payload):
    if payload is MISSING:
        return MISSING, None
    if type(payload) is not dict:
        raise InvalidInput("tool observation payload must be an object")
    parsed = payload.get("arguments", MISSING)
    if "arguments_raw" not in payload:
        return parsed, None
    raw = payload["arguments_raw"]
    if type(raw) is not str:
        raise InvalidInput("raw arguments must be a string")
    try:
        if len(raw.encode("utf-8")) > PROJECTION_LIMIT:
            return MISSING, "capture_limit"
        raw_value = parse_original_arguments(raw)
        if parsed is not MISSING and canonical_bytes(parsed) != canonical_bytes(raw_value):
            return MISSING, "conflicting_arguments"
        return raw_value if parsed is MISSING else parsed, None
    except (ValueError, RecursionError):
        return MISSING, "invalid_raw_arguments"


def arguments_digest(payload, *, key, run_id, complete, enabled):
    value, reason = _argument_value(payload)
    if not enabled:
        reason = "disabled"
    elif not complete:
        reason = "original_incomplete"
    if reason is not None:
        return unavailable_digest(run_id, ARGS_PROJECTION, reason)
    return original_digest(value, key=key, run_id=run_id, projection=ARGS_PROJECTION,
                           complete=complete, enabled=enabled)


def text_digest(value, text_paths, *, key, run_id, complete, enabled):
    if text_paths is None:
        return unavailable_digest(run_id, TEXT_PROJECTION,
                                  "disabled" if not enabled else "text_paths_unavailable")
    if type(text_paths) is not list:
        raise InvalidInput("text paths must be an ordered list or null")
    for path in text_paths:
        validate_pointer(path)
    try:
        blocks = [pointer_get(value, path) for path in text_paths]
    except InvalidInput:
        return unavailable_digest(run_id, TEXT_PROJECTION, "text_extraction_unavailable")
    return original_digest(blocks, key=key, run_id=run_id, projection=TEXT_PROJECTION,
                           complete=complete, enabled=enabled)
