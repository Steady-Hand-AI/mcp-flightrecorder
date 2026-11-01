"""Constructor and bounded control validation, separate from payload capture."""
from collections.abc import Mapping
import math

from ._values import InvalidInput, MISSING, freeze, validate_control


DEFAULT_LIMITS = {
    "ordinary_items": 256, "ordinary_bytes": 8_388_608,
    "completion_items": 64, "completion_bytes": 262_144,
    "batch_items": 64, "batch_bytes": 1_048_576,
    "batch_delay_ms": 20, "committed_cache_items": 512, "busy_timeout_ms": 1000,
}
RESERVATION_BYTES = 4096


def boolean(value, name):
    if type(value) is not bool:
        raise InvalidInput(name + " must be boolean")
    return value


def choice(value, allowed, name):
    if type(value) is not str or value not in allowed:
        raise InvalidInput("unsupported " + name)


def integer(value, name, *, nonnegative=False):
    if value is None:
        return None
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise InvalidInput(name + " must be a signed 64-bit integer")
    if nonnegative and value < 0:
        raise InvalidInput(name + " cannot be negative")


def fields(value, allowed, required=()):
    if type(value) is not dict or set(value) - set(allowed) or set(required) - set(value):
        raise InvalidInput("unsupported or missing fields")


def sequence(value, name):
    if type(value) not in (list, tuple):
        raise InvalidInput(name + " must be an ordered list or tuple")
    return value


def timeout_seconds(value):
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError("timeout must be positive finite seconds")
    return float(value)


def limits_config(value):
    if value is None:
        return dict(DEFAULT_LIMITS)
    if not isinstance(value, Mapping) or set(value) - set(DEFAULT_LIMITS):
        raise ValueError("limits must contain only supported keys")
    result = dict(DEFAULT_LIMITS)
    for name, limit in value.items():
        if type(limit) is not int or limit <= 0:
            raise ValueError("limits must be positive integers")
        result[name] = limit
    return result


def coverage_config(value):
    value = freeze(value)
    fields(value, ("producers", "model_complete"), ("producers",))
    fields(value["producers"], ("model", "host", "mcp"), ("model", "host", "mcp"))
    for boundary, producer in value["producers"].items():
        validate_control(producer, boundary + " producer", nullable=True, nonempty=True)
    value["model_complete"] = boolean(value.get("model_complete", False), "model_complete")
    if value["model_complete"] and value["producers"]["model"] is None:
        raise InvalidInput("complete model coverage requires a model producer")
    return value


def constructor_config(capture, coverage, comparisons, environment, limits):
    if type(capture) is not str or capture not in ("content", "metadata"):
        raise ValueError("capture must explicitly be content or metadata")
    boolean(comparisons, "comparisons")
    environment = freeze({} if environment is None else environment)
    if environment is MISSING:
        raise ValueError("environment must be declared JSON")
    return coverage_config(coverage), environment, limits_config(limits)
