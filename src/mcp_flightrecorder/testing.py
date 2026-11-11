"""Evidence assertions for ordinary Python tests."""
from . import inspect
from ._inspect_text import render

__all__ = ["EvidenceReadError", "assert_evidence"]


class EvidenceReadError(Exception):
    """An unavailable verdict, with the original reason in `reason`."""

    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


def assert_evidence(path, *, run=None, expect=("model", "host", "mcp"),
                    min_activity=None, allow_local_tools=False):
    """Return a passing gate result for an explicit recording file.

    FAIL and UNKNOWN raise AssertionError with the bounded gate explanation.
    Unavailable evidence and invalid configuration raise EvidenceReadError.
    Only the Boolean True selects MCP-only answer comparison.
    """
    if type(allow_local_tools) is not bool:
        raise EvidenceReadError("invalid_allow_local_tools", "allow_local_tools must be a boolean.")
    if min_activity is None:
        min_activity = {"answer_occurrences": 1}
    result = inspect.gate(path, run_id=run, expect=expect, min_activity=min_activity,
                          answers="mcp" if allow_local_tools else "all")
    if not result["ok"]:
        error = result["error"]
        raise EvidenceReadError(error["reason"], error["message"])
    if result["data"]["status"] == "pass":
        return result
    raise AssertionError(render(result))
