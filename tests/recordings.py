"""Generate reader inputs from test-owned producers, once per test process.

Only declarative service inputs live in the checkout. Recording databases stay
in temporary storage; tests that change them must make their own copies.
"""
import atexit
from functools import cache
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
_TEMPORARY = TemporaryDirectory(prefix="mfr-test-recordings-")
atexit.register(_TEMPORARY.cleanup)


def _generate(producer, name, *arguments):
    directory = Path(_TEMPORARY.name)
    path = directory / f"{name}.sqlite"
    process = subprocess.run(
        [sys.executable, "-I", "-B", str(ROOT / "tests" / "producers" / producer),
         "--output", str(path), *arguments],
        cwd=directory, capture_output=True, text=True, timeout=50,
    )
    assert process.returncode == 0, (
        f"Recording producer {producer} failed:\n{process.stdout}\n{process.stderr}"
    )
    summary = json.loads(process.stdout)
    assert summary["close"]["writer_stopped"] and summary["close"]["portable"], (
        "Reader inputs must be stopped, portable recordings before inspection."
    )
    return path, summary


@cache
def core_recording():
    """Return the core recording path and its producer-assigned identities."""
    return _generate("record_core.py", "core")


@cache
def integration_recording():
    """Return the SDK recording path and its producer-assigned symbol map."""
    path, summary = _generate(
        "minimal_host.py", "integration", "--inputs",
        str(ROOT / "tests" / "fixtures" / "integration"),
    )
    return path, summary["symbols"]
