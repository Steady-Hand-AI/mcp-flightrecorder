# MCP Flight Recorder

**Version 0.5.17 · Python 3.11+ · SQLite schema 6**

MCP Flight Recorder records what an MCP host actually exposes: submitted model input, model tool choices, host dispatch, MCP operations, and tool results submitted back to the model. It keeps these observations and their explicit relationships in a portable SQLite recording.

The recorder uses a bounded queue and one capture-time writer thread. Its core and inspection CLI use only the Python standard library. Optional adapters observe existing SDK calls; the host continues to own clients, routing, retries, caches and stream consumption.

An accepted capture receipt means the observation entered the queue. It does not promise a durable commit. Missing observations, unavailable attribution and output limits remain explicit. Equal text, tool names, timestamps and provider IDs never establish provenance by themselves.

## Install

Use the version pin below to install MCP Flight Recorder from [PyPI](https://pypi.org/project/mcp-flightrecorder/0.5.17/). The core has no third-party runtime dependencies.

Select an available supported GIL-enabled CPython once and use the resulting environment for the commands below. The recipe uses `python3` by default. Set `MFR_PYTHON` to choose another supported interpreter. The preflight prints the actual Python and SQLite versions. [Testing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/testing.md) distinguishes contributor setup from completed qualification.

```sh
mfr_python=$(command -v "${MFR_PYTHON:-python3}") || exit 1
"$mfr_python" -I -B - <<'PYTHON'
import sys, sqlite3
assert sys.implementation.name == "cpython"
assert sys.version_info[:2] >= (3, 11)
assert sys.version_info.releaselevel == "final"
assert getattr(sys, "_is_gil_enabled", lambda: True)()
assert not sys.flags.optimize
print(sys.version, sqlite3.sqlite_version, sys.executable)
PYTHON
mfr_env=$(mktemp -d /tmp/mfr-env-XXXXXX)
"$mfr_python" -m venv "$mfr_env"
"$mfr_env/bin/python" -m pip install 'mcp-flightrecorder==0.5.17'
"$mfr_env/bin/fr" --version
```

Install an optional SDK adapter only when needed:

```sh
"$mfr_env/bin/python" -m pip install 'mcp-flightrecorder[openai-chat]==0.5.17'
"$mfr_env/bin/python" -m pip install 'mcp-flightrecorder[mcp]==0.5.17'
"$mfr_env/bin/python" -m pip install 'mcp-flightrecorder[anyllm-ollama]==0.5.17'
```

The MCP 1.23.1 session adapter uses the separate `mcp-session-legacy` extra and a separate environment. Adapter signatures, version constraints and host attachment examples are in [Integrations](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/integrations.md).

## Try a recording without a service

Run these commands from any directory; no checkout or service is needed:

```sh
recording_dir=$(mktemp -d)
"$mfr_env/bin/python" -I -B - "$recording_dir/run.sqlite" <<'PYTHON'
import sys
from mcp_flightrecorder import Recorder

recorder = Recorder(
    sys.argv[1],
    capture="content",
    coverage={
        "producers": {"model": None, "host": "quickstart", "mcp": None},
        "model_complete": False,
    },
)
try:
    with recorder.turn("Prepare context") as turn:
        if turn.receipt.status != "accepted":
            raise RuntimeError(f"Turn not captured: {turn.receipt.reason}")
        receipt = recorder.observe(
            "host.tool_result",
            scope=recorder.capture_scope(),
            payload={"text": "Host-prepared context"},
            facts={"text_paths": ["/text"]},
        )
        if receipt.status != "accepted":
            raise RuntimeError(f"Observation not captured: {receipt.reason}")
finally:
    close_result = recorder.close()
if not close_result.writer_stopped or not close_result.portable:
    raise RuntimeError(f"Recording not portable: {close_result.reason}")
print(sys.argv[1])
PYTHON
"$mfr_env/bin/fr" turns "$recording_dir/run.sqlite"
"$mfr_env/bin/fr" schema
```

This uses only the installed public API and creates one host observation without network access. It prints the new recording path. `fr check` may report `unknown` for this deliberately partial observation; that is not a failed recording. The recorder never overwrites or resumes an existing file. The source checkout also provides this example as `examples/quickstart.py`; longer deterministic regression scenarios live under `tests/producers/`.

The installed `fr` command and `python -m mcp_flightrecorder` share one JSON interface. Use `fr explain`, `fr slice`, `fr payload` and `fr sql` to inspect a recording. `fr snapshot SOURCE OUT` creates a portable copy without changing the source. The five scoped reader commands (`check`, `turns`, `explain`, `payload`, `slice`) accept `--timeout` up to 30 seconds, default 10. With inferred `--run`, selection and operation are separate reads, each using that budget. The [inspection contract](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/inspection.md) describes arguments, private snapshot-cache limits, output fields and exit codes.

## Use the capture API

Check capture receipts and `recorder.health()` when recording completeness matters. `close_result.portable` reports whether the stopped recording was finalized and verified as a standalone file. A portable recording can still contain incomplete capture. Use `capture="metadata"` when bodies should be omitted; metadata, structural membership and retention facts remain available.

See [Capture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/capture.md) for the complete public contract and [Architecture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/architecture.md) for admission, transaction, identity and shutdown rules.

## Operational logging

The library uses Python's standard `logging` package under `mcp_flightrecorder`. Configure logging in your host application before creating a recorder:

```python
import logging

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logging.getLogger("mcp_flightrecorder").setLevel(logging.INFO)
```

`INFO` reports startup, clean portable closure and verified snapshots. `WARNING` reports capture loss, incomplete closure, busy/timeout finalization and operational inspection failures. `ERROR` reports unexpected capture/writer failure, lost admitted prerequisites and failed snapshot cleanup. Repeated capture reasons are bounded; captured content, paths, SQL and raw exceptions are excluded.

The library installs only a `NullHandler`, so it stays silent until the application configures logging. It creates no log files and does not configure the root logger. Receipts, health and inspection JSON remain the authoritative results. See [logging behavior](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/capture.md#6-operational-logging) for component names, limits and handler ownership.

## Develop and test

Local recipes accept a supported **GIL-enabled CPython 3.11+**. Record the actual Python, SQLite and dependency versions with each test run; the declared `>=3.11` metadata floor does not establish that every interpreter has been tested.

### Install from source

From an empty directory, clone tag `0.5.17` and install it into a fresh environment with your selected interpreter.

```sh
git clone --depth 1 --branch 0.5.17 https://github.com/Steady-Hand-AI/mcp-flightrecorder.git
cd mcp-flightrecorder
mfr_python=$(command -v "${MFR_PYTHON:-python3}") || exit 1
mfr_source_env=$(mktemp -d /tmp/mfr-source-env-XXXXXX)
"$mfr_python" -m venv "$mfr_source_env"
"$mfr_source_env/bin/python" -m pip install .
"$mfr_source_env/bin/fr" --version
```

Install source extras with `"$mfr_source_env/bin/python" -m pip install '.[openai-chat]'`, substituting `mcp` or `anyllm-ollama` as needed. Keep the legacy MCP adapter in its separate environment.

Follow the complete [Testing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/testing.md) recipe to select one interpreter, export a committed source tree, build in a disposable copy and install the wheel into fresh environments. It provides the **19-module SDK-free** selection and a separate **10-module integration** selection: four adapter/fixture modules plus six SDK-backed inspection modules. `pytest tests` includes both selections. Optional native host profiles use their own separate environments and direct requirements; legacy MCP 1.x must not share the modern MCP 2.x environment. A missing optional SDK is not a failed core test or a passed compatibility test. Tests construct recordings in temporary directories and check the examples with ordinary pytest assertions and independent SQL. The source distribution includes the seven declarative JSON inputs needed by the controlled integration scenario.

The [CI workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/.github/workflows/ci.yml) is configured for Linux core jobs on CPython 3.11 and 3.14 and a separate CPython 3.14 integration job. It checks the exported source and built distributions against an explicit public inventory, rebuilds from the sdist, and exercises installed artifacts. The workflow uses read-only repository permissions; it does not publish packages. Check the actual [Actions runs](https://github.com/Steady-Hand-AI/mcp-flightrecorder/actions) for observed results. Configuration alone does not establish a passing run.

The manual [release workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/releasing.md) provides validation and staged publication through TestPyPI, PyPI and GitHub. It promotes one reviewed wheel/sdist pair, verifies downloaded bytes and installations between stages, and uses separate registry approval gates. The [release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/release-notes.md) describe the changes in 0.5.17.

## Documentation and source

| Resource | Purpose |
|---|---|
| [Architecture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/architecture.md) / [HTML map](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/architecture.html) | Current modules, data flow and invariants |
| [Detailed control flows](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/architecture.md#detailed-control-flows) | Component ownership, admission, persistence and failure-path diagrams |
| [Capture API](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/capture.md) | Recorder construction, scopes, observations, lifecycle and shutdown |
| [Inspection API and CLI](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/inspection.md) | Read-only queries, explanation, ancestry, integrity and snapshots |
| [Integrations](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/integrations.md) | SDK facades, evidence receipts and host attachment recipes |
| [Testing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/testing.md) | Reproducible local suites and temporary fixtures |
| [Releasing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/releasing.md) | Candidate validation, registry setup and staged publication |
| [Release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/release-notes.md) | Changes in 0.5.17 |
| [Changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/CHANGELOG.md) | Version history and unreleased changes |
| [Repository](https://github.com/Steady-Hand-AI/mcp-flightrecorder) | Source code |
| [Issues](https://github.com/Steady-Hand-AI/mcp-flightrecorder/issues) | Bug reports and feature requests |

Runtime code is under `src/mcp_flightrecorder/`; `sql/schema.sql` is the canonical schema. `tests/` contains the normal suite and independent fixture assertions, `compatibility_tests/` contains native host checks, and `examples/` contains short usage examples; `tests/producers/` holds the longer controlled test scenarios. Capture, slice and snapshot coverage exercises the installed library and CLI. Generated recordings, reports, dependency wheels, environment directories and historical handoffs are not part of the source package.

## License and ownership

Copyright 2026 Volodymyr Barylyak.

MCP Flight Recorder is licensed under the Apache License, Version 2.0 (`Apache-2.0`). See [LICENSE](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/LICENSE) for the complete terms and [NOTICE](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/NOTICE) for project attribution.
