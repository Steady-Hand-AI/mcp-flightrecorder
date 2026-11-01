# MCP Flight Recorder

**Version 0.5.16 · Python 3.11+ · SQLite schema 6**

MCP Flight Recorder records what an MCP host actually exposes: submitted model input, model tool choices, host dispatch, MCP operations, and tool results submitted back to the model. It keeps these observations and their explicit relationships in a portable SQLite recording.

The recorder uses a bounded queue and one capture-time writer thread. Its core and inspection CLI use only the Python standard library. Optional adapters observe existing SDK calls; the host continues to own clients, routing, retries, caches and stream consumption.

An accepted capture receipt means the observation entered the queue. It does not promise a durable commit. Missing observations, unavailable attribution and output limits remain explicit. Equal text, tool names, timestamps and provider IDs never establish provenance by themselves.

## Install from source

Select an available supported GIL-enabled CPython once and use the resulting environment for the commands below. Set `MFR_PYTHON` to choose an interpreter; otherwise the recipe uses `python3`. The preflight prints the actual Python and SQLite versions. [Testing](docs/testing.md) distinguishes contributor setup from completed qualification.

```sh
git clone https://github.com/Steady-Hand-AI/mcp-flightrecorder.git
cd mcp-flightrecorder
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
"$mfr_env/bin/python" -m pip install .
"$mfr_env/bin/fr" --version
```

Install an optional SDK adapter only when needed:

```sh
"$mfr_env/bin/python" -m pip install '.[openai-chat]'
"$mfr_env/bin/python" -m pip install '.[mcp]'
"$mfr_env/bin/python" -m pip install '.[anyllm-ollama]'
```

The MCP 1.23.1 session adapter uses the separate `mcp-session-legacy` extra and a separate environment. Adapter signatures, version constraints and host attachment examples are in [Integrations](docs/integrations.md).

## Try a recording without a service

From the source directory, generate a controlled recording in a new temporary directory:

```sh
recording_dir=$(mktemp -d)
"$mfr_env/bin/python" -I -B examples/quickstart.py "$recording_dir/run.sqlite"
"$mfr_env/bin/fr" turns "$recording_dir/run.sqlite"
"$mfr_env/bin/fr" schema
```

`quickstart.py` uses only the installed public API and creates one host observation without network access. It prints the new recording path. `fr check` may report `unknown` for this deliberately partial observation; that is not a failed recording. The recorder never overwrites or resumes an existing file. Longer deterministic scenarios used for regression testing live under `tests/producers/`, not `examples/`.

The installed `fr` command and `python -m mcp_flightrecorder` share one JSON interface. Use `fr explain`, `fr slice`, `fr payload` and `fr sql` to inspect a recording. `fr snapshot SOURCE OUT` creates a portable copy without changing the source. The five scoped reader commands (`check`, `turns`, `explain`, `payload`, `slice`) accept `--timeout` up to 30 seconds, default 10. With inferred `--run`, selection and operation are separate reads, each using that budget. The [inspection contract](docs/inspection.md) describes arguments, private snapshot-cache limits, output fields and exit codes.

## Use the capture API

```python
from mcp_flightrecorder import Recorder

recorder = Recorder(
    "new-recording.sqlite",
    capture="content",
    coverage={
        "producers": {"model": None, "host": "my-host", "mcp": None},
        "model_complete": False,
    },
)
try:
    with recorder.turn("Prepare context"):
        receipt = recorder.observe(
            "host.tool_result",
            scope=recorder.capture_scope(),
            payload={"text": "Host-prepared context"},
            facts={"text_paths": ["/text"]},
        )
finally:
    close_result = recorder.close()
```

Check capture receipts and `recorder.health()` when recording completeness matters. `close_result.portable` reports whether the stopped recording was finalized and verified as a standalone file. A portable recording can still contain incomplete capture. Use `capture="metadata"` when bodies should be omitted; metadata, structural membership and retention facts remain available.

See [Capture](docs/capture.md) for the complete public contract and [Architecture](docs/architecture.md) for admission, transaction, identity and shutdown rules.

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

The library installs only a `NullHandler`, so it stays silent until the application configures logging. It creates no log files and does not configure the root logger. Receipts, health and inspection JSON remain the authoritative results. See [logging behavior](docs/capture.md#6-operational-logging) for component names, limits and handler ownership.

## Develop and test

Local recipes accept a supported **GIL-enabled CPython 3.11+**. Record the actual Python, SQLite and dependency versions with each test run; the declared `>=3.11` metadata floor does not establish that every interpreter has been tested.

Follow the complete [Testing](docs/testing.md) recipe to select one interpreter, export a committed source tree, build in a disposable copy and install the wheel into fresh environments. It provides the **19-module SDK-free** selection and a separate **10-module integration** selection: four adapter/fixture modules plus six SDK-backed inspection modules. `pytest tests` includes both selections. Optional native host profiles use their own separate environments and direct requirements; legacy MCP 1.x must not share the modern MCP 2.x environment. A missing optional SDK is not a failed core test or a passed compatibility test. Tests construct recordings in temporary directories and check the examples with ordinary pytest assertions and independent SQL. The source distribution includes the seven declarative JSON inputs needed by the controlled integration scenario.

The [CI workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/.github/workflows/ci.yml) is configured for Linux core jobs on CPython 3.11 and 3.14 and a separate CPython 3.14 integration job. It checks the exported source and built distributions against an explicit public inventory, rebuilds from the sdist, and exercises installed artifacts. The workflow uses read-only repository permissions; it does not publish packages. Check the actual [Actions runs](https://github.com/Steady-Hand-AI/mcp-flightrecorder/actions) for observed results. Configuration alone does not establish a passing run.

## Documentation and source

| Resource | Purpose |
|---|---|
| [Architecture](docs/architecture.md) / [HTML map](docs/architecture.html) | Current modules, data flow and invariants |
| [Detailed control flows](docs/architecture.md#detailed-control-flows) | Component ownership, admission, persistence and failure-path diagrams |
| [Capture API](docs/capture.md) | Recorder construction, scopes, observations, lifecycle and shutdown |
| [Inspection API and CLI](docs/inspection.md) | Read-only queries, explanation, ancestry, integrity and snapshots |
| [Integrations](docs/integrations.md) | SDK facades, evidence receipts and host attachment recipes |
| [Testing](docs/testing.md) | Reproducible local suites and temporary fixtures |
| [Changelog](CHANGELOG.md) | Version history and unreleased changes |
| [Repository](https://github.com/Steady-Hand-AI/mcp-flightrecorder) | Source code |
| [Issues](https://github.com/Steady-Hand-AI/mcp-flightrecorder/issues) | Bug reports and feature requests |

Runtime code is under `src/mcp_flightrecorder/`; `sql/schema.sql` is the canonical schema. `tests/` contains the normal suite and independent fixture assertions, `compatibility_tests/` contains native host checks, and `examples/` contains short usage examples; `tests/producers/` holds the longer controlled test scenarios. Capture, slice and snapshot coverage exercises the installed library and CLI. Generated recordings, reports, dependency wheels, environment directories and historical handoffs are not part of the source package.

## License and ownership

Copyright 2026 Volodymyr Barylyak.

MCP Flight Recorder is licensed under the Apache License, Version 2.0 (`Apache-2.0`). See [LICENSE](LICENSE) for the complete terms and [NOTICE](NOTICE) for project attribution.
