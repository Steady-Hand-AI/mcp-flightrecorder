# MCP Flight Recorder

**Version 0.5.20 · Python 3.11+ · SQLite schema 6**

MCP Flight Recorder records what an MCP host actually exposes: submitted model input, model tool choices, host dispatch, MCP operations, and tool results submitted back to the model. It keeps these observations and their explicit relationships in a portable SQLite recording.

The recorder uses a bounded queue and one capture-time writer thread. Its core and inspection CLI use only the Python standard library. Optional adapters observe existing SDK calls; the host continues to own clients, routing, retries, caches and stream consumption.

An accepted capture receipt means the observation entered the queue. It does not promise a durable commit. Missing observations, unavailable attribution and output limits remain explicit. Equal text, tool names, timestamps and provider IDs never establish provenance by themselves.

## Install and record

Start in an empty directory. Install the pinned package from PyPI into a fresh environment, then run this complete SDK-free example. The core has no third-party runtime dependencies; no checkout, API key or network service is needed for the recording.

Choose a supported GIL-enabled CPython (`python3` by default, or set `MFR_PYTHON`). The preflight prints the actual Python and SQLite versions.

```sh
set -eu
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
"$mfr_env/bin/python" -m pip install 'mcp-flightrecorder==0.5.20'
"$mfr_env/bin/fr" --version
recording_dir=$(mktemp -d)
cd "$recording_dir"
"$mfr_env/bin/python" -I -B - "$recording_dir/run.sqlite" <<'PYTHON'
import sys
from mcp_flightrecorder.easy import AgentRecorder

messages = [{"role": "user", "content": "Echo alpha"}]
rec = AgentRecorder(sys.argv[1], capture="metadata", all_model_calls_recorded=True)
rec.start()
conversation = rec.conversation("demo")
try:
    with conversation.turn("Echo alpha"):
        with conversation.model_call({"messages": messages}) as call:
            response = {"name": "echo", "arguments": {"text": "alpha"}}
            call.done(response)  # This demo supplies a fixed, ordinary JSON response.
        choice = call.choice(response["name"], response["arguments"])
        with choice.run(local=True) as attempt:
            result = attempt.call(lambda text: text, **response["arguments"])
        messages.append(attempt.answer({"role": "tool", "content": result}, key="answer-1"))
    with conversation.turn("Continue"):
        with conversation.model_call({"messages": messages}, message_keys=[None, "answer-1"]) as call:
            call.done({"content": result})
finally:
    conversation.close()
    closed = rec.close()
print("Answer:", result)
print("Close:", closed)
assert closed.writer_stopped and closed.portable, closed
PYTHON
"$mfr_env/bin/fr" doctor "$recording_dir/run.sqlite"
"$mfr_env/bin/fr" turns "$recording_dir/run.sqlite" --last --text
"$mfr_env/bin/fr" explain "$recording_dir/run.sqlite" --last --text
```

The demo records manual JSON model observations, an explicitly invoked local tool and replay in a second turn. It uses metadata retention and writes a fresh SQLite file. Local results have a model association but no MCP-result text comparison; `result_unavailable` in inspection is expected. Doctor explains recorded gaps and cutoffs rather than declaring the whole run correct. Use another new directory or filename for another recording.

For native SDK calls, install the optional extras into the same environment:

```sh
"$mfr_env/bin/python" -m pip install 'mcp-flightrecorder[openai-chat,mcp]==0.5.20'
```

The host continues to own SDK clients and their initialization/close. Keep legacy MCP 1.x in its separate environment. See [Easy integration](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/easy-integration.md) for the supported routes and [Integrations](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/integrations.md) for existing adapter contracts.

## Add recording to your host

Start an `AgentRecorder` in the worker, create one conversation handle, and wrap the native MCP session once. Inside your existing async turn loop, keep request construction, response parsing and tool selection in your host:

```python
from mcp_flightrecorder.easy import AgentRecorder

rec = AgentRecorder("new-recording.sqlite", capture="metadata")
rec.start()
conversation = rec.conversation("chat-123")
tools = rec.mcp(native_session, kind="session")
try:
    with conversation.turn(user_input):
        with conversation.model_call(payload) as call:
            response = await existing_model_call(payload)
            call.done(response)
        name, arguments = parse_selected_tool(response)
        choice = call.choice(name, arguments)
        with choice.run() as attempt:
            result = await tools.call_tool(name, arguments)
        messages.append(attempt.answer(make_tool_message(result)))
finally:
    conversation.close()
    closed = await rec.aclose()
```

This is an attachment fragment: `native_session`, model calls, parsing, messages and result formatting belong to the application. The [complete guide](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/easy-integration.md) covers startup/close checks, native OpenAI, replay keys, selected retry attempts, parallel tools, prepared messages, local calls and consumed streams. It also explains limits, health and fresh-path recovery. `path=None` disables recording; construction alone starts no writer. Declare `all_model_calls_recorded=True` only when your host actually promises that coverage.

An answer binds the invocation your host selected, including unavailable capture. The library handles receipts, event IDs and insertion annotations. It never substitutes an earlier successful retry, guesses provenance from equal text, or changes native execution because capture is full. The [model → tool → model walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/tool_roundtrip.md) remains a complete lower-level SDK-backed example.

The installed `fr` command and `python -m mcp_flightrecorder` share the same readers. Existing commands retain JSON defaults; `turns --text` and `explain --text` add readable labels and IDs, and `doctor --json` exposes the separate diagnostic envelope. `--last` selects a recorded turn; files with multiple runs require `--run`. See the [inspection contract](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/inspection.md) for bounds, preparation explanations and exit codes.

## Inspect through MCP and verify evidence

Install `mcp-flightrecorder[mcp]==0.5.20` to launch `fr mcp /absolute/path/to/recording.sqlite` from an MCP stdio client. The operator selects one stable portable recording at startup; tools cannot select other files or run SQL. Five metadata-only tools and one schema resource are available by default. `--allow-content` adds explicit, bounded payload retrieval. Metadata can still be sensitive; no-content output is not anonymized.

Use `fr snapshot live.sqlite copy.sqlite` or `inspect.snapshot(...)` separately when a portable copy is needed. The MCP server does not acquire live files, watch for writers or recover WAL filesets. The [MCP client walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/mcp-inspection.md) creates a recording, launches a genuine subprocess client, follows ancestry and retrieves content only when enabled.

For CI, `fr gate` evaluates the complete selected run under explicit coverage and activity expectations. For example, after the SDK tool-roundtrip example:

```sh
fr gate run.sqlite --expect model,host,mcp --min-turns 1 \
    --min-model-requests 2 --min-model-responses 2 --min-decisions 1 \
    --min-tool-calls 1 --min-answer-occurrences 1
```

Exit 0 means the stated evidence requirements passed; failed requirements exit 2 and unverified evidence exits 4. The gate preserves raw text mismatches while recognizing fully validated preparation. It cannot certify answer quality or prove that no unobserved calls occurred. The local metadata quickstart above has no MCP-result comparison and is not a passing gate example. See the [gate policy and limits](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/inspection.md#evidence-gate).

## Use the capture API

Check capture receipts and `recorder.health()` when recording completeness matters. `close_result.portable` reports whether the stopped recording was finalized and verified as a standalone file. A portable recording can still contain incomplete capture. Use `capture="metadata"` when bodies should be omitted; metadata, structural membership and retention facts remain available.

See [Capture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/capture.md) for the complete public contract and [Architecture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/architecture.md) for admission, transaction, identity and shutdown rules.

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

The library installs only a `NullHandler`, so it stays silent until the application configures logging. It creates no log files and does not configure the root logger. Receipts, health and inspection JSON remain the authoritative results. See [logging behavior](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/capture.md#6-operational-logging) for component names, limits and handler ownership.

## Develop and test

Local recipes accept a supported **GIL-enabled CPython 3.11+**. Record the actual Python, SQLite and dependency versions with each test run; the declared `>=3.11` metadata floor does not establish that every interpreter has been tested.

### Tagged source and source distributions

Source-shipped examples are available in tag `0.5.20` and the matching source distribution. They are not installed by the wheel. From an empty directory, obtain the tagged source:

```sh
git clone --depth 1 --branch 0.5.20 https://github.com/Steady-Hand-AI/mcp-flightrecorder.git
cd mcp-flightrecorder
mfr_source=$(pwd -P)
```

Alternatively, download `mcp_flightrecorder-0.5.20.tar.gz` from the [PyPI release files](https://pypi.org/project/mcp-flightrecorder/0.5.20/#files), extract it, and set `mfr_source` to the absolute path of its `mcp_flightrecorder-0.5.20` directory. Continue with the same fresh source installation and run the source-shipped facade demo outside that directory:

```sh
mfr_python=$(command -v "${MFR_PYTHON:-python3}") || exit 1
mfr_source_env=$(mktemp -d /tmp/mfr-source-env-XXXXXX)
"$mfr_python" -m venv "$mfr_source_env"
"$mfr_source_env/bin/python" -m pip install "$mfr_source"
"$mfr_source_env/bin/fr" --version
recording_dir=$(mktemp -d)
(
    cd "$recording_dir"
    "$mfr_source_env/bin/python" -I -B "$mfr_source/examples/agent_recorder.py" --output "$recording_dir/run.sqlite"
    "$mfr_source_env/bin/fr" doctor "$recording_dir/run.sqlite"
)
```

Install source extras with `"$mfr_source_env/bin/python" -m pip install "$mfr_source[openai-chat]"`, substituting `mcp` or `anyllm-ollama` as needed. Keep the legacy MCP adapter in its separate environment. The [complete SDK walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/tool_roundtrip.md) gives matching pinned-package/tagged-source commands for the model → MCP tool → model example.

Follow the complete [Testing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/testing.md) recipe to select one interpreter, export a committed source tree, build in a disposable copy and install the wheel into fresh environments. It provides explicit SDK-free and modern integration selections, including installed CLI, facade and example checks. `pytest tests` includes both selections. Optional native host profiles use their own separate environments and direct requirements; legacy MCP 1.x must not share the modern MCP 2.x environment. A missing optional SDK is not a failed core test or a passed compatibility test. Tests construct recordings in temporary directories and check the examples with ordinary pytest assertions and independent SQL. The source distribution includes the seven declarative JSON inputs needed by the controlled integration scenario.

The [CI workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/.github/workflows/ci.yml) is configured for Linux core jobs on CPython 3.11 and 3.14 and a separate CPython 3.14 integration job. It checks the exported source and built distributions against an explicit public inventory, rebuilds from the sdist, and exercises installed artifacts. The workflow uses read-only repository permissions; it does not publish packages. Check the actual [Actions runs](https://github.com/Steady-Hand-AI/mcp-flightrecorder/actions) for observed results. Configuration alone does not establish a passing run.

The manual [release workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/releasing.md) provides validation and staged publication through TestPyPI, PyPI and GitHub. It promotes one reviewed wheel/sdist pair, verifies downloaded bytes and installations between stages, and uses separate registry approval gates. The [release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/release-notes.md) describe the changes in 0.5.20.

## Documentation and source

| Resource | Purpose |
|---|---|
| [Easy integration](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/easy-integration.md) | Concepts, host attachment, replay/retries, diagnostics and operations |
| [Architecture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/architecture.md) / [HTML map](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/architecture.html) | Current modules, data flow and invariants |
| [Detailed control flows](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/architecture.md#detailed-control-flows) | Component ownership, admission, persistence and failure-path diagrams |
| [Capture API](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/capture.md) | Recorder construction, scopes, observations, lifecycle and shutdown |
| [Inspection API and CLI](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/inspection.md) | Read-only queries, explanation, ancestry, integrity and snapshots |
| [Integrations](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/integrations.md) | SDK facades, evidence receipts and host attachment recipes |
| [Complete tool roundtrip](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/tool_roundtrip.md) | Run local SDK peers and investigate prepared tool text |
| [Testing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/testing.md) | Reproducible local suites and temporary fixtures |
| [Releasing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/releasing.md) | Candidate validation, registry setup and staged publication |
| [Release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/release-notes.md) | Changes in 0.5.20 |
| [Changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/CHANGELOG.md) | Version history and unreleased changes |
| [Repository](https://github.com/Steady-Hand-AI/mcp-flightrecorder) | Source code |
| [Issues](https://github.com/Steady-Hand-AI/mcp-flightrecorder/issues) | Bug reports and feature requests |

Runtime code is under `src/mcp_flightrecorder/`; `sql/schema.sql` is the canonical schema. `tests/` contains the normal suite and independent fixture assertions, `compatibility_tests/` contains native host checks, and `examples/` contains short usage examples; `tests/producers/` holds the longer controlled test scenarios. Capture, slice and snapshot coverage exercises the installed library and CLI. Generated recordings, reports, dependency wheels, environment directories and historical handoffs are not part of the source package.

## License and ownership

Copyright 2026 Volodymyr Barylyak.

MCP Flight Recorder is licensed under the Apache License, Version 2.0 (`Apache-2.0`). See [LICENSE](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/LICENSE) for the complete terms and [NOTICE](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/NOTICE) for project attribution.
