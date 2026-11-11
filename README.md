# MCP Flight Recorder

**Version 0.5.22 · Python 3.11+ · SQLite schema 6**

MCP Flight Recorder records what an MCP host actually exposes: submitted model input, model tool choices, host dispatch, MCP operations, and tool results submitted back to the model. It keeps these observations and their explicit relationships in a portable SQLite recording.

The recorder uses a bounded queue and one capture-time writer thread. Its core and inspection CLI use only the Python standard library. Optional adapters observe existing SDK calls; the host continues to own clients, routing, retries, caches and stream consumption.

An accepted capture receipt means the observation entered the queue. It does not promise a durable commit. Missing observations, unavailable attribution and output limits remain explicit. Equal text, tool names, timestamps and provider IDs never establish provenance by themselves.

## Install and record

Try four **synthetic controlled observations** without credentials, an SDK or a live model. Once installed, the demo needs no source checkout and makes no network calls. It writes fresh recordings and prints complete commands to investigate them.

Version 0.5.22 includes the installed demo, fresh directory destinations and readable gate verdicts. See [source installation](#install-this-source-checkout) for development. Use a fresh environment:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install mcp-flightrecorder==0.5.22
python -m mcp_flightrecorder.demo
```

Use GIL-enabled CPython 3.11+. Both installed commands, `fr` and `mcp-flightrecorder`, share the same CLI and version. With [uv's isolated tools](https://docs.astral.sh/uv/guides/tools/), the equivalent route is:

```sh
uvx --from mcp-flightrecorder==0.5.22 mcp-flightrecorder demo
```

Use the printed **uvx** commands for follow-ups from that route; it does not install a global `fr`. For an unreleased wheel, use that same wheel as the package source in the launch and printed uvx commands. `--output-dir PARENT` selects an existing output parent; every invocation creates a fresh subdirectory, including repeated calls with the same parent.

| Recording | Default gate verdict | Expected exit |
|---|---|---|
| `faithful.sqlite` | PASS: supported evidence is consistent | 0 |
| `mutated.sqlite` | FAIL: the submitted answer differs from its recorded source | 2 |
| `incomplete.sqlite` | UNKNOWN: required evidence is unavailable | 4 |
| `mixed.sqlite` | UNKNOWN: the default includes an unavailable local-to-MCP comparison | 4 |

Demo generation itself exits 0 on success. Copy its printed gate commands to see the separate verdicts. FAIL and UNKNOWN are intentional here: run those commands individually, without `set -e` aborting the tour or `|| true` hiding unexpected errors. Exits 1 and 3 are operational errors, not evidence verdicts.

The demo also prints the explicit `--answers mcp` command for `mixed.sqlite`: it passes with one validated local occurrence excluded from result-text comparison. That does not verify local forwarding. Default `--answers all`, raw comparisons and other integrity/coverage/health requirements stay intact. Narrow scope only when it matches the intended policy, never merely to obtain PASS.

Follow the [agent investigation workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/agent-workflow.md) to resolve one file/run, diagnose the mutation and cite actual event IDs. For a complete SDK-free application example, use the preserved [host recording quickstart](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/easy-integration.md#complete-sdk-free-host-example); for real applications, use [host integration](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/easy-integration.md). These synthetic examples establish behavior, not application correctness or real-world adoption.

For native SDK calls, install the optional extras into the same environment:

```sh
.venv/bin/python -m pip install 'mcp-flightrecorder[openai-chat,mcp]==0.5.22'
```

The host continues to own SDK clients and their initialization/close. Keep legacy MCP 1.x in its separate environment. See [Easy integration](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/easy-integration.md) for the supported routes and [Integrations](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/integrations.md) for existing adapter contracts.

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

This is an attachment fragment: `native_session`, model calls, parsing, messages and result formatting belong to the application. The [complete guide](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/easy-integration.md) covers startup/close checks, native OpenAI, replay keys, selected retry attempts, parallel tools, prepared messages, local calls and consumed streams. It also explains limits, health and fresh-path recovery. `path=None` disables recording; construction alone starts no writer. Declare `all_model_calls_recorded=True` only when your host actually promises that coverage.

An answer binds the invocation your host selected, including unavailable capture. The library handles receipts, event IDs and insertion annotations. It never substitutes an earlier successful retry, guesses provenance from equal text, or changes native execution because capture is full. The [model → tool → model walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/tool_roundtrip.md) remains a complete lower-level SDK-backed example.

The installed `fr` command and `python -m mcp_flightrecorder` share the same readers. Existing commands retain JSON defaults; `turns --text` and `explain --text` add readable labels and IDs, and `doctor --json` exposes the separate diagnostic envelope. `--last` selects a recorded turn; files with multiple runs require `--run`. See the [inspection contract](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/inspection.md) for bounds, preparation explanations and exit codes.

## Record a stdio MCP server

For a host whose MCP server launch command you can edit, use the installed recording bridge:

```text
fr proxy --record /absolute/path/to/new-recording.sqlite -- SERVER_COMMAND ARGUMENT...
```

One client connects to one child server. Native bytes keep their order; capture observes supported `tools/list` and `tools/call` exchanges without retrying native calls. Recording defaults to metadata; `--capture content` opts into retained bodies, and omitting `--record` forwards without a database. Keep stderr separate from protocol stdout and use a fresh recording path.

This records the MCP boundary only: no model choices, host turns or prepared-answer relationships are invented. Use `fr check recording.sqlite --expect mcp` and the [proxy walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/proxy.md) for the deterministic source-shipped peer, public operation/payload queries and real MCP inspection. The existing host evidence gate is not a passing check for an MCP-only recording. The bridge requires POSIX pipes; capture loss is visible on stderr and does not repair or conceal native transport failure.

## Inspect through MCP and verify evidence

Install `mcp-flightrecorder[mcp]==0.5.22` to launch `fr mcp /absolute/path/to/recording.sqlite` from an MCP stdio client. The operator selects one stable portable recording at startup; tools cannot select other files or run SQL. Five metadata-only tools and one schema resource are available by default. `--allow-content` adds explicit, bounded payload retrieval. Metadata can still be sensitive; no-content output is not anonymized.

Use `fr snapshot live.sqlite copy.sqlite` or `inspect.snapshot(...)` separately when a portable copy is needed. The MCP server does not acquire live files, watch for writers or recover WAL filesets. The [MCP client walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/mcp-inspection.md) creates a recording, launches a genuine subprocess client, follows ancestry and retrieves content only when enabled.

For CI, `fr gate` evaluates the complete selected run under explicit coverage and activity expectations. For example, after the SDK tool-roundtrip example:

```sh
fr gate run.sqlite --expect model,host,mcp --min-turns 1 \
    --min-model-requests 2 --min-model-responses 2 --min-decisions 1 \
    --min-tool-calls 1 --min-answer-occurrences 1
```

Exit 0 means the stated evidence requirements passed; failed requirements exit 2 and unverified evidence exits 4. The gate preserves raw text mismatches while recognizing fully validated preparation. It cannot certify answer quality or prove that no unobserved calls occurred. The [SDK-free local host example](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/easy-integration.md#complete-sdk-free-host-example) uses metadata capture and has no MCP-result comparison, so it is not a passing gate example. It is distinct from the synthetic demo above and the MCP-backed attachment fragment. See the [gate policy and limits](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/inspection.md#evidence-gate).

## Use the capture API

Check capture receipts and `recorder.health()` when recording completeness matters. `close_result.portable` reports whether the stopped recording was finalized and verified as a standalone file. A portable recording can still contain incomplete capture. Use `capture="metadata"` when bodies should be omitted; metadata, structural membership and retention facts remain available.

See [Capture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/capture.md) for the complete public contract and [Architecture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/architecture.md) for admission, transaction, identity and shutdown rules.

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

Library logging installs only a `NullHandler`; it creates no log files and does not configure the root logger. Applications choose handlers. Separately, `fr proxy` deliberately emits bounded capture/cleanup diagnostics to stderr and inherits the child's native stderr. Protocol stdout remains reserved for the child. Preserve proxy diagnostics alongside receipts, health and inspection JSON: observer loss can occur before recorder admission. See [logging behavior](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/capture.md#6-operational-logging) for component names, limits and handler ownership.

## Develop and test

Local recipes accept a supported **GIL-enabled CPython 3.11+**. Record the actual Python, SQLite and dependency versions with each test run; the declared `>=3.11` metadata floor does not establish that every interpreter has been tested.

### Install this source checkout

To work from source, install the current checkout into a fresh environment and run the packaged demo outside the source directory:

```sh
mfr_source=$(pwd -P)
mfr_work=$(mktemp -d /tmp/mfr-demo-XXXXXX)
python3 -m venv "$mfr_work/env"
"$mfr_work/env/bin/python" -m pip install "$mfr_source"
cd "$mfr_work"
. "$mfr_work/env/bin/activate"
python -I -B -m mcp_flightrecorder.demo
```

A built wheel from the same source is another package-acquisition input. For isolated tools, use `uvx --from /absolute/path/to/package.whl mcp-flightrecorder demo` and retain that package source for the printed uvx follow-ups. Use the same package version for the initial launch and its follow-ups.

### Tagged source and source distributions

Source-shipped examples are available in tag `0.5.22` and the matching source distribution. They are not installed by the wheel. From an empty directory, obtain the tagged source:

```sh
git clone --depth 1 --branch 0.5.22 https://github.com/Steady-Hand-AI/mcp-flightrecorder.git
cd mcp-flightrecorder
mfr_source=$(pwd -P)
```

Alternatively, download `mcp_flightrecorder-0.5.22.tar.gz` from the [PyPI release files](https://pypi.org/project/mcp-flightrecorder/0.5.22/#files), extract it, and set `mfr_source` to the absolute path of its `mcp_flightrecorder-0.5.22` directory. Continue with the same fresh source installation and run the source-shipped facade demo outside that directory:

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

Install source extras with `"$mfr_source_env/bin/python" -m pip install "$mfr_source[openai-chat]"`, substituting `mcp` or `anyllm-ollama` as needed. Keep the legacy MCP adapter in its separate environment. The [complete SDK walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/tool_roundtrip.md) gives matching pinned-package/tagged-source commands for the model → MCP tool → model example.

Follow the complete [Testing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/testing.md) recipe to select one interpreter, export a committed source tree, build in a disposable copy and install the wheel into fresh environments. It provides explicit SDK-free and modern integration selections, including installed CLI, facade and example checks. `pytest tests` includes both selections. Optional native host profiles use their own separate environments and direct requirements; legacy MCP 1.x must not share the modern MCP 2.x environment. A missing optional SDK is not a failed core test or a passed compatibility test. Tests construct recordings in temporary directories and check the examples with ordinary pytest assertions and independent SQL. The source distribution includes the seven declarative JSON inputs needed by the controlled integration scenario.

The [CI workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/.github/workflows/ci.yml) is configured for Linux core jobs on CPython 3.11 and 3.14 and a separate CPython 3.14 integration job. It checks the exported source and built distributions against an explicit public inventory, rebuilds from the sdist, and exercises installed artifacts. The workflow uses read-only repository permissions; it does not publish packages. Check the actual [Actions runs](https://github.com/Steady-Hand-AI/mcp-flightrecorder/actions) for observed results. Configuration alone does not establish a passing run.

The manual [release workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/releasing.md) provides validation and staged publication through TestPyPI, PyPI and GitHub. It promotes one reviewed wheel/sdist pair, verifies downloaded bytes and installations between stages, and uses separate registry approval gates. The [release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/release-notes.md) describe the changes in 0.5.22.

## Documentation and source

| Resource | Purpose |
|---|---|
| [Agent investigation workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/agent-workflow.md) | Resolve a recording, preserve evidence policy and cite actual IDs |
| [Easy integration](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/easy-integration.md) | Concepts, host attachment, replay/retries, diagnostics and operations |
| [Architecture](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/architecture.md) / [HTML map](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/architecture.html) | Current modules, data flow and invariants |
| [Detailed control flows](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/architecture.md#detailed-control-flows) | Component ownership, admission, persistence and failure-path diagrams |
| [Capture API](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/capture.md) | Recorder construction, scopes, observations, lifecycle and shutdown |
| [Inspection API and CLI](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/inspection.md) | Read-only queries, explanation, ancestry, integrity and snapshots |
| [Integrations](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/integrations.md) | SDK facades, evidence receipts and host attachment recipes |
| [Complete tool roundtrip](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/tool_roundtrip.md) | Run local SDK peers and investigate prepared tool text |
| [Stdio recording proxy](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/proxy.md) | Change one server launch, capture MCP-only evidence and inspect retained operations |
| [Testing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/testing.md) | Reproducible local suites and temporary fixtures |
| [Releasing](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/releasing.md) | Candidate validation, registry setup and staged publication |
| [Release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/release-notes.md) | Changes in 0.5.22 |
| [Changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/CHANGELOG.md) | Version history and unreleased changes |
| [Repository](https://github.com/Steady-Hand-AI/mcp-flightrecorder) | Source code |
| [Issues](https://github.com/Steady-Hand-AI/mcp-flightrecorder/issues) | Bug reports and feature requests |

Runtime code is under `src/mcp_flightrecorder/`; `sql/schema.sql` is the canonical schema. `tests/` contains the normal suite and independent fixture assertions, `compatibility_tests/` contains native host checks, and `examples/` contains short usage examples; `tests/producers/` holds the longer controlled test scenarios. Capture, slice and snapshot coverage exercises the installed library and CLI. Generated recordings, reports, dependency wheels, environment directories and historical handoffs are not part of the source package.

## License and ownership

Copyright 2026 Volodymyr Barylyak.

MCP Flight Recorder is licensed under the Apache License, Version 2.0 (`Apache-2.0`). See [LICENSE](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/LICENSE) for the complete terms and [NOTICE](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/NOTICE) for project attribution.
