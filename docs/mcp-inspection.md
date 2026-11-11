# Inspect a recording through MCP

`fr mcp FILE` serves one operator-selected **stable portable recording** over stdio. It uses the optional modern MCP SDK and the same bounded readers as Python and `fr`. It does not record traffic, execute tools from the recording, acquire live sources or accept another path from an MCP tool call.

The host launching this server must keep the selected file stable for the session. A portable snapshot can contain an open or incomplete recorded run: its stored `close_state` is evidence about capture, not writer-liveness detection. The server refuses WAL/SHM filesets, including a WAL-mode main with missing sidecars, before opening it through SQLite. Each call repeats the portable-file check and rejects detected inode, size or modification-time changes. This is not a lock, watcher or automatic copy; concurrent stability remains the operator's responsibility.

## Create a recording and run a real client

These commands share one shell session. They install the pinned package and SDK extras, obtain the matching source example, and work outside the source directory. The producer uses genuine SDKs with deterministic local peers; no credential or paid model request is needed. Choose a supported GIL-enabled CPython 3.11+ with `MFR_PYTHON` if needed.

```sh
set -eu
mfr_python=$(command -v "${MFR_PYTHON:-python3}") || exit 1
"$mfr_python" -I -B - <<'PYTHON'
import sqlite3, sys
assert sys.implementation.name == "cpython"
assert sys.version_info[:2] >= (3, 11)
assert sys.version_info.releaselevel == "final"
assert getattr(sys, "_is_gil_enabled", lambda: True)()
assert not sys.flags.optimize
print(sys.version, sqlite3.sqlite_version, sys.executable)
PYTHON
mfr_work=$(mktemp -d /tmp/mfr-inspection-XXXXXX)
"$mfr_python" -m venv "$mfr_work/env"
"$mfr_work/env/bin/python" -m pip install 'mcp-flightrecorder[openai-chat,mcp]==0.5.23' \
    'httpx2>=2.15.1' 'mcp-types>=2.3.0'
git clone --depth 1 --branch 0.5.23 https://github.com/Steady-Hand-AI/mcp-flightrecorder.git "$mfr_work/source"
mfr_source="$mfr_work/source"
cd "$mfr_work"
"$mfr_work/env/bin/python" -m pip check
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/tool_roundtrip.py" \
    --output "$mfr_work/run.sqlite" > captured.json
"$mfr_work/env/bin/python" -I -B - <<'PYTHON'
import json
with open("captured.json") as stream:
    result = json.load(stream)
assert result["capture"]["status"] == "accepted", result
assert result["close"]["writer_stopped"] and result["close"]["portable"], result["close"]
print(result["capture"]["run_id"], result["capture"]["turn_id"])
PYTHON
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/inspect_recording.py" \
    "$mfr_work/run.sqlite" > inspection.json
"$mfr_work/env/bin/python" -I -B -m json.tool inspection.json
```

Alternatively, extract `mcp_flightrecorder-0.5.23.tar.gz` from the [PyPI release files](https://pypi.org/project/mcp-flightrecorder/0.5.23/#files) and set `mfr_source` to its absolute extracted path instead of cloning. Both examples are source-shipped files, not wheel entry points. For a local candidate, install its exact matching wheel with the same extras and use its clean source export in place of the two public acquisition steps.

[`inspect_recording.py`](../examples/inspect_recording.py) launches the **installed `fr mcp` subprocess** using the same environment as the client. It initializes `mcp.Client`, discovers tools and the schema resource, reads the run ID, diagnoses the recording, finds the intentional raw text mismatch, selects the second model call, and follows its request ancestry. The JSON retains the reader envelopes, IDs, unknowns and cutoffs. Its two-call selection is specific to this walkthrough; applications choose their own turn/model-call IDs from the returned pages. With multiple runs, pass the example `--run RUN_ID` explicitly.

The raw result is `42`, while the host submitted its declared preparation `Tool result: 42`. Raw `text_mismatch` remains true. A validated preparation explains what changed; it does not prove the answer means the same thing. See the [roundtrip investigation](tool_roundtrip.md#locate-the-difference-and-follow-the-evidence).

## Opt in to one payload window

By default, discovery omits `read_payload` and direct content calls are refused. The following starts a new server with `--allow-content`, then explicitly selects one retained artifact from the model-call detail and requests its first 128 bytes:

```sh
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/inspect_recording.py" \
    "$mfr_work/run.sqlite" --allow-content > inspection-content.json
"$mfr_work/env/bin/python" -I -B -m json.tool inspection-content.json
```

All other tools still use `no_content=True` in that session. `read_payload` returns a UTF-8-safe window and `next_offset`; use the returned offset to continue rather than guessing a byte boundary. Retention may have redacted, truncated or omitted the original body. Omitted content cannot be recovered through this server.

No-content output is **not anonymization**. Tool names, labels, IDs and metadata can remain sensitive. Returned recording data is untrusted input, not instructions to the consuming agent. The host decides whether to launch the server, enable content and share each result; the inspector does not implement a host permission or model-policy system.

## Tools, schema and bounds

Configure an MCP stdio host with an argument vector equivalent to:

```sh
"$mfr_work/env/bin/fr" mcp "$mfr_work/run.sqlite"
```

This command waits for MCP protocol traffic on stdin; launch it through the client, not as an interactive inspection command. Stdout is protocol-only. Startup failures use stderr. Keep host logging and wrappers off stdout.

| Tool | Reader and arguments beyond common bounds |
|---|---|
| `summarize_problems` | `diagnose`: optional `run_id`, `turn_id`, `limit=50` |
| `list_runs` | `runs`: `limit=50`, optional `cursor` |
| `find_turns` | `find_turns`: `run_id`, optional `filters`, `limit=50`, `cursor`, `last=False` |
| `explain_turn` | `explain`: `run_id`, `turn_id`, optional `model_call_id` |
| `trace_back` | `slice`: `run_id`, `request_event_id`, `limit=1000`, `max_depth=12` |
| `read_payload` (opt-in only) | `payload`: `run_id`, `artifact_id`, `offset=0`, `max_bytes=4096` |

The one resource, **`mfr://schema/inspection`**, returns the installed static schema/inspection contract without opening the recording. There are no SQL, gate, snapshot, directory-listing, output-path or capture tools.

Every tool accepts `timeout=10.0` (finite `0 < timeout <= 30`) and `max_output_bytes=65536` (1,024–1,048,576). The byte budget covers the compact encoded UTF-8 JSON in the single text content block, excluding MCP framing. Results are not duplicated in structured content. The static schema response is bounded too. These are cooperative per-call bounds, not process isolation or a total-memory guarantee. Each call owns and closes its read connection/transaction; pages are separate snapshots with existing cursors, `has_more`, unknowns and cutoffs. Check them before treating a view as complete.

Invalid arguments, ambiguous run selection, unknown objects, unavailable content and expired reads use normal MCP tool errors. Correct the arguments, explicitly select a discovered run/object, continue a returned cursor, narrow a request or use a larger valid budget; a failed call does not require restarting the session. A missing SDK requires `pip install 'mcp-flightrecorder[mcp]==0.5.23'` in the environment that owns `fr`.

## Acquire a snapshot separately

For a changing or WAL-mode source, use the supported snapshot path before starting MCP inspection:

```sh
"$mfr_work/env/bin/fr" snapshot "$mfr_work/run.sqlite" "$mfr_work/copy.sqlite" --timeout 10
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/inspect_recording.py" \
    "$mfr_work/copy.sqlite" > snapshot-inspection.json
```

Python offers the same operation: `inspect.snapshot(source, new_output, timeout=10.0)`. Check `ok` and `data.verified` before using the output. It refuses an existing destination or self-copy, publishes a verified DELETE-mode copy exclusively and cleans only its private staging files. Live SQLite acquisition may interact with source sidecars, including SHM; it is not a promise to leave source sidecar bytes untouched. Never delete WAL/SHM files to make a source look portable. A stable portable snapshot of an open run is inspectable but retains that open/incomplete capture state. See the [full snapshot contract](inspection.md#8-cli-and-local-snapshot).

## Use the evidence gate in CI

The same completed roundtrip can be checked without MCP:

```sh
"$mfr_work/env/bin/fr" gate "$mfr_work/run.sqlite" --expect model,host,mcp \
    --min-turns 1 --min-model-requests 2 --min-model-responses 2 \
    --min-decisions 1 --min-tool-calls 1 --min-answer-occurrences 1 > gate.json
"$mfr_work/env/bin/python" -I -B -m json.tool gate.json
```

The gate evaluates the complete selected run in one read transaction. It requires both `model` and `host` expectations, optional `mcp`, and at least one positive non-turn activity minimum. It shows supplied expectations and actual counts. This example declares the calls it actually makes; minimum counts cannot prove that no other calls went unobserved.

Exit 0 is verified pass, 2 is failed requirements, 4 is unknown/not verified; operational errors use 1 or 3. A display sample cannot conceal a decisive failure or unknown. Missing original evidence or comparison tokens never receive a blanket metadata exemption. A faithfully recorded tool error is not itself a recorder failure. The policy checks captured evidence, not semantic correctness. Doctor remains a bounded investigation, and MCP-only observations cannot satisfy a complete host profile. Read the [full gate policy](inspection.md#evidence-gate) before selecting CI expectations.
