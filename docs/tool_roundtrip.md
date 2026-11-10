# Follow a model → tool → model roundtrip

[`examples/tool_roundtrip.py`](../examples/tool_roundtrip.py) runs two nonstreaming OpenAI SDK calls, one MCP `tools/list` discovery and one selected MCP `tools/call`. An `httpx2.MockTransport` supplies fixed model responses, and an owned in-process MCP server returns `TextContent(text="42")`. No external service or credential is needed. Discovery supplies the catalog required by the MCP client for native result validation and is counted separately from tool execution.

The host selects `answer` with `{}` from the first native model response, executes it, constructs the string `Tool result: 42` once, and submits that same value in the second request's tool message. The second native response says `The answer is 42.`. Capture observes this flow through the existing adapters and explicit host annotations.

## Install the matching package and source

The example is shipped at `examples/tool_roundtrip.py` in the [0.5.21 source tag](https://github.com/Steady-Hand-AI/mcp-flightrecorder/tree/0.5.21) and source distribution. It is not a wheel entry point. Install the matching package into a fresh environment, then run the source example from outside its source directory. No source directory needs to be added to the package import path.

Select a supported GIL-enabled CPython 3.11+ using `MFR_PYTHON` if `python3` is not your intended interpreter. The commands below share one shell session.

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
mfr_work=$(mktemp -d /tmp/mfr-roundtrip-XXXXXX)
"$mfr_python" -m venv "$mfr_work/env"
```

Choose one of the following installation routes, then continue to **Run the example**.

### Published package and tagged source

Install the pinned package and its SDK extras from PyPI, then obtain the example from the matching source tag.

```sh
"$mfr_work/env/bin/python" -m pip install 'mcp-flightrecorder[openai-chat,mcp]==0.5.21' \
    'httpx2>=2.15.1' 'mcp-types>=2.3.0'
git clone --depth 1 --branch 0.5.21 https://github.com/Steady-Hand-AI/mcp-flightrecorder.git "$mfr_work/source"
mfr_source="$mfr_work/source"
```

Alternatively, extract `mcp_flightrecorder-0.5.21.tar.gz` from the [PyPI release files](https://pypi.org/project/mcp-flightrecorder/0.5.21/#files) and set `mfr_source` to the absolute path of its `mcp_flightrecorder-0.5.21` directory. The example has the same `examples/tool_roundtrip.py` location in that source distribution.

### Contributor or candidate wheel

For local candidate validation, use the [contributor build recipe](testing.md#1-qualify-the-sdk-free-core) to export committed source and build its matching wheel. Replace the two absolute paths below with those outputs. Use this route instead of the published-package installation above.

```sh
mfr_source=/absolute/path/to/matching-source
mfr_wheel=/absolute/path/to/mcp_flightrecorder-0.5.21-py3-none-any.whl
test -f "$mfr_wheel"
"$mfr_work/env/bin/python" -m pip install "${mfr_wheel}[openai-chat,mcp]" \
    'httpx2>=2.15.1' 'mcp-types>=2.3.0'
```

## Run the example

```sh
test -f "$mfr_source/examples/tool_roundtrip.py"
"$mfr_work/env/bin/python" -m pip check
cd "$mfr_work"
"$mfr_work/env/bin/python" -I -B - <<'PYTHON'
from pathlib import Path
import mcp_flightrecorder, sys
assert Path(mcp_flightrecorder.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
print(mcp_flightrecorder.__version__, mcp_flightrecorder.__file__)
PYTHON
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/tool_roundtrip.py" \
    --output "$mfr_work/run.sqlite" > captured.json
cat captured.json
```

Keep the output path fresh. An existing recording or SQLite sidecar is refused. For another run, use another new directory or filename.

The JSON includes `host`, the independently observed `backend` requests/responses and tool calls/results, `capture`, and `close`. Read `capture.run_id`, `capture.turn_id` and the two `capture.model_call_ids` from this invocation. `capture.status="accepted"` describes admission. Check `close.writer_stopped`, `close.portable` and `close.run_close_state` separately for the shutdown result.

Compare the same host flow with capture disabled.

```sh
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/tool_roundtrip.py" \
    --no-capture > uncaptured.json
"$mfr_work/env/bin/python" -I -B - <<'PYTHON'
import json
with open("captured.json") as stream:
    captured = json.load(stream)
with open("uncaptured.json") as stream:
    uncaptured = json.load(stream)
assert captured["host"] == uncaptured["host"]
assert captured["backend"] == uncaptured["backend"]
assert uncaptured["capture"]["status"] == "disabled"
print("Host inputs, native responses and selected tool work agree.")
PYTHON
```

## Locate the difference and follow the evidence

The following commands use the actual IDs printed by the example. They create readable JSON files in the external working directory. They require accepted capture with two available model-call IDs.

```sh
"$mfr_work/env/bin/python" -I -B - <<'PYTHON' > ids.txt
import json
with open("captured.json") as stream:
    capture = json.load(stream)["capture"]
assert capture["status"] == "accepted", capture
assert len(capture["model_call_ids"]) == 2 and all(capture["model_call_ids"])
print(capture["run_id"], capture["turn_id"], *capture["model_call_ids"])
PYTHON
read -r mfr_run mfr_turn mfr_first mfr_second < ids.txt
"$mfr_work/env/bin/fr" turns "$mfr_work/run.sqlite" --run "$mfr_run" \
    --text-mismatch true > turns.json
"$mfr_work/env/bin/python" -I -B -m json.tool turns.json
"$mfr_work/env/bin/fr" explain "$mfr_work/run.sqlite" --run "$mfr_run" \
    --turn "$mfr_turn" > overview.json
"$mfr_work/env/bin/python" -I -B -m json.tool overview.json
"$mfr_work/env/bin/fr" explain "$mfr_work/run.sqlite" --run "$mfr_run" \
    --turn "$mfr_turn" --model-call "$mfr_first" > first-call.json
"$mfr_work/env/bin/python" -I -B -m json.tool first-call.json
"$mfr_work/env/bin/fr" explain "$mfr_work/run.sqlite" --run "$mfr_run" \
    --turn "$mfr_turn" --model-call "$mfr_second" > second-call.json
"$mfr_work/env/bin/python" -I -B -m json.tool second-call.json
```

`turns.json` locates the turn with `flags.text_mismatch=true`. Filters combine with AND. Search tool errors or unattributed operations in separate `fr turns` invocations when you want those populations independently.

`overview.json` provides turn-wide counts and `data.calls` with both model-call IDs. The healthy example has ten observed events, two model requests/responses, one decision and two operations. One operation is `tools/list` discovery, shown in `unassigned_operations` because it precedes model selection. The other is the selected `tools/call`. Its `data.detail` is null. Select a model call to inspect its detail.

| File | Evidence to follow |
|---|---|
| `first-call.json` | The first request/response, model decision, explicitly attributed MCP operation, and `produced_answers`. The result-to-preparation `transformed_from` link connects the selected terminal result to the host's prepared material. |
| `second-call.json` | The submitted request's `members` and `input_answers`, plus the `inserted_into` placement at `/messages/2/content`. The answer comparison retains `text_equal=false` for `42` versus `Tool result: 42`. |

The preparation records its source at the MCP result's `/content/0/text`. The insertion identifies the same prepared string in message index 2, with the answer binding relative to `/content`. These explicit identities establish the chain. A provider tool-call ID or matching text alone does not.

`explain` includes links touching the selected call; it does not recursively include every linked endpoint or body. Inspect both calls. Check each envelope's errors, diagnostics and cutoff indicators, discovery's `has_more`/`next_cursor`, and detail `totals` before treating a displayed page as complete. Use the [inspection contract](inspection.md#4-discovery-and-explanation) for limits and continuation behavior.

The literal mismatch is intentional preparation. It can coexist with passing integrity checks. Neither a check pass nor the example's `model_complete=True` declaration proves semantic correctness, answer quality or unobserved real-world coverage. Missing or rejected capture remains unavailable. The example binds only the selected invocation's admitted decision and terminal result; an unavailable selected result produces an unresolved answer annotation rather than borrowing another result or asserting that no answer was submitted.

## Ownership and limits

The host owns native clients, tool selection, message history and prepared text. Capture receipts never decide whether the selected tool executes. The example uses fresh `ModelEvidence` for each model invocation and `OperationEvidence` for the tool invocation, propagates the selected decision with `decision=`, and records `host.tool_result`, sources, annotations and insertions explicitly.

The entry point establishes its async worker with `asyncio.run(record_roundtrip(output))`. Inside that worker, `record_roundtrip` constructs the `Recorder`, awaits `run_host`, and awaits `recorder.aclose()` in `finally`. When adapting this to multiple processes, establish each worker before constructing its recorder and give each worker its own fresh recording path. An inherited recorder is unsupported; this pattern does not make multithreaded fork safe. Native exceptions and cancellation remain host-owned. The example covers one nonstreaming turn, with no replay, retries or concurrent conversations.

See [construction and capture limits](capture.md#1-construction-and-values), [receipts and admission](capture.md#2-receipts-scopes-and-identity), [transaction durability](architecture.md#4-transactions-and-shutdown), [health and shutdown](capture.md#5-retention-and-shutdown), and [reader limits](inspection.md#1-public-python-surface-and-limits) before adapting the example to a larger host.
