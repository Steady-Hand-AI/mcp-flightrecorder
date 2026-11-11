# Run an OpenAI and MCP host with recording

[`examples/openai_mcp.py`](../examples/openai_mcp.py) runs a complete host using the facade. Its sibling [`openai_mcp_peers.py`](../examples/openai_mcp_peers.py) supplies deterministic peers and reuses `tool_roundtrip.py`. Keep the matching source examples directory intact.

The example runs genuine `AsyncOpenAI` and MCP clients through `AgentRecorder`. The host selects a model tool call, executes it, submits the answer in a second model request, closes recording, and checks evidence alongside an independent application expectation. A separate peer module supplies deterministic model responses and MCP tool results. The recipe uses no credentials or external provider service.

## Install the package and matching source

Start in an empty working directory. Install the pinned package and four SDK dependencies, then download the matching tagged source archive. The package comes from the installed wheel. The archive supplies the source-shipped examples.

Use a supported GIL-enabled CPython 3.11+ with normal assertions. Set `MFR_PYTHON` if `python3` is not your intended interpreter. Keep the following commands in one shell session.

```sh
set -eu
mfr_python=$(command -v "${MFR_PYTHON:-python3}") || exit 1
mfr_work=$(mktemp -d /tmp/mfr-openai-mcp-XXXXXX)
"$mfr_python" -m venv "$mfr_work/env"
"$mfr_work/env/bin/python" -m pip install mcp-flightrecorder==0.5.24 \
    'openai==3.49.0' 'mcp==2.3.0' 'mcp-types==2.3.0' 'httpx2==2.16.0'
curl -fsSL https://github.com/Steady-Hand-AI/mcp-flightrecorder/archive/refs/tags/0.5.24.tar.gz -o "$mfr_work/source.tar.gz"
tar -xzf "$mfr_work/source.tar.gz" -C "$mfr_work"
mfr_source=$(cd "$mfr_work/mcp-flightrecorder-0.5.24" && pwd -P)
"$mfr_work/env/bin/python" -m pip check
cd "$mfr_work"
"$mfr_work/env/bin/python" -I -B - <<'PYTHON'
from importlib.metadata import version
import mcp_flightrecorder
print(mcp_flightrecorder.__version__, mcp_flightrecorder.__file__)
for name in ("openai", "mcp", "mcp-types", "httpx2"):
    print(name, version(name))
PYTHON
```

The four SDK pins match the existing [integration requirements](../requirements/integration.txt). Retain the printed installed versions and package origin with your results.

Alternatively, extract `mcp_flightrecorder-0.5.24.tar.gz` from the [PyPI release files](https://pypi.org/project/mcp-flightrecorder/0.5.24/#files) and set `mfr_source` to the absolute path of its `mcp_flightrecorder-0.5.24` directory instead of downloading the tagged archive. Keep the whole `examples/` directory intact and continue with the same commands outside that directory.

## Run the complete host

Run the faithful single-text case with a fresh recording path.

```sh
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/openai_mcp.py" \
    --case single --output "$mfr_work/single.sqlite" > "$mfr_work/single.txt"
cat "$mfr_work/single.txt"
```

The command prints the recording path and separate `evidence` and `application` outcomes. The application check reads actual SDK request bytes observed by the peer. It verifies the selected tool call and the exact submitted tool message against independently authored expectations.

The application code and peer setup have separate files. Read `run_host` in `openai_mcp.py` when adapting the host flow. The peer module uses `httpx2.MockTransport`, an in-process MCP server and shared deterministic services from `tool_roundtrip.py`. Keep the examples directory intact.

1. The host owns the native model and MCP clients and their cleanup. It attaches capture through the existing facade and performs MCP discovery before the selected tool call. The MCP client closes after that call. The OpenAI client remains open for answer formatting and the second request.
2. The first model call returns a native response. The host selects a choice and tool occurrence, parses its arguments, and gives those same values to the selected attempt and native MCP call.
3. The selected attempt binds the tool answer submitted in the second model request. The host retains responsibility for message formatting and any intended transformation.
4. Recording closes before evidence inspection. `assert_evidence` and the independent application check evaluate separate outcomes.

Use a new filename for another run. Existing recordings and sidecars are preserved. See [capture lifecycle](capture.md#5-retention-and-shutdown) and [shutdown behavior](easy-integration.md#start-and-close-in-the-host) for ownership and recovery rules.

## Preserve the intended answer

The example-local `answer_openai_tool(attempt, tool_call_id, result, *, transform=None)` helper handles complete text-only results. One text block becomes a string. Multiple text blocks remain ordered typed text parts. Tool-error text is forwarded unchanged. An empty content list, a non-text block, structured content or an unsupported result shape raises `UnsupportedToolResult`, a `ValueError` subclass. The helper stays in the example.

An explicit transform receives the ordered `list[str]` and must return a string. Its exceptions propagate, and a non-string return raises `TypeError`. The example declares preparation only for that intended transform. Joining text blocks without declaring preparation remains a mismatch. A declaration explains captured preparation, while the application assertion still checks the intended submitted value.

Keep the default evidence policy and independent expected answer when fixing host behavior. Missing capture, ambiguous attribution and unavailable original text retain their existing uncertainty. A passing evidence result establishes the stated recording requirements. It does not establish the quality of a model answer or the absence of unobserved calls.

## Compare faithful and deliberately broken cases

The evidence assertion uses the application's stated minimum activity: one turn, two model requests and responses, one selected decision, one tool call and one submitted answer. Tool discovery is a separate operation. The application check independently verifies the actual native call and submitted content.

| Case | Submitted content | Evidence | Application | CLI exit |
|---|---|---|---|---|
| `single` | String `42` | PASS | PASS | 0 |
| `multiple` | Ordered text parts `4`, `2` | PASS | PASS | 0 |
| `tool-error` | String `Tool unavailable` | PASS | PASS | 0 |
| `prepared` | Declared string `Tool result: 4 \| 2` | PASS | PASS | 0 |
| `joined` | Undeclared string `42` instead of two parts | FAILED | FAILED | 2 |
| `unsupported` | Mixed text/image result refused before follow-up | FAILED | PASS: no second request | 2 |

Run the remaining faithful cases with distinct files.

```sh
for mfr_case in multiple tool-error prepared; do
    "$mfr_work/env/bin/python" -I -B "$mfr_source/examples/openai_mcp.py" \
        --case "$mfr_case" --output "$mfr_work/$mfr_case.sqlite" > "$mfr_work/$mfr_case.txt"
    cat "$mfr_work/$mfr_case.txt"
done
```

The next two controls intentionally exit 2. The shell checks that specific exit, while the printed evidence and application outcomes remain separate. Read errors or configuration errors remain distinct from these expected failures.

```sh
for mfr_case in joined unsupported; do
    if "$mfr_work/env/bin/python" -I -B "$mfr_source/examples/openai_mcp.py" \
        --case "$mfr_case" --output "$mfr_work/$mfr_case.sqlite" > "$mfr_work/$mfr_case.txt"; then
        echo "Expected $mfr_case to exit 2" >&2
        exit 1
    else
        mfr_exit=$?
        test "$mfr_exit" -eq 2
    fi
    cat "$mfr_work/$mfr_case.txt"
done
```

The joined case deliberately changes the answer after binding it. Fix the host's formatting while preserving its expected ordered parts. The prepared case instead declares the intended transformation before submission and checks that transformed value independently. The unsupported case leaves the required second call absent, so it cannot satisfy the complete-flow evidence requirements even though refusal is the correct application behavior.

## Inspect the recording

The existing readers expose the same recording after the host has closed it.

```sh
"$mfr_work/env/bin/fr" doctor "$mfr_work/single.sqlite"
"$mfr_work/env/bin/fr" turns "$mfr_work/single.sqlite" --last --text
"$mfr_work/env/bin/fr" gate "$mfr_work/single.sqlite" \
    --expect model,host,mcp --min-turns 1 \
    --min-model-requests 2 --min-model-responses 2 --min-decisions 1 \
    --min-tool-calls 1 --min-answer-occurrences 1 --text
```

Follow the [agent investigation workflow](agent-workflow.md#follow-the-evidence-and-cite-ids) to select actual run, turn and model-call IDs for detailed explanation. Keep [raw comparison facts and preparation limits](inspection.md#diagnostic-report-and-preparation) visible. The [regression guide](agent-workflow.md#keep-a-regression-check) describes assertion errors and read errors.

This recipe exercises installed SDK serialization against owned deterministic peers. It does not contact a live provider. For your application, retain native client ownership, tool selection, retries, concurrency and cancellation behavior described in [Easy integration](easy-integration.md) and the [adapter contracts](integrations.md).
