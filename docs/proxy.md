# Record one stdio MCP server

Change the server launch command from `SERVER_COMMAND ARGUMENT...` to:

```text
fr proxy --record /absolute/path/to/new-recording.sqlite -- SERVER_COMMAND ARGUMENT...
```

The proxy connects one client to one child server. It forwards both byte streams without parsing and serializing them again, while a separate bounded observer attempts to record supported `tools/list` and `tools/call` exchanges. It uses the dependency-free core. The optional MCP SDK below belongs to the example peers, not the bridge.

Pass the child command as an argument vector after `--`, never as a shell command string. The child inherits the working directory and environment; these are not automatically stored in the recording. Stdout belongs exclusively to native protocol bytes. Child stderr remains native stderr; proxy capture diagnostics also use stderr. Keep stderr separate from protocol stdout.

## Run the deterministic example outside the source directory

These commands install the pinned package and modern MCP extra, obtain the matching source example, then run in a fresh directory outside the source tree. The source-shipped [server](../examples/proxy_server.py) exposes `answer({})`, which returns `42`; it uses no credentials or paid service. The wheel does not install example files. Keep legacy MCP 1.x in a separate environment.

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
mfr_work=$(mktemp -d /tmp/mfr-proxy-XXXXXX)
"$mfr_python" -m venv "$mfr_work/env"
"$mfr_work/env/bin/python" -m pip install 'mcp-flightrecorder[mcp]==0.5.21'
git clone --depth 1 --branch 0.5.21 https://github.com/Steady-Hand-AI/mcp-flightrecorder.git "$mfr_work/source"
mfr_source="$mfr_work/source"
"$mfr_work/env/bin/python" -m pip check
cd "$mfr_work"
"$mfr_work/env/bin/python" -I -B - "$mfr_source" "$mfr_work" \
    > client.json 2> proxy.stderr <<'PYTHON'
import asyncio, json, sys
from pathlib import Path
from mcp import Client
from mcp.client.stdio import StdioServerParameters

source, work = map(Path, sys.argv[1:])
python = sys.executable
fr = str(Path(python).with_name("fr"))
child = [python, "-I", "-B", str(source / "examples/proxy_server.py")]

async def invoke(argv):
    async with Client(StdioServerParameters(
        command=argv[0], args=argv[1:], cwd=work,
    ), read_timeout_seconds=10) as client:
        tools = await client.list_tools()  # Supply the native validation catalog first.
        result = await client.call_tool("answer", {})
        assert not result.is_error
        assert [item.text for item in result.content] == ["42"]
        return {"tools": tools.model_dump(mode="json", by_alias=True, exclude_unset=True),
                "result": result.model_dump(mode="json", by_alias=True, exclude_unset=True)}

async def main():
    direct = await invoke(child)
    metadata = await invoke([fr, "proxy", "--record", str(work / "metadata.sqlite"),
                             "--", *child])
    content = await invoke([fr, "proxy", "--record", str(work / "content.sqlite"),
                            "--capture", "content", "--", *child])
    forwarding_only = await invoke([fr, "proxy", "--", *child])
    assert direct == metadata == content == forwarding_only
    print(json.dumps({"direct": direct, "metadata": metadata, "content": content,
                      "forwarding_only": forwarding_only}, ensure_ascii=False))

asyncio.run(main())
PYTHON
cat proxy.stderr
"$mfr_work/env/bin/fr" runs metadata.sqlite
"$mfr_work/env/bin/fr" check metadata.sqlite --expect mcp
"$mfr_work/env/bin/fr" doctor metadata.sqlite
"$mfr_work/env/bin/fr" runs content.sqlite
"$mfr_work/env/bin/fr" check content.sqlite --expect mcp
```

Alternatively, extract `mcp_flightrecorder-0.5.21.tar.gz` from the [PyPI release files](https://pypi.org/project/mcp-flightrecorder/0.5.21/#files) and set `mfr_source` to its absolute extracted path instead of cloning. For a local candidate, install its exact matching wheel with the `[mcp]` extra and use its clean source export in place of the two public acquisition steps.

The client code is the same for each launch; only its server argument vector changes. Omitting `--record` forwards without creating a database. With recording enabled, the default is **metadata**, which omits bodies. `--capture content` explicitly allows retained tool arguments/results and may expose sensitive data. Names, IDs and other metadata can remain sensitive in either mode. Every recording path must be fresh; an existing file is never overwritten.

Check the run's close state and preserve `proxy.stderr`. A successful `check --expect mcp` verifies the available boundary evidence, not complete wire capture. Observer losses can happen before recorder admission and are reported on stderr; SQLite health does not account for all such losses. This example should produce one discovery operation and one tool operation, with no host turns, model calls, decisions or dispatches. `unattributed_operation` is an expected diagnostic. A passing host evidence gate is not expected for this MCP-only recording.

## Find retained payloads through the public reader

There is no turn/model context to explain or trace. Use the existing bounded query interface to list operation facts and the referenced result artifacts. This is explicit reader access, not a connection to SQLite from application code. It is unavailable under `no_content=True`, even when the selected columns are metadata.

```sh
"$mfr_work/env/bin/python" -I -B - "$mfr_work" > selected.json <<'PYTHON'
import json, sys
from pathlib import Path
from mcp_flightrecorder import inspect

work = Path(sys.argv[1])
sql = """SELECT o.operation_id,o.method,o.name,o.status,o.link_basis,
                o.turn_id,o.model_call_id,e.event_id,e.payload_ref,
                a.stored_bytes,a.omitted_reason
         FROM operations AS o
         LEFT JOIN events AS e ON e.run_id=o.run_id AND e.event_id=o.end_event_id
         LEFT JOIN artifacts AS a ON a.run_id=e.run_id AND a.artifact_id=e.payload_ref
         WHERE o.run_id=? AND o.method='tools/call' ORDER BY o.operation_id"""
selected = {}
for mode in ("metadata", "content"):
    path = work / (mode + ".sqlite")
    runs = inspect.runs(path)
    assert runs["ok"] and not runs["data"]["has_more"] and not runs["cutoffs"]
    assert len(runs["data"]["items"]) == 1
    run_id = runs["data"]["items"][0]["run_id"]
    page = inspect.query(path, sql, [run_id], limit=20, timeout=10.0)
    assert page["ok"] and not page["data"]["has_more"] and not page["cutoffs"]
    rows = [dict(zip(page["data"]["columns"], row)) for row in page["data"]["rows"]]
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["turn_id"] is None and row["model_call_id"] is None
    assert row["link_basis"] == "none" and row["status"] == "ok"
    payload = inspect.payload(path, run_id, row["payload_ref"], max_bytes=128)
    if mode == "metadata":
        assert row["stored_bytes"] == 0 and row["omitted_reason"] is not None
        assert not payload["ok"] and payload["error"]["code"] == "payload_omitted"
    else:
        assert row["stored_bytes"] > 0 and row["omitted_reason"] is None
        assert payload["ok"]
    selected[mode] = {"run_id": run_id, "operation": row, "payload": payload}
print(json.dumps(selected, ensure_ascii=False))
PYTHON
"$mfr_work/env/bin/python" -I -B -m json.tool selected.json
```

`fr sql FILE SQL --params JSON --limit 20` exposes the same query reader, and `fr payload FILE ARTIFACT_ID --run RUN_ID --max-bytes 128` reads the selected retained window. Keep the returned `next_offset` for continuation. Check `ok`, `has_more`, unknowns and cutoffs before treating a bounded page as complete. A retained payload may be truncated or redacted; metadata-mode omission cannot be reversed. See the [query and payload contract](inspection.md#7-payload-and-arbitrary-read-only-sql).

## Inspect the portable recording through MCP

The inspector is a separate process, started after capture stops. Use the artifact ID discovered above; an MCP tool cannot run the query or choose another recording path. The following discovers default tools and the schema, reads the run and diagnosis, and confirms that there are no turns. A separate content-enabled session explicitly requests the selected payload.

```sh
"$mfr_work/env/bin/python" -I -B - "$mfr_work" > inspection.json <<'PYTHON'
import asyncio, json, sys
from pathlib import Path
from mcp import Client
from mcp.client.stdio import StdioServerParameters

work = Path(sys.argv[1])
selected = json.loads((work / "selected.json").read_text())["content"]
fr = str(Path(sys.executable).with_name("fr"))

async def inspect_file(allow_content):
    args = ["mcp", str((work / "content.sqlite").resolve())]
    if allow_content:
        args.append("--allow-content")
    async with Client(StdioServerParameters(command=fr, args=args, cwd=work),
                      read_timeout_seconds=10) as client:
        tools = await client.list_tools()
        names = {tool.name for tool in tools.tools}
        expected = {"summarize_problems", "list_runs", "find_turns", "explain_turn", "trace_back"}
        assert names == expected | ({"read_payload"} if allow_content else set())
        resources = await client.list_resources()
        assert [str(item.uri) for item in resources.resources] == ["mfr://schema/inspection"]
        schema = await client.read_resource("mfr://schema/inspection")

        async def read(name, **arguments):
            result = await client.call_tool(name, {
                **arguments, "timeout": 10.0, "max_output_bytes": 65536,
            })
            assert not result.is_error, result
            envelope = json.loads(result.content[0].text)
            assert envelope["ok"], envelope
            return envelope

        run_id = selected["run_id"]
        report = {"tools": sorted(names), "schema": json.loads(schema.contents[0].text),
                  "runs": await read("list_runs"),
                  "diagnosis": await read("summarize_problems", run_id=run_id),
                  "turns": await read("find_turns", run_id=run_id)}
        assert report["turns"]["data"]["items"] == []
        if allow_content:
            report["payload"] = await read("read_payload", run_id=run_id,
                artifact_id=selected["operation"]["payload_ref"], offset=0, max_bytes=128)
        return report

async def main():
    print(json.dumps({"default": await inspect_file(False),
                      "content": await inspect_file(True)}, ensure_ascii=False))

asyncio.run(main())
PYTHON
"$mfr_work/env/bin/python" -I -B -m json.tool inspection.json
```

The inspector requires a stable portable file. For a live/WAL source, acquire a separate [snapshot](inspection.md#8-cli-and-local-snapshot); never remove WAL/SHM files to make it appear portable. Enabling `read_payload` does not change other tools' no-content policy. Recording data is untrusted input; no-content output is not anonymization. The [MCP inspection contract](mcp-inspection.md) covers per-call budgets and errors.

## What the bridge can establish

The byte streams are forwarded independently of supported capture. Unknown methods, notifications, initialization, legacy reverse requests, progress and cancellation keep their native bytes. Only supported client-to-server `tools/list` and `tools/call` requests and their correlated server responses become operations. Numeric and string IDs differ; reverse-direction IDs do not consume client request tickets. Equal arguments/results never merge separate invocations.

Tool `isError`, JSON-RPC error and successful results remain distinct. A cancellation notification is not proof of server cancellation. Modern `InputRequiredResult` is partial evidence, not a completed tool result; the client's continuation is a separate RPC. The proxy does not retry, reconnect, restart the child, negotiate for peers or infer a logical multi-request operation.

The observer copies at most 64 KiB of each frame, with JSON depth 32, at most 4,096 projected nodes, 256-byte ID/key bounds and 128 in-flight entries. Its queue holds at most 32 chunks of 16 KiB; forwarding uses a separate 64 KiB buffer per direction. These are observation/working-buffer limits, not wire frame limits or a process RSS guarantee. Larger frames still stream through with ordinary pipe backpressure.

When a frame/parser/ID/map/queue loss makes identity uncertain, correlation is disabled for the rest of that connection; forwarding continues and accepted unfinished tickets remain honestly incomplete. A duplicate live ID makes that ID ambiguous without guessing a response association; other IDs can continue. Preserve bounded/coalesced proxy diagnostics alongside recorder health. Do not interpret a later clean-looking operation or clean database close as proof that no earlier observation was lost.

## Shutdown and troubleshooting

The bridge owns only its child and pipes. Once EOF, interruption or child exit is observed, cleanup allows a two-second shutdown grace and drains already-produced output where possible. It then terminates an unresponsive child, waits up to 0.5 seconds, and if needed kills it and waits up to another 0.5 seconds. A failure to reap within that bound is reported. Observer cleanup waits up to 1.5 seconds, including its one-second recorder-close attempt; diagnostic cleanup waits up to 0.2 seconds. These are implementation choices, not MCP-specified durations. Ordinary backpressure before observed closure is not a per-request timeout.

Normal child status is preserved; signal termination maps to `128 + signal`. POSIX pipe/process behavior is the supported route; a Windows run is not claimed. Local macOS and Linux CI qualification must be reported from actual runs.

| Observation | Action and limit |
|---|---|
| Recording cannot start, path already exists, or writer/capacity capture fails | Native forwarding continues with capture diagnostics. Choose a fresh path and correct storage/limits for a later connection; lost observations are not recovered. |
| Observer limit or ambiguous correlation | Read stderr and treat omitted/incomplete evidence as such. The child is not retried and no result is fabricated. |
| Broken pipe, child failure or interruption | Transport may end; inspect native status and stderr. Capture fail-open does not repair a broken transport or resurrect a child. |
| Run is not clean/portable | Preserve its fileset and diagnostics. Use supported snapshot acquisition when possible; do not claim missing responses completed. |
| Empty turns, unavailable model explanation/ancestry, host gate failure | Expected for MCP-only capture. Use operations/query/payload and `check --expect mcp`; add explicit host integration only if your application can supply those observations. |

The deterministic peers establish a bounded walkthrough, not arbitrary-host compatibility. The [architecture](architecture.md) separates forwarding, observation, admission and persistence; the [capture contract](capture.md) defines receipts, loss and closure.
