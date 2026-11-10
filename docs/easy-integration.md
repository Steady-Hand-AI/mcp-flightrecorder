# Add recording to an existing host

`mcp_flightrecorder.easy.AgentRecorder` keeps capture bookkeeping inside the library. Your host still owns model requests, parsing, tool selection, retries, concurrency, client lifetimes and stream consumption. Use the [0.5.19 installation and complete demo](../README.md#install-and-record) to start from an empty directory.

## The concepts

| Handle | What the host states |
|---|---|
| `AgentRecorder` | Where recording is enabled, retention/limits, and when recording starts and closes. Construction is inert; `path=None` disables recording. |
| `conversation(name)` | One caller-owned history across successive turns. Separate conversations may run concurrently; overlapping turns on one handle are diagnosed and left unattributed. |
| `conversation.turn(prompt)` | The lifetime of one application turn. |
| `conversation.model_call(payload)` | The ordinary JSON your host submits; `call.done(response)` records the exposed response. |
| `call.choice(name, arguments)` | The decision your host selected from its native response. The facade does not produce an executable tool list. |
| `choice.run()` / `attempt` | One actual invocation, including an invocation whose capture is rejected. |
| `attempt.answer(message)` | This selected invocation produced this submitted message. It returns the exact message without changing it. |

No ordinary application code constructs receipts, IDs, JSON pointers, evidence objects or annotation dictionaries. Existing [low-level capture](capture.md) and [SDK adapters](integrations.md) remain available for advanced integrations.

## Start and close in the host

```python
from mcp_flightrecorder.easy import AgentRecorder

rec = AgentRecorder(
    "new-recording.sqlite",       # use None for disabled recording
    capture="metadata",          # or "content" when retention is appropriate
    all_model_calls_recorded=True,  # only if your host makes this promise
)
rec.start()                       # construct/start in the worker that uses it
conversation = rec.conversation("chat-123")
try:
    # Run the application's turn loop here, using the recipes below.
    pass
finally:
    conversation.close()         # release its retained history bindings
    closed = rec.close()          # never closes your native clients
```

A context manager (`with AgentRecorder(...) as rec`) also starts/closes recording. In an async host, use `await rec.aclose()` at shutdown. When recording is disabled, close and health return `None`. With recording enabled, inspect `closed.writer_stopped`, `closed.portable`, `closed.run_close_state` and `closed.reason` before treating the file as portable. Admission is not durability; portable storage is not proof of complete evidence. Keep an existing host exception or cancellation authoritative when reporting cleanup problems.

If the writer has stopped but the file is not portable, release any external reader or other finalization blocker, then retry with `rec.finalize(timeout=5.0)` and inspect the returned status. This retries finalization after close; it is not automatic recovery from storage failure. Disabled recording returns `None`.

The parent directory must exist and the recording path must be fresh. Startup/configuration errors can raise before application work. Do not reuse an inherited recorder after a process fork. Create workers before starting their recorders, each with its own fresh path. The creator-process guard rejects inherited use before recorder-owned locks or storage access; it does not make threaded fork safe.

## Ordinary JSON model calls and MCP

The following is an integration fragment inside your existing async application. `native_session`, `request_json`, `parse_selected_tool` and `make_tool_message` belong to your host; keep their behavior unchanged. Wrap the MCP session once. Use `kind="client"` for the modern MCP client, or `kind="session"` for its ClientSession adapter. The facade exposes `list_tools` and `call_tool`; your application initializes and closes the native session/transport.

```python
tools = rec.mcp(native_session, name="tools", kind="session")

with conversation.turn(user_input):
    payload = {"messages": messages, "tools": tool_definitions}
    with conversation.model_call(payload) as call:
        response = await request_json(payload)
        call.done(response)

    name, arguments = parse_selected_tool(response)
    choice = call.choice(name, arguments)
    with choice.run() as attempt:
        result = await tools.call_tool(name, arguments)
    messages.append(attempt.answer(make_tool_message(result)))
```

Keep tool discovery where the native SDK requires it, normally by calling `await tools.list_tools()` after native initialization. Discovery is a separate operation and cannot stand in for an executed result. A wrapper around several tool calls is not one unambiguous attempt. Ordinary JSON means supported Python JSON values; this manual boundary does not serialize arbitrary SDK objects or implement generic raw streaming.

For exactly one actual invocation, `choice.answer(message)` is shorthand for that invocation's `attempt.answer(message)`. Count rejected capture attempts too. The facade never chooses the latest accepted result for you.

## Native OpenAI Chat

Install `mcp-flightrecorder[openai-chat]==0.5.19` in your environment. Keep selection and argument parsing on the native response, including which completion alternative and which tool occurrence your application chooses.

```python
import json

with conversation.model_call(payload, create=client.chat.completions.create) as call:
    response = await call.create()
# These positions come from your host's selection policy.
selected_choice = response.choices[host_choice_position]
selected = selected_choice.message.tool_calls[host_tool_position]
name = selected.function.name
arguments = json.loads(selected.function.arguments)
choice = call.choice(name, arguments,
                     choice_index=selected_choice.index, tool_index=host_tool_position)
```

Use the same selected `name` and `arguments` for native execution. Alternative completions remain alternatives. Do not flatten recorded choices or let capture limits decide which tools execute. `call.create()` uses the existing OpenAI adapter, preserving its supported native return and streaming behavior. `record_usage=True` enables the existing optional usage observation; it does not invent missing provider counts.

## Replay, stable keys and prepared messages

Reuse one conversation across its turns. Ordinary in-memory history uses object identity, retaining the original message object so an address cannot be reused accidentally. For history rebuilt from storage, explicitly bind a stable key and submit the corresponding key beside each provider message. The key is not sent to the model.

```python
message = attempt.answer(tool_message, key="tool-answer-17", prepared=True)
messages.append(message)
message_keys.append("tool-answer-17")

# Later, after rebuilding messages and the parallel key list from your store:
with conversation.model_call({"messages": messages}, message_keys=message_keys) as call:
    response = await request_json({"messages": messages})
    call.done(response)
```

`message_keys` has one entry per message; `None` uses ordinary identity for an unkeyed message. A key lookup that fails stays unknown, even if another identity happens to match. Keys are scoped to the conversation and recorder generation. Conflicting associations cannot overwrite one another. A configured `conversation(..., key=key_function)` is a convenience over the same bounded string-key mechanism. A key callback returning `None` is unresolved; it does not silently fall back to identity.

Dedicated `role="tool"` messages without a binding are unknown. If your host uses an overloaded role such as `system` for tool answers, supply `is_tool_message=your_predicate` when creating the conversation. That declaration can downgrade an unrecognized answer to unknown; it cannot discover its source or make every system message a tool answer. Keep it applicable to old/rebuilt messages too, including after restart or binding overflow.

`prepared=True` snapshots supported string content or ordinary text blocks at bind time. The facade supplies that declared preparation again on later requests. It never replaces or mutates the caller's message. Later mutation remains detectable. Unsupported multimodal/custom structures and unavailable prepared snapshots receive diagnostics; they are not silently treated as verified preparation. Raw tool-to-submitted text mismatch remains visible. See the separate [preparation diagnosis contract](inspection.md#diagnostic-report-and-preparation).

## Retries and parallel choices

Keep retry decisions inside the existing retry helper. Instrument the actual inner invocation and return its selected attempt together with the native result:

```python
async def invoke_selected(choice, name, arguments):
    for retry in range(3):                 # your existing retry policy
        try:
            with choice.run() as attempt:
                result = await tools.call_tool(name, arguments)
            return result, attempt
        except YourRetryableError:        # your existing exception policy
            if retry == 2:
                raise

result, selected_attempt = await invoke_selected(choice, name, arguments)
messages.append(selected_attempt.answer(make_tool_message(result)))
```

A rejected final attempt cannot borrow an earlier accepted receipt, even when results are equal. Binding one handle around a helper that invokes tools repeatedly is ambiguous. Preserve the native retry count and exceptions; do not retry because capture failed.

For parallel choices, each task owns its own `with choice.run() as attempt` and binds its own result. `asyncio.gather` may schedule those tasks as your host already does. Do not share one attempt across fan-out. Use distinct conversation handles for concurrent turns; one conversation retains sequential turn history.

## Local and explicitly host-initiated work

A real local function is explicit; missing MCP capture never implies local execution:

```python
with choice.run(local=True) as attempt:
    result = attempt.call(local_function, **arguments)
messages.append(attempt.answer({"role": "tool", "content": str(result)}))
# For an async local function: result = await attempt.acall(function, **arguments)
```

This captures a host result and can associate the answer with the selected model decision. It does not create an MCP response: the legacy raw MCP-result text comparison remains unknown. Native function values and exceptions stay under host control.

For work the host initiates without a model decision, use `conversation.host_call(name, arguments)` inside a turn, then `with host_choice.run():` around the actual wrapped MCP invocation (or `run(local=True)` and `attempt.call`). The dispatch is explicitly host-initiated. An answer without a model decision remains unknown rather than acquiring a fictional model choice.

## Consumed SDK streams

For the native async OpenAI Chat adapter, pass `stream=True` in the host payload and consume the returned stream yourself:

```python
with conversation.model_call(payload, create=client.chat.completions.create) as call:
    stream = await call.create()
    try:
        async for chunk in stream:
            handle_native_chunk(chunk)
    finally:
        await stream.close()
```

There is no prefetch, forced drain or generic iterator wrapper. Early close, cancellation, native errors and partial capture remain distinct. The host must close abandoned streams and perform its normal selection from consumed native content. Existing [streaming limits and policy behavior](integrations.md) apply; legacy MCP and other model facades remain separate low-level adapters rather than new `AgentRecorder` promises.

## Size, health and recovery

`AgentRecorder` passes `limits`, `capture`, `comparisons`, `environment` and supported `retention_policy` through the current core/adapters. Core `limits` keys and defaults are in [capture admission](capture.md). Metadata retention omits bodies while retaining structure and available original comparison tokens. A retention callback uses the existing detached-value policy contract for observed model/MCP and host-result payloads; it does not change native requests or results. Core turn prompts and dispatch arguments have no retention-callback seam and are not redacted by that callback. Use `capture="metadata"` for broad body omission, or avoid putting sensitive data in those fields.

Per conversation, `binding_limit=1024` caps retained entries, `key_byte_limit=256` caps UTF-8 key bytes and `prepared_byte_limit=1048576` caps detached prepared bytes. Overflow refuses new bindings with a diagnostic; attribution stays unknown. Identity mode retains original caller objects, so these bounds do not limit arbitrary later mutation or process RSS. Prefer stable keys for service histories, close finished conversations, and close the recorder at worker shutdown. There is no global history store or automatic eviction service.

Inspect `rec.diagnostics` for bounded local `reason`, `hint` and `count` entries, and `rec.health()` for admitted-capture/storage state. `strict=True` is for development integration mistakes. Normal mode reports capture problems while preserving native work; capacity/storage loss never becomes an execution gate, including in strict mode. Invalid startup configuration remains an error. Local diagnostic notes are not all stored in the recording.

| Observation | Next action |
|---|---|
| Unknown binding after rebuilding history | Pass the same explicit message keys; check the selected attempt and conversation generation. |
| Ambiguous retry/fan-out | Bind the selected inner attempt, with one actual invocation per handle. |
| Binding conflict/limit | Use distinct occurrence keys; release completed conversations or adjust the relevant bound. |
| Prepared content unavailable | Use supported text; inspect size limits and later message mutation. |
| Rejected capture | Inspect receipt/health reasons and workload sizing; preserve the native result. |
| Writer/storage failure | Stop this recorder, retain its result/health, fix storage, then start a new recorder/path and new conversation bindings. Do not resume or overwrite the damaged recording. |
| Incomplete shutdown | Follow the existing [close/finalize contract](capture.md); never infer portability from file existence. |

Use `fr doctor FILE`, `fr turns FILE --last --text` and `fr explain FILE --last --text` to investigate. Every cutoff and unknown remains material. Doctor is descriptive, not a whole-run PASS gate, and it cannot prove which unobserved call was missing. The [inspection contract](inspection.md) documents JSON compatibility, selection, preparation validation and exit codes.
