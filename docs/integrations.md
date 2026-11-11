# SDK and host integrations

**Package 0.5.23.** Integrations wrap callables or sessions the host already owns. They observe exposed requests, results and consumed stream chunks; they do not create a client-routing layer, infer provider-ID associations or take over application cleanup.

The recorder core has no SDK dependency. Import an adapter explicitly after installing the matching extra or test profile. Separate dependency environments are necessary for incompatible MCP versions; [Testing](testing.md) lists the profiles.

For a runnable end-to-end example, see [Follow a model → tool → model roundtrip](tool_roundtrip.md). It uses genuine OpenAI and MCP SDKs with local deterministic peers, explicit selected-result bindings and prepared text, followed by the existing CLI investigation flow.

## 1. Public adapters and dependency boundaries

| Module under `mcp_flightrecorder.integrations` | Entry point | Package extra / minimum exercised SDK baseline |
|---|---|---|
| `openai_chat` | `wrap_model(recorder, native_create, *, instance_id, retention_policy=None, record_usage=False)` | `openai-chat`: `openai>=3.35.1` |
| `anyllm_ollama` | `wrap_model(recorder, native_acompletion, *, instance_id, retention_policy=None)` | `anyllm-ollama`: `any-llm-sdk[ollama]>=1.29.0`, `ollama>=0.6.3` |
| `mcp_client` | `wrap_mcp(recorder, native_client, *, client_id, server_key=None, retention_policy=None)` | `mcp`: `mcp>=2.3.0` |
| `mcp_session` | `wrap_session(recorder, native_session, *, client_id, server_key=None, retention_policy=None)` | `mcp-session`: `mcp>=2.3.0` |
| `mcp_session_legacy` | `wrap_session(recorder, native_session, *, client_id, server_key=None, retention_policy=None)` | `mcp-session-legacy`: `mcp>=1.23.1,<2` |

Labels are bounded recorder control strings. An integration snapshots recorder scope when the exposed callable is invoked, before returning lazy work. It freezes the actual exposed request when that work executes. Creating an unawaited coroutine produces no request, policy call or operation reservation.

Capture validation/capacity failures are reported in receipts and bounded evidence losses. Ordinary model and MCP policy faults also preserve native outcomes by selecting fixed omission before capture; process-control exceptions from a policy propagate as described below. Wrong integration arguments, an invalid evidence object, or reusing a single-use evidence object are caller errors and fail before native work begins.

## 2. Explicit evidence

`ModelEvidence` and `OperationEvidence` are exported by the adapter modules. Create a new object for each invocation and pass it through the explicit `evidence=` argument. Evidence objects contain receipts and bounded losses; they do not duplicate retained request/result bodies.

`ModelEvidence` exposes the request receipt, response receipt, decisions and losses. Each observed decision records its choice/tool indexes, provider reference and recorder receipt. `OperationEvidence` exposes start and terminal receipts and losses. The operation evidence type is shared by all three MCP facades.

Propagate the model decision's recorder receipt to an operation with `decision=` when the host knows that association. A host dispatch scope supplies the stronger explicit host-hook relationship. A call without either remains unattributed. Provider IDs, matching names or equal arguments alone are not a resolver.

For submitted model context, `annotations=` supplies the [capture contract](capture.md#3-closed-observations-and-request-annotations)'s per-message binding annotations. `insertions=` supplies explicit host-prepared material inserted into this request. The adapter does not invent annotations from provider message IDs.

When the host intentionally flattens or rewrites a tool result, record the actual prepared host material and explicit `sources`, `insertions`, binding and relative text-path evidence through the existing [capture annotations](capture.md#3-closed-observations-and-request-annotations) and [MCP lifecycle](capture.md#4-dispatch-and-mcp-lifecycle) APIs; comparison of genuinely different original projections can remain a mismatch.

## 3. OpenAI Chat

```python
from mcp_flightrecorder.integrations.openai_chat import ModelEvidence, wrap_model

create = wrap_model(
    recorder,
    native_client.chat.completions.create,
    instance_id="primary-model",
)
evidence = ModelEvidence()
response = await create(
    model="chosen-model",
    messages=[{"role": "user", "content": "Hello"}],
    annotations=[{"bindings": [], "unresolved": [], "complete": True}],
    evidence=evidence,
)
```

The returned callable has this boundary:

```text
create(*, evidence=None, annotations=None, insertions=(), **sdk_kwargs)
```

The wrapper validates the bound native signature at invocation and forwards the SDK arguments unchanged. It observes ordinary JSON explicitly supplied to the call, without synthesizing SDK defaults. Transport options such as `extra_headers`, `extra_query` and `timeout` are excluded from the model request projection. Unsupported one-shot values, legacy function forms or overriding `extra_body` prevent an authoritative request projection; they still reach the native SDK unchanged.

Trusted OpenAI response objects are projected with their JSON, API-name, exclude-unset serializer. A normal call returns the original native response object. Exposed raw tool arguments remain exact strings; capture does not invent a parsed body. Original argument comparison is available only when the core can validate a complete supported projection.

When the native result is an asynchronous stream, the returned proxy supports native async iteration/context management, response access and close methods. It forwards each native chunk as consumed, without prefetching or draining. The caller remains responsible for consumption and cleanup. Failure, cancellation, early close and incomplete chunks remain distinct terminal observations.

Stream accumulation uses choice/tool indexes and accepts nullable fragments. Two independent pools each retain at most 1,048,576 encoded bytes: whole raw chunk projections, and pending tool ID/name/argument fragments across all keys. There are at most 64 choices and 64 lifetime tool keys; emitted keys still occupy their positions. Emission or unsupported-tool discard releases only its fragment bytes. The first raw chunk that would exceed its pool closes raw retention and makes the aggregate partial, while selection inspection continues on that chunk and later host-consumed chunks without serializing their discarded full projections again. A fully observed tool can still produce a complete decision before its finishing native chunk is returned. Fragment or cardinality overflow keeps the conservative selection stop; actual projection gaps taint pending and future keys without revoking already emitted decisions. Complete aggregate observation requires observed end-of-stream and finished choices without a partial-source condition. Tool decisions require complete exposed names/arguments and the applicable tool finish reason. The combined bound is 2,097,152 retained encoded buffer bytes plus bounded container/object overhead; it does not bound native chunk size, transient projections, assembled payloads, callback time, total capture memory or process RSS.

### Opt-in response usage

The OpenAI factory accepts `record_usage=False`. It must be an exact Boolean; another type raises `ValueError("record_usage must be a boolean")` when constructing the wrapper. With the default `False`, the wrapper omits the normalized `details_json.usage` field on terminal `model.response` events. An ordinarily retained provider response body can still contain native usage fields under the configured content capture and retention policy. This option does not redact the response body. Set it to `True` to explicitly preserve three reported provider counts in `details_json.usage`, independently of response-body retention. The option belongs to the recorder wrapper and is never forwarded to the provider.

For streamed usage, the host must also request and consume the provider's aggregate:

```python
create = wrap_model(recorder, client.chat.completions.create,
                    instance_id="agent-model", record_usage=True)
stream = await create(model=model_name, messages=messages, stream=True,
                      stream_options={"include_usage": True}, evidence=evidence)
```

The host still owns turn, stream and recorder cleanup. The wrapper does not inject `stream_options`, drain, prefetch, issue another request or warn when usage is unavailable. An eligible aggregate has explicitly exposed empty `choices` and non-null usage, so unconditional `chunk.choices[0]` is unsafe. Stopping at the content finish marker, or closing before consuming the aggregate, can leave the recorded counts null.

| Recorded key | Explicit native field |
|---|---|
| `prompt_tokens` | `usage.prompt_tokens` |
| `cached_tokens` | `usage.prompt_tokens_details.cached_tokens` |
| `cache_write_tokens` | `usage.prompt_tokens_details.cache_write_tokens`, when exposed |

Each value is an exact integer in `0..2**63-1`, or null. The adapter checks `model_fields_set` at every native level: an unset SDK default is not a reported zero. Missing, null or invalid counts become null independently; valid siblings and explicit zeros survive. Valid but numerically inconsistent reports remain unchanged. Original counts are detached before normal response projection or retention, so replacement, omission, metadata capture and an independent content-projection failure cannot replace them with retained-body values. Usage extraction does not serialize the whole SDK object or retain it in the snapshot. Ordinary extraction faults remain local, and missing optional usage does not add a capture loss or make content partial. Existing process-control exception boundaries still apply.

With `record_usage=False`, no normalized usage fact is added. With `record_usage=True` and no observed snapshot, the wrapper attaches all three keys with null values. This differs from direct public `usage=None`, which leaves normalized usage absent. For a stream, the last consumed eligible aggregate replaces the entire per-call snapshot, including nulls. Counts are never summed, maximized or merged across aggregates. Other chunks leave the snapshot unchanged; usage on arbitrary content chunks is not treated as a final aggregate. A later consumed aggregate can update usage after body or selection capture has stopped, without reopening either capture path. The existing single terminal attempt attaches the snapshot and releases it in `finally`; native arguments, results, exceptions, cancellation and cleanup keep their existing behavior.

Usage counts remain potentially sensitive metadata even when bodies are omitted. They describe provider reports, not verified bills, exact uncached components, cache causes or monetary savings. This option is specific to the OpenAI factory; the AnyLLM and MCP factory signatures remain unchanged.

### Inspect recorded usage with `fr sql`

Read a finalized recording or a fixed snapshot. Use `fr runs run.sqlite` to find the desired run ID and substitute it for `RUN_ID`. The second parameter, `-1`, starts at the beginning of that run:

```sh
fr sql "run.sqlite" "
SELECT sequence, event_id, model_call_id,
       json_extract(details_json, '$.usage.prompt_tokens') AS prompt_tokens,
       json_extract(details_json, '$.usage.cached_tokens') AS cached_tokens,
       json_extract(details_json, '$.usage.cache_write_tokens') AS cache_write_tokens,
       CASE
         WHEN json_type(details_json, '$.usage.prompt_tokens') = 'integer'
          AND json_type(details_json, '$.usage.cached_tokens') = 'integer'
          AND json_extract(details_json, '$.usage.prompt_tokens') > 0
          AND json_extract(details_json, '$.usage.cached_tokens') >= 0
          AND json_extract(details_json, '$.usage.cached_tokens')
              <= json_extract(details_json, '$.usage.prompt_tokens')
         THEN 1.0 * json_extract(details_json, '$.usage.cached_tokens')
              / json_extract(details_json, '$.usage.prompt_tokens')
       END AS read_fraction
FROM observed_events
WHERE run_id = ? AND kind = 'model.response' AND sequence > ?
ORDER BY sequence
" --params '["RUN_ID", -1]' --limit 100
```

This uses the existing inspection envelope. Inspect `ok`, `error`, `cutoffs` and `data.has_more`, then read `data.columns` and `data.rows`. When a successful page has more rows, rerun the same query with the last returned `sequence` as its second parameter. Do not guess a continuation after an error or an empty partial result. Existing access refusals and CLI exit codes still apply.

Rows without usage remain present with NULL count fields. The three columns are ordinary SQLite JSON projections of producer-normalized integers or nulls, not a second sanitizer for damaged recordings. The CASE guards apply only to `read_fraction`: positive prompt count with explicit cached zero gives `0.0`; zero prompt count, unavailable counts or cached greater than prompt gives NULL. A missing write count does not prevent a read fraction. For example, reported prompt 10,000 and cached 8,000 yield `0.8`; the ratio is not monetary savings.

Sequence orders observed response events, not request starts, hidden attempts or requests lacking a response. Distinct events remain distinct even when their model-call IDs match. The query does not infer duplicates, select only a latest terminal or calculate session totals.

## 4. AnyLLM with Ollama

```python
from mcp_flightrecorder.integrations.anyllm_ollama import wrap_model

acompletion = wrap_model(
    recorder,
    provider.acompletion,
    instance_id="ollama-model",
)
```

The callable accepts the native positional/keyword signature plus the reserved integration keywords:

```text
create(*native_args, evidence=None, annotations=None, insertions=(), **native_kwargs)
```

It binds the native signature and captures supported ordinary JSON request arguments. `messages` must be an exposed list. `timeout` and `session_label` are not model input. Supported Ollama option keys are `num_keep`, `seed`, `num_predict`, `top_k`, `top_p`, `min_p`, `typical_p`, `repeat_last_n`, `temperature`, `repeat_penalty`, `presence_penalty`, `frequency_penalty`, `stop`, `num_ctx` and `num_batch`.

The shared model lifecycle and stream accumulator apply. Ollama streams use representation `anyllm_ollama_stream/1`; completed tool choices accept the provider's `stop` or `tool_calls` finish reason. An async iterator with a callable `aclose` is wrapped with async iteration and `aclose`; it does not acquire OpenAI-specific context-manager or response conveniences. Native return objects, exceptions and caller-owned closure remain intact.

### Selective model body retention

Both model factories accept keyword-only `retention_policy=None`; the returned native-call signatures and argument forwarding above remain unchanged. `None` adds no policy call or policy copies. The callback must be synchronous. Factory creation rejects noncallable values and declared coroutine or async-generator functions before native work. Returned asynchronous values are never awaited or scheduled; an ordinary returned coroutine is closed and treated as invalid output.

Call `retention_policy(kind, payload, context)` for each available body:

| Kind | Existing projected payload | Context values |
|---|---|---|
| `model.request` | Supported submitted body, including ordered `messages` and any submitted `tools`; existing transport/default/extra-body exclusions still apply | `instance_id`; every other value is null |
| `model.tool_call` | Separate exposed tool-decision body, including `arguments_raw` when available | `instance_id`, exposed `choice_index` and `tool_index`, bounded original `name` and `provider_ref` |
| `model.response` | Normal response projection, or aggregate of stream chunks actually consumed | `instance_id`; every other value is null |

Context has exactly those five keys. A normal decision uses its tool position; a stream decision uses its exposed tool index. Context is local to the invocation and does not change original control metadata or attribution. Native arguments, SDK results/chunks, original capture projections, callback payload/context copies and the detached returned mapping have separate ownership. Validation and UTF-8 encoding finish before admission; later callback-held mutation cannot change stored bytes or original evidence.

Use the existing [core retention mapping](capture.md#5-retention-and-shutdown): bounded nonempty `policy_id`, either `retained` or nonempty `omitted_reason`, and optional boolean `redacted`/`truncated`. Callback return `None` is invalid. `retained: None` stores JSON null for decisions/responses; it is invalid for a structured request because required paths disappear.

For example, this host explicitly keeps bodies from a public model, redacts each request message and the separate decision for a sensitive model, and omits its normal or consumed-stream response. The shown native request uses ordinary message objects with a `role`:

```python
from mcp_flightrecorder.integrations.openai_chat import wrap_model


def retain_model(kind, payload, context):
    if context["instance_id"] == "public-model":
        return {"policy_id": "keep-observed", "retained": payload}
    if kind == "model.request":
        replacement = {
            "messages": [
                {"role": message["role"], "content": "[redacted]"}
                for message in payload["messages"]
            ]
        }
        if "tools" in payload:
            replacement["tools"] = []
        return {"policy_id": "request-redaction", "retained": replacement,
                "redacted": True}
    if kind == "model.tool_call":
        return {"policy_id": "decision-redaction",
                "retained": {"arguments_raw": "{}"}, "redacted": True}
    if kind == "model.response":
        return {"policy_id": "response-omit", "omitted_reason": "not-retained"}


create = wrap_model(
    recorder, native_client.chat.completions.create,
    instance_id="sensitive-model", retention_policy=retain_model,
)
response = await create(
    model="chosen-model",
    messages=[{"role": "user", "content": "Customer 123"}],
)
```

The native call receives the original request and returns its original SDK object. Pass the same callback option to the AnyLLM `wrap_model` factory for its supported submitted body. Each body kind makes its own policy selection: masking decision arguments does not mask the same text in the response projection. A host must handle every representation it wants to omit or redact.

Retained requests use core's same structural rule before capture: keep `/messages` and the submitted `/tools` path when present; messages remain an array with the original count. A recognizable unchanged original message cannot move to a different original position. Per-position replacement is allowed. Retained tools may be replaced, as above; the rule adds no retained-catalog length/order requirement and cannot infer arbitrary semantic reordering after messages are rewritten. Null replacement, a missing required path, a non-array/count change or recognizable reordering selects fixed omission. The original request still determines membership, catalog relationship, bindings and insertion validation. The structural `request_refs/2` manifest remains available even when referenced content is omitted.

Ordinary callback, mapping, encoding or topology faults, including callback-raised `asyncio.CancelledError`, select exactly `{"policy_id": "adapter_policy_error", "omitted_reason": "retention_policy_failed"}` before one core observation attempt. The side channel adds the bounded loss and logs only that reason once per invocation, even if request, decisions and response all fail. No raw fallback, exception text, second admission or duplicate native call is introduced. Omission alone does not make a source-complete observation partial. Invalid retention submitted directly to core still rejects under its existing rules.

Original request-part `observed_bytes`, membership and supported decision `args_digest` values describe original values. Stored lengths, SHA256 and flags describe retained bodies. Equal supported original arguments compare within one run under different policies; retention-distinct message artifact IDs may differ. No model-response text digest, catalog comparison token or public message-digest field is added. Global `capture="metadata"` still calls a configured policy and omits content with `capture_metadata` precedence, including after policy failure; valid policy IDs/flags and original completeness follow the existing core semantics.

Scope is captured at invocation; projection and policy run only when lazy work executes. Never-awaited work calls neither policy nor native code and creates no observation. `MISSING` projections skip the callback rather than become JSON null. Native error/cancellation paths with no body retain their existing response state and do not expose a new error-payload API.

For streams, policy selection occurs for emitted decisions and the final aggregate of consumed chunks, not for every chunk. Native chunk identity/order, bounded accumulation, early close and cleanup remain unchanged. There is no prefetch or drain. A consumed partial/error/cancelled aggregate can be retained or redacted while keeping its original incomplete state. Policy execution adds synchronous work; it is not a remedy for synchronous capture cost or a performance guarantee.

`SystemExit`, `KeyboardInterrupt` and other unhandled `BaseException` values propagate. A request interruption can prevent native execution. A decision or terminal interruption can replace a pending result, withhold a just-consumed chunk, or leave a missing terminal and incomplete recording. These are explicit exceptions to ordinary policy-fault containment. If the host catches an interruption and resumes that stream, automatic cleanup and at-most-once policy invocation have no new guarantee. The existing host attachment helper signatures remain unchanged and do not accept `retention_policy`.

## 5. MCP operations

### MCP 2 Client

```text
wrapped.list_tools(*, evidence=None, cursor=None, meta=None, cache_mode="use")
wrapped.call_tool(name, arguments=None, read_timeout_seconds=None,
                  progress_callback=None, *, evidence=None, decision=None,
                  input_responses=None, request_state=None, meta=None)
```

### MCP 2 ClientSession

```text
wrapped.list_tools(*, params=None, evidence=None)
wrapped.call_tool(name, arguments=None, read_timeout_seconds=None,
                  progress_callback=None, *, input_responses=None,
                  request_state=None, meta=None, allow_input_required=False,
                  allow_claimed=False, evidence=None, decision=None)
```

`params` is the native `PaginatedRequestParams` object or `None`. Its supported serializer retains API aliases and excludes unset fields; an absent value remains distinct from an explicitly empty object.

### MCP 1.23.1 ClientSession

```text
wrapped.list_tools(cursor=None, *, params=None, evidence=None)
wrapped.call_tool(name, arguments=None, read_timeout_seconds=None,
                  progress_callback=None, *, meta=None, evidence=None,
                  decision=None)
```

The legacy facade uses MCP 1's `ListToolsResult`, `CallToolResult`, `McpError.error` and camel-case `isError` contract. Its timeout argument is the native `timedelta`, forwarded unchanged. It uses MCP 1/httpx exception classes; the MCP 2 facades use their native MCP 2/httpx2 classes. This small version boundary is explicit rather than a generalized runtime SDK detector.

### Shared lifecycle

Each actually executed call creates one `mcp.request` and, when exposed, one terminal `mcp.response`. Start and terminal receipts share the operation's frozen scope. Return values retain their native identity. Tool-error, protocol-error, timeout, transport-error, cancellation and local-error outcomes retain their native distinction; a recording failure does not swallow or replace the native exception.

An observation covers the wrapped public method. SDK-internal retry attempts and unwrapped sampling, elicitation or progress notifications are not separate observed calls. An exposed generic MCP error code alone cannot establish transport origin; preserve the native error data rather than infer an origin from that code.

The adapters observe public result projections and exposed text locations. Unsupported or partial native result forms remain incomplete, with unavailable comparison tokens. MCP 2 input-required results allowed by the caller retain their exposed body as an incomplete observation; they do not claim a final text result.

`resolution` remains `unknown` and `input_rounds` remains unavailable unless a producer positively witnesses those facts. A cache option or absence of network activity does not prove a served cache hit. The core's `cache.served` observation is available for a host that actually observes one.

### Selective MCP body retention

All three MCP factories accept keyword-only `retention_policy=None`; their returned `list_tools` and `call_tool` signatures above are unchanged. `None` keeps the default route without policy execution or extra policy copying. A configured callback must be synchronous. Factory creation rejects noncallable values and declared coroutine or async-generator functions before native work.

The callback is called as `retention_policy(kind, payload, context)`. `kind` is `mcp.request` or `mcp.response`. `context` has exactly `client_id`, `server_key`, `method` and `name`; `method` is `tools/list` or `tools/call`, and listing has `name=None`. Payload and context are separate detached ordinary-JSON copies. Method/name remain local to each invocation, including overlapping calls.

For a request, `payload` is the full `{"arguments": ...}` envelope. Listing arguments preserve the facade's existing cursor, metadata and typed pagination projection. A response payload is the existing translated discovery, tool result, tool-error result or projected terminal error body. The callback may modify its copies without modifying native arguments/results or original capture comparisons. Its returned mapping is detached, validated and UTF-8 encoded before the one capture attempt, so subsequent mutation of policy-owned references cannot change admitted evidence.

For example, omit discovery bodies and replace both recorded bodies for one sensitive tool, while explicitly keeping other observed bodies:

```python
from mcp_flightrecorder.integrations.mcp_client import wrap_mcp


def retain_mcp(kind, payload, context):
    if context["method"] == "tools/list":
        return {"policy_id": "discovery-omit", "omitted_reason": "not-retained"}
    if context["name"] == "lookup_customer":
        replacement = (
            {"arguments": {"redacted": True}}
            if kind == "mcp.request"
            else {"redacted": True}
        )
        return {"policy_id": "customer-redaction", "retained": replacement,
                "redacted": True}
    return {"policy_id": "keep-observed", "retained": payload}


observed = wrap_mcp(
    recorder, native_client, client_id="customer-tools",
    server_key="customer-service", retention_policy=retain_mcp,
)
result = await observed.call_tool("lookup_customer", {"customer_id": "123"})
```

The native client still receives the original customer ID and returns its original result object. Supply the same callback through either `wrap_session` factory when using a session. These mappings use the existing [core retention contract](capture.md#5-retention-and-shutdown): a bounded nonempty `policy_id`, either `retained` or a nonempty `omitted_reason`, and optional boolean `redacted`/`truncated`. A configured callback must explicitly return a mapping. Returning `None` is a policy failure; returning `{"policy_id": "null", "retained": None}` is a valid JSON-null replacement. Unknown fields, invalid labels/flags, conflicting omission, unsupported/cyclic values, nonfinite numbers and encoding failures are invalid.

Ordinary callback or validation exceptions, including callback-raised `asyncio.CancelledError`, select `{"policy_id": "adapter_policy_error", "omitted_reason": "retention_policy_failed"}` before capture. The evidence side channel records the bounded `retention_policy_failed` loss and logs that reason once per invocation. Core receipts keep their existing body-omission semantics. No raw payload or callback exception text enters diagnostics, and there is no raw fallback, second admission attempt or second native call. Asynchronous callback results are never awaited or scheduled; an ordinary returned coroutine is closed and treated as invalid output.

`SystemExit`, `KeyboardInterrupt` and other unhandled `BaseException` values propagate. A request-side interruption can prevent native execution; a response-side interruption can replace the pending native outcome and leave an unfinished operation and incomplete close. This exception to ordinary fault containment also applies while recording a native error. Apart from such process-control interruption, native return, exception and cancellation identity is preserved even when the policy fails.

Scope is fixed at wrapper invocation, while projection and policy execution occur when its lazy work executes. Never-awaited work calls neither the policy nor native code. An unavailable `MISSING` projection skips the callback and preserves unavailable evidence; ordinary JSON null remains a value. Original argument/result-text comparisons and `observed_bytes` use originals. Retained bytes determine stored size, SHA256 and flags. Global `capture="metadata"` always omits bodies, including retained policy output, with the existing `capture_metadata` reason taking precedence. Policy omission alone does not make an otherwise complete original observation incomplete.

The model factories provide their separate three-kind policy boundary described above. The existing host attachment helpers below keep their current signatures and do not accept `retention_policy`.

## 6. Host attachment examples

These are source examples for concrete host seams. They preserve the host's ownership of routing and lifecycle. They do not promise capture for arbitrary future host versions or every clone/cache path.

Passive attachment without the host's decision/binding annotations can remain unknown or incomplete even when the application succeeds; see the [request annotation contract](capture.md#3-closed-observations-and-request-annotations).

| Example | Host seam and responsibility |
|---|---|
| [`fast_agent.py`](../examples/fast_agent.py) | `make_recorded_llm_factory(recorder, model_spec, *, instance_prefix)` delegates to the host factory and attaches the actual OpenAI create method once per produced model. Prefixes are at most 223 UTF-8 bytes. Attach the factory with the host's `attach_llm`; a model-changing clone needs attachment before its first send. |
| [`fastmcp_session.py`](../examples/fastmcp_session.py) | `attach_session(recorder, native_session, *, client_id, server_key=None)` wraps the two session methods after entering the host's async client context. Reattach after reconnect. High-level cached results can bypass the session and create no session operation. |
| [`mcp_agent_session.py`](../examples/mcp_agent_session.py) | `make_session_init_hook(recorder, *, client_id, server_key=None)` returns the synchronous `(session, auth) -> bool` hook. Compose existing host initialization first. A missing session returns false; the host does not treat that return as a startup abort. |
| [`pydantic_openai.py`](../examples/pydantic_openai.py) | A no-network mock route attaches the actual OpenAI callable and declares incomplete model coverage. If a host creates a per-run client clone, attach its callable too. Run with `python -I -B examples/pydantic_openai.py NEW.sqlite`. |

The fast-agent recipe uses the host's `_openai_client` attachment seam, which can change with future host releases. The FastMCP recipe attaches to a native session exposed by the released FastMCP SDK rather than to its high-level client; recheck that seam when upgrading. The legacy mcp-agent example uses the historical 0.2 API and requires a separately isolated MCP 1.x environment. The lower bounds in `requirements/` identify exercised SDK baselines, not a promise that every newer version preserves these integration seams. See [Testing](testing.md) for separate environments and execution commands.

The test-only [`minimal_host.py`](../tests/producers/minimal_host.py) scenario uses the small JSON inputs in `tests/fixtures/integration/`. It exercises ordinary and streamed responses, dispatch, MCP operations and repeated context membership. [Testing](testing.md) explains how to generate its recording temporarily; no recorded database is shipped.

## 7. Maintaining an adapter

SDK-specific projection and exception translation live in the thin public facades. `_model_capture.py`/`_model_stream.py` provide the shared model lifecycle, and `_mcp_capture.py` provides the shared operation lifecycle, including completion and cleanup.

Validate a changed seam with its native dependency profile. Check native return identity, exception identity, cancellation, lazy execution, stream cleanup and explicit scope/receipt propagation. Any host path that bypasses the observed callable must remain declared unavailable rather than being silently counted as covered.


## Operational diagnostics

The shared evidence guard logs each newly observed loss reason once per invocation through `mcp_flightrecorder.integrations._common`, including invocations without an explicit evidence object. This covers capture rejection, unavailable projections, `retention_policy_failed` and bounded stream/decision loss without per-chunk warnings. The logs exclude SDK inputs, outputs, names and raw exceptions. Ordinary application-handler failures do not replace native returns, errors or cancellation. Configure the standard logger in the host; see the [logging contract](capture.md#6-operational-logging).
