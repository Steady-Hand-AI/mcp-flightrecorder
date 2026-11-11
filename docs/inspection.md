# Inspection API and CLI

**Package 0.5.24 · schema 6 · inspection `mfr.inspect/1` · diagnostics `mfr.diagnose/1`.** Standard-library readers serve Python and the CLI. They expose retained observations, explicit attribution and uncertainty. They do not score agents, write findings, repair recordings or infer provenance.

## Start with a readable investigation

For a complete file/run selection and evidence-policy workflow, see [investigating a recording](agent-workflow.md). Version 0.5.22 added directory selection and explicit answer-scope/readable-gate conveniences.

```sh
fr doctor recording.sqlite
fr turns recording.sqlite --last --text
fr explain recording.sqlite --last --text
# Copy a displayed model-call ID to inspect its recorded details:
fr explain recording.sqlite --last --model-call MODEL_CALL_ID --text
```

`--last` selects the latest recorded turn by `started_at`, then `turn_id`; it does not select an active conversation or infer which user you meant. With `turns`, filters apply before selecting the last match. Use `--turn TURN_ID` for a stable explicit selection. If a file has multiple runs, first use `fr runs recording.sqlite`, then add `--run RUN_ID` to each command. Ambiguous and empty run populations are refused. A run with no recorded turns cannot satisfy `explain --last` or `doctor --last`.

The numbered turn and model-call labels are local display labels; the displayed IDs remain authoritative. `turns` and `explain` still emit their existing JSON by default. `--text` escapes control characters and keeps unknowns and cutoffs visible. Continue a turn page with its displayed `--cursor`, or narrow the selection; `--last` cannot be combined with a cursor. Text output has the selected `--max-output-bytes` budget, including its terminal newline, and announces text clipping. A clipped view is incomplete.

`doctor` defaults to readable text; `--json` returns the diagnostic envelope below. Its successful exit means that inspection completed, **not** that every event was captured or that the agent answered correctly. Missing evidence does not prove one unique missing host call. Recorded metadata and labels remain untrusted and potentially sensitive.

### Diagnostic report and preparation

```python
report = inspect.diagnose(
    "recording.sqlite", run_id=None, turn_id=None,
    limit=50, max_output_bytes=65536, no_content=True, timeout=10.0,
)
```

`diagnose` is the one public diagnosis function. It uses one existing read snapshot, infers a run only from exact singleton cardinality, and returns the common transport fields with `format="mfr.diagnose/1"` and `operation="diagnose"`. The original `mfr.inspect/1` readers, flags, statistics and `schema()` response keep their existing meanings. Diagnosis is a separate descriptive contract.

The diagnostic `data` object contains `run`, `diagnostics`, `preparations`, `evaluated_events`, `event_limit`, `events_remaining`, `diagnostics_omitted`, `preparations_omitted`, and `whole_run_verdict="not_evaluated"`. Each diagnostic has a `code`, explicit `location`, factual `observed` description and a likely corrective `hint`. Categories distinguish unknown bindings, intentional metadata content omission, unsupported request input, rejected/lost capture and writer failure. Run health counters describe the whole run even when a turn is selected. Facade development diagnostics are live local information; doctor does not pretend that every such note was persisted.

`limit` is 1–200 (default 50), bounding inspected events and each displayed array separately. Events are visited in recorded sequence order, optionally within `turn_id`. Event limits, omitted items, reader unknowns and output clipping remain explicit. Doctor has no whole-run PASS state, including for an empty or apparently healthy page. `--limit`, `--turn`, `--last`, `--run`, `--timeout` and `--max-output-bytes` provide bounded investigation choices. A deadline returns an operational error, not a partial successful report. CLI run and latest-turn selection are separate bounded reads before the requested reader, so they do not promise one end-to-end snapshot or deadline.

Each preparation entry names its insertion link, prepared event, submitting request and target pointer. It reports `state` (`matches`, `differs`, or `unknown`), `prepared_text_equal`, `raw_text_equal`, `reason`, `source_count`, `unavailable_sources`, and `semantic_equivalence="not_verified"`. A positive entry means **host declared preparation; prepared text matches submitted text**. It requires authoritative same-run endpoints in order, valid insertion and answer membership, every declared source resolved and complete, and compatible `observed_original` `text_blocks_json/1` tokens. The selected original result must be one of the declared sources, and the original result comparison must be available. Source validation finishes completely within the deadline; a sampled source list cannot earn a positive result.

Metadata retention can preserve those tokens and therefore support a selected-text comparison without retained bodies. It does not excuse a missing source, invalid pointer, incomplete observation or incompatible token. Unsupported structures, unavailable original comparisons and ambiguous submitted locations remain unknown. There is no fallback from original evidence to a retained, redacted or truncated prefix. A root source location can be resolved with an omitted body; an unavailable nested source location remains uncertain.

Declared preparation never clears `text_mismatch` or changes `text_equal`. Prepared text can match what was submitted while differing from the raw tool result. Matching prepared bytes establish neither semantic equivalence nor model attention, causality or answer correctness. A changed prepared message yields `differs` when compatible original tokens remain available.

## 1. Public Python surface and limits

Use `from mcp_flightrecorder import inspect`. This is a package submodule; importing the base package does not eagerly import readers or SDKs. Functions return ordinary JSON-ready dictionaries, including expected operational errors. There is no public reader/session wrapper or result class.

```python
check(path, run_id, expect=None, *, max_output_bytes=65536, no_content=False, timeout=10.0)
find_turns(path, run_id, *, filters=None, limit=50, cursor=None,
           max_output_bytes=65536, no_content=False, timeout=10.0, last=False)
explain(path, run_id, turn_id, model_call_id=None, *,
        max_output_bytes=65536, no_content=False, timeout=10.0)
payload(path, run_id, artifact_id, offset=0, max_bytes=4096, *,
        max_output_bytes=65536, no_content=False, timeout=10.0)
slice(path, run_id, request_event_id, *, limit=1000, max_depth=12,
      max_output_bytes=65536, no_content=False, timeout=10.0)
query(path, sql, params=None, *, limit=200, timeout=10.0,
      max_output_bytes=65536, no_content=False)
runs(path, *, limit=50, cursor=None, max_output_bytes=65536, no_content=False, timeout=10.0)
gate(path, run_id=None, *, expect=None, min_activity=None, answers="all",
     max_output_bytes=65536, timeout=10.0)
snapshot(path, output, *, timeout=10.0)
schema()
```

`path` is a string or `os.PathLike` local file path, not a caller-supplied SQLite URI. IDs are nonempty strings. Integers exclude bool; booleans are actual bool. Invalid argument values/types/ranges and non-finite numbers are `invalid_request`. Wrong Python arity or an unknown Python keyword may retain native `TypeError`; unknown CLI arguments use the JSON invalid_request rule. `check` accepts `expect=None` or a list/tuple/set/frozenset of distinct names from `model`, `host`, `mcp`; preserve canonical model, host, MCP output order without mutating callers. Duplicate list/tuple entries are invalid; sets already contain distinct values. For `check`, empty Python collections mean no additional expectation; `gate` has stricter expectations below. A string or unknown name, including `context`, is invalid. `filters` is None or the closed mapping in §4. No automatic coercion of arbitrary Python objects.

| Limit | Default / permitted range |
|---|---|
| Whole encoded JSON | 65,536 bytes; caller range 1,024–1,048,576 |
| Display string | 512 UTF-8 bytes; full character boundaries |
| `runs` / `find_turns` items | 50; 1–200 |
| Complete cursor token | 8,192 ASCII characters; shared input/output cap |
| `explain` overview / selected-call detail | 50 calls / 200 detail records; fixed |
| `check` witnesses | 10 per item; aggregates remain complete |
| `slice` records / graph depth | 1,000 / 12; caller 1–1,000 / 0–12 |
| Payload retained-content bytes | 4,096; caller 4–65,536 |
| Query rows / input SQL | 200 (1–1,000) / 65,536 UTF-8 bytes |
| Query SQLite strings / BLOBs / encoded rows | 8,388,608 bytes; fixed, independent of output, display, payload and cache budgets |
| Diagnostics | 20 unknowns and 20 cutoff entries; count omitted entries |
| Reader invocation / snapshot deadline | 10 seconds; check/find_turns/explain/payload/slice/query/runs/diagnose/gate/snapshot permit finite numeric `0 < timeout <= 30`, excluding bool |
| SQLite busy wait | At most 1,000 ms and remaining deadline |

All limits are practical/cooperative bounds, not an OS timeout or total-memory guarantee. Each reader invocation has one monotonic deadline covering opening, reading, conversion and encoding, with checks between Python work and an SQLite progress handler. A completed count must finish within that deadline; otherwise the invocation returns an operational error. CLI run inference, when needed, is a separate reader invocation with its own deadline before the selected operation. The implementation has no process-isolation guarantee.

The scoped readers and `runs` accept keyword-only `timeout=10.0`. Invalid timeout values are rejected before recording access. The corresponding CLI commands forward `--timeout` to **each** read: an inferred run uses one selection read and one operation read, each with the chosen budget; an explicit run uses only the operation read. This is not a single end-to-end deadline. `runs` forwards the same chosen timeout; SQL/snapshot timeout behavior is unchanged. Gate selects and evaluates its run in one transaction and deadline.

Within one read snapshot, a private LRU retains immutable event/artifact rows, relation facts, compact answer descriptors and conservative membership minima. It has an **8,388,608-byte accounted cap, 4,096-entry cap and 1,048,576-byte per-entry cap**. Accounting includes keys, values and conservative container overhead. Request manifests and oversized entries bypass the cache. These limits do not bound SQL buffers, returned arrays or process RSS. Missing rows are cached only for that snapshot; raw and observed rows, runs and later calls stay distinct. Deadlines are checked even on cache hits, and the cache is cleared on success or failure.

Answer completeness is evaluated for each occurrence, not reused from an earlier request. Both diagnostic branches retain their repetitions and omission counts. A fully covered membership prefix can prove that no strictly earlier member exists; an earlier minimum still requires the original manifest-authority check. Eviction or refusal of any minimum permanently disables that shortcut for the invocation. It never converts unknown or incomplete evidence into authority.

## 2. Envelope, encoding and error rules

Every inspection operation, including `schema`, `gate` and snapshot, uses these exact top-level keys. The separate diagnosis contract above uses the same keys with its own format and operation:

```json
{
  "format": "mfr.inspect/1",
  "operation": "explain",
  "ok": true,
  "scope": {"run_id": "r_...", "turn_id": "t_..."},
  "data": {},
  "unknowns": [],
  "cutoffs": [],
  "diagnostics_omitted": {"unknowns": 0, "cutoffs": 0},
  "error": null,
  "untrusted_content": true
}
```

Operation names are Python function names (`find_turns`, `query`), including `snapshot` and `gate`. Scope contains only applicable validated IDs: `run_id`, `turn_id`, `model_call_id`, `request_event_id`, `artifact_id`. Discovery/schema/query/snapshot use `{}`. On failure `ok=false`, `data=null`, `error={"code":..., "reason":..., "message":...}`; scope may be `{}` to avoid echoing invalid/oversized input. A successful `check` with failed integrity items still has `ok=true`: the read completed. CLI status then follows its check result.

An unknown/cutoff entry has `code` (stable machine reason), `path` (JSON Pointer into data or empty), `ids` (applicable named IDs) and `count` (nonnegative integer, or null if unknowable). Codes use specific reasons such as `manifest_unavailable`, `association_unresolved`, `non_authoritative`, `missing_endpoint`, `comparison_unavailable`, `item_limit`, `byte_limit`, `string_limit`, `depth_limit`. Additional specific reason strings are allowed; the fields and error codes are fixed. Diagnostics must not contain bodies, arbitrary input or exception reprs. Omitted diagnostics increment the respective counter; zero never conceals discarded entries.

Measure `json.dumps(..., ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')`; the CLI's one trailing newline is excluded. IDs, cursors and identity-bearing pointers remain intact. Other display strings may be clipped to 512 UTF-8 bytes with a cutoff. Trim whole trailing records/witnesses, keeping exact completed totals. If required scope/summary or even one requested page item cannot fit, return `limit_exceeded`; never return malformed JSON or clipped IDs. Error envelopes fit 1,024 bytes and avoid arbitrary input echo. An invalid output budget uses that small standard error envelope even when the caller requested fewer bytes. Diagnostics themselves must fit the final cap.

Pages use the last **emitted** item after byte trimming, never the last scanned item. Query/slice/explain return explicit cutoffs, not fabricated complete results. `schema` is a fixed bounded discovery response; it also respects the default output budget.

| Error code | CLI exit | Meaning |
|---|---:|---|
| `invalid_request` | 1 | Invalid arguments, wrong root kind, destination refusal or ambiguous CLI run |
| `unknown_run`, `unknown_turn`, `unknown_event`, `unknown_artifact` | 1 | ID absent from selected scope |
| `cursor_mismatch` | 1 | Invalid cursor or changed bound scope/filter/policy |
| `payload_omitted` | 1 | Existing artifact has no retained body |
| `content_disabled` | 1 | Payload/SQL requested under no-content |
| `sql_rejected` | 1 | Statement, function, binding or result type outside query contract |
| `query_timeout` | 1 | Cooperative read/copy deadline expired |
| `limit_exceeded` | 1 | Required intact result cannot fit its valid cap, or SQLite rejects a query value/encoded row (`sql_value_limit`) |
| `incompatible_schema` | 3 | Unsupported version or missing required table/view/column |
| `recording_unreadable` | 3 | Missing/unreadable/corrupt DB or other storage failure |
| `recording_open_wal` | 3 | Established WAL-related read failure; accessible original/complete stopped fileset needed |

Successful ordinary commands exit 0. `check` and `gate` exit **2** for any required fail, otherwise **4** for any required unknown, otherwise 0; fail outranks unknown. There is no score threshold. Python does not swallow `KeyboardInterrupt` or `SystemExit`; CLI interruption exits 130 without a partial success envelope. Unexpected implementation exceptions propagate rather than being converted into `recording_unreadable` storage errors.

## 3. Read scope, metadata projection and comparison

Each reader invocation owns one escaped `mode=ro` SQLite connection and short read transaction, closed explicitly on success/failure/interruption. It sets `query_only` and verifies version 6 and the required names/columns in [schema.sql](../sql/schema.sql). Benign additional indexes are compatible. It does not compare DDL text, create schema, set source journal mode or use `immutable=1`. Most run-scoped CLI commands without `--run` first select the sole run in a separate read, then invoke the selected reader; an explicit `--run` avoids that lookup. `gate` and `diagnose` support singleton selection inside their own reader transaction, though CLI doctor may use separate selection reads. `fr schema` returns static data without opening a recording. Pages are separate snapshots: live discovery is best effort, not a repeatable multi-page session. Prefer closed recordings or portable snapshots for measurements.

Error precedence is argument shape/range validation, no-content rejection, DB/schema opening, run/turn/child selection, cursor validation, then operation/encoding. Value validation needing stored content (such as a UTF-8 offset) occurs in the operation step. Unknown run is checked before children. A foreign-run/turn ID returns the same unknown code as an absent ID. A missing selected model call uses `unknown_event`, reason `unknown_model_call`. A present selected event of the wrong slice root kind is `invalid_request`; an unselected/non-authoritative root is `unknown_event`. Existing zero-call turns explain successfully.

Use `observed_events` authority (`observed`, event_version 1, selected boundary producer). Do not silently count imported/wrong-source/unsupported-version rows as observations. `events` may still show them with `authoritative=false` and an unknown diagnostic. Operations/links remain explicit stored facts; joins are run-scoped. Never match names, payload equality, timestamps or provider IDs to assign provenance.

### Shared metadata records

These are output projections, not new storage tables/classes. Fields retain their stored meaning; nullable fields remain null. Stored boolean columns (`redacted`, `truncated`, `artifact_missing`) become JSON booleans, not 0/1; genuinely numeric counts stay integers. Unless specified below, return exactly the named fields; never expose a raw `details_json`, policy/environment/finding blob or body through metadata.

| Record | Fields |
|---|---|
| Run | `run_id, started_at, ended_at, close_state, dropped_events, writer_errors, coverage, health` |
| Turn | `run_id, turn_id, conversation_id, started_at, ended_at, prompt_ref, dataset_item_id, repeat_index, condition_key, conditions, host_outcome, capture_state, coverage` |
| Event | All scalar columns of `events` **except** `details_json`, plus `authoritative`, `complete` (known boolean or null) |
| Operation | All columns of `operations` (all are scalar) |
| Artifact | All columns of `artifacts` **except** `body_text` |
| Link | All columns of `links` **except** `details_json`, plus `binding_format, message_artifact_id, relative_pointer, result_event_id` from those named details keys, otherwise null |
| Member | All columns of `request_members` |
| Answer | `run_id, request_event_id, turn_id, model_call_id, request_sequence, message_index, message_artifact_id, link_id, source_event_id, binding_witness_event_id, binding_request_event_id, target_pointer, association_state, result_event_id, text_equal, comparison_reason, origin, origin_reason, origin_status, dispatch_event_id` |

Answer `origin` is `mcp`, `local` or `unavailable`; `origin_status` and `origin_reason` preserve the shared validator outcome, and `dispatch_event_id` is present only when established. Current/original binding witnesses, same-run authoritative endpoints and explicit relevant dispatch/operation evidence must support origin. Null result identity alone does not prove local handling. This additive projection does not change raw `text_equal` or `comparison_reason`.

`coverage={"producers":{"model":string|null,"host":string|null,"mcp":string|null},"model_complete":bool|null}` projects only known fields. Missing/malformed fields yield null plus uncertainty, not invented explicit unconfigured coverage. `health={"reason_counts":{known_reason:nonnegative_int},"complete":bool}` reads `runs.policy_json.health.reason_counts`. The required known keys are `body_omitted, capacity, closed, duplicate_completion, finalize_busy, finalize_timeout, invalid_input, invalid_scope, other, partial, prerequisite_lost, turn_update_lost, unavailable_boundary, unresolved, writer_failed`. Counter values are nonnegative integers, excluding bool. Emit valid known entries only; complete means all 15 are present/valid, not that capture was complete. Missing/malformed diagnostics are unknown, but any known positive actual-loss counter still fails health. Use run columns for dropped_events/writer_errors. Do not serialize arbitrary extra health keys.

Ordinary mode includes `conditions` as the **whole parsed JSON value only if its compact encoded size is <=512 bytes**. Otherwise null plus cutoff. Under `no_content`, conditions is null and this deliberate policy omission is not capture loss. `condition_key` remains available for filtering. All other metadata projections are identical across modes. Names/labels/declared conditions are untrusted host data; no-content is an output policy, not a confidentiality guarantee for a maliciously chosen name.

Known manifest structure, binding diagnostics and comparison tokens may be inspected internally even in no-content mode. No-content rejects `payload` and `query` before body/SQL access. Metadata exposes artifact refs and retention facts, not raw requests, arguments, results, provider catalogs or raw HMAC values. `schema` describes this policy.

### Original-comparison outcomes

`forwarded_changed` compares a selected decision's `args_digest` with its explicitly associated observed host dispatch `args_digest`; it does not compare guessed MCP operations. `text_equal` compares an answer's explicit observed MCP result `result_text_digest` to its link's `submitted_text_digest`. Read the named `result_event_id`. Require complete observations and supported tokens: value exactly 64 lowercase hex characters, same run scope, matching projection (`args_json_python/1` or `text_blocks_json/1`) and `basis='observed_original'`. Missing/disabled/partial/malformed/incompatible evidence yields null and a specific reason. Complete original tokens remain usable after body retention loss. Do not recompute original HMACs or treat null as false. Differences are facts, not check failures.

## 4. Discovery and explanation

`runs.data={"items":[Run...],"next_cursor":string|null,"has_more":bool}`. Sort `(started_at, run_id)` ascending. Turn discovery uses `(started_at, turn_id)` ascending within the run, with the same data keys. Each item is `{"turn":Turn,"counts":Counts,"flags":Flags}`. Counts are exact retained facts for that turn, independent of emitted detail limits.

`Counts` keys: `events`, `observed_events`, `model_calls`, `model_requests`, `model_responses`, `decisions`, `operations`, `request_members`, `input_answers`, `produced_answers`. Model calls are distinct IDs on authoritative model events or explicitly referenced by same-turn operations. `input_answers` belongs to requests submitted in the selected turn; `produced_answers` belongs to decisions observed there and may occur in later turns. Absence claims additionally need declared completeness/health; counts alone do not establish complete capture.

`Flags` are exactly `tool_error`, `text_mismatch`, `unattributed_operation`, `forwarded_change`. They mean a positive retained witness exists; false means none witnessed, with coverage still returned:

- tool_error: a same-turn operation in `tool_error`, `protocol_error`, `transport_error`, `timeout` or `local_error`. Cancelled/running/incomplete are not this flag.
- text_mismatch: an answer submitted **in this turn** with a valid `text_equal=false` comparison, even when its decision is older.
- unattributed_operation: same-turn `method='tools/call'` and `link_basis='none'`; discovery remains visible but is not this issue.
- forwarded_change: an observed same-turn host dispatch with valid `forwarded_changed=true`.

Filters accept only `condition_key`, `host_outcome`, `capture_state` (complete/partial/unknown) and the four boolean flag names. String filters are nonempty exact values; unset differs from false. Combine with AND. Unknown keys/types are invalid; no matches is an empty success.

Cursors are unpadded base64url JSON version-1 continuation tokens binding operation, resolved recording path (or its SHA-256), selected run, normalized filters, no-content policy, fixed ordering and the last emitted full ordering tuple. Validate scope and strict UTF-8 encodability of both decoded ordering strings before SQLite binding; malformed/mismatched tokens, including inputs over **8,192 ASCII characters**, return `cursor_mismatch/cursor_scope`. The same 8,192-character ceiling applies to every generated token. When a required continuation exceeds it, the reader immediately returns `limit_exceeded/cursor_size` with `data=null`, including creation/recomputation during ordinary byte trimming. It does not shorten the cursor by clipping identities or dropping rows, or report false exhaustion. A terminal page needs no cursor and may retain long IDs if its envelope fits. Output-budget changes never raise this ceiling. Both errors use existing small envelopes and CLI exit 1. Changing row/byte limits remains allowed. Moving/copying a recording requires a new page. No signature/session/mtime binding: tokens are not authority and live insertions can change later pages. One-extra-item lookahead establishes `has_more`; byte-trimmed rows remain available on later pages.

`explain.data` has `turn`, `counts`, `flags`, `calls`, `unassigned_operations`, `detail`. A call summary has `model_call_id`, `request_event_ids`, `response_event_ids`, `decision_event_ids`, `operation_ids`, `complete` (bool|null). Call complete is false if any selected authoritative model request/response/decision reports complete=false; true only when at least one authoritative request and response exist and every such model observation reports complete=true; otherwise null. Missing request/response stays unknown. This is observed call completeness, not proof of run capture completeness. Sort calls by earliest authoritative event or operation start sequence, then ID; unassigned operations by start sequence/ID. A reference with missing request stays visible with uncertainty. Overview emits at most 50 calls; `detail=null`. Unassigned operations are visible, bounded by the same overall byte cap (and 200-item hard cap).

When `model_call_id` is selected, `calls` contains that summary and `detail={"events":[],"operations":[],"links":[],"artifacts":[],"members":[],"input_answers":[],"produced_answers":[],"dispatches":[],"totals":{...}}`:

- Events belong to the selected call or its explicitly attributed operations/dispatches. Links are same-run rows whose source_event_id, target_event_id or witness_event_id is one of those selected events. Including a link does not recursively add its endpoints or witness bodies.
- Operations join by explicit model-call, decision or dispatch identity, deduplicated by operation ID. Include no unrelated sibling operation.
- Members/input_answers belong to this call's submitted request(s). Produced_answers belong to its decision(s), including later requests/turns. This prevents an ambiguous single `answers` array.
- Artifacts are deduplicated metadata for selected event payload/catalog references and their available manifest references, including message handles in emitted answer records; missing referenced rows are diagnostics.
- Dispatch rows are `{decision_event_id,dispatch_event_id,forwarded_changed,comparison_reason}` for explicit `dispatch_source` relationships. Links and decisions without an operation remain visible.
- `totals` maps each array name to its exact pre-output-limit size. The 200 detail-record cap counts every emitted row across these arrays. Select deterministic prefixes in the listed array order; arrays may be truncated but totals remain exact. The selected call's exact totals must finish before success.

Overview's turn counts remain turn-wide even in selected-call mode. Stable record orders are defined in §6. Prior witnesses and deeper dependencies are available through slice; explain need not grow an unbounded recursive narrative.

## 5. Check: fixed integrity and availability facts

`check.data={"run":Run,"status":...,"items":[...],"stats":{...},"required_failures":[],"required_unknowns":[]}`. Emit exactly nine items in the following order. Every item has `id`, `required=true`, `status`, `unit`, `applicable`, `observed`, `unknown`, `failed`, `witnesses`. A witness is `{ "ids":{...}, "reason":... }`; cap ten per item without truncating counts. `applicable = observed + unknown`; failed is a subset of observed. Count units once, even when several defects affect them; fail takes priority over unknown within a unit. Whole-item status is fail if any failed unit, otherwise unknown if any unknown unit, otherwise pass if applicable>0, otherwise not_applicable. Overall status uses that same precedence across required items. `warn` is reserved for future advisory presentation and is not emitted as one of these nine statuses by these checks.

| ID / unit | Exact classification |
|---|---|
| `coverage.model`, `coverage.host`, `coverage.mcp` / declaration | Nonempty configured producer: pass 1/1/0/0. Explicit null, not in expect: not_applicable 0/0/0/0. Expected null: fail 1/1/0/1. Missing/malformed declaration: unknown 1/0/1/0. No event from a configured producer alone is not failure. |
| `capture_health` / run | Known dropped events/writer errors or positive actual-loss reason counters: fail. Open/unclean/incomplete run, incomplete turn coverage, missing health or unexplained partial/unresolved observations without known loss: unknown. Clean ended run, complete ended turns and valid zero-loss diagnostics: pass. |
| `artifact_storage` / artifact | Verify retained UTF-8 length and SHA-256, or valid omission representation. Retention omission is valid storage, not an empty body. Actual contradiction fails; unverifiable metadata is unknown. No original-HMAC recomputation. |
| `request_structure` / authoritative model.request | Available complete valid request_refs/2, every ordered message/template/field reference accounted: pass, including known empty list. Missing referenced artifact/manifest, omitted or partial manifest, direct request without message array or unsupported representation: unknown. Lossless available malformed declared structure: fail. Omitted referenced body preserves known membership; it only prevents content reconstruction. |
| `operation_lifecycle` / operation | Coherent explicit start/terminal identity, kinds/status/sequence and nonnegative duration: pass, including real errors/cancellation. Running or incomplete without a terminal: unknown. Contradictory available witness/timing/state: fail. Lack of model attribution alone does not fail. |
| `links` / link | Correct same-run authoritative witness, relation and pointer/slot shape: pass. Allowed symbolic/unobserved endpoints: unknown. Known wrong kind/order/contradictory shape: fail. Apply the [capture contract](capture.md)'s four relation rules and message_binding/2 first-witness placement; do not invent FKs for symbolic endpoints. |
| `answer_bindings` / authoritative model.request | Complete available association evidence, including explicit no bindings: pass. Unresolved binding, unavailable manifest or omitted association diagnostics: unknown. Contradictory available occurrence: fail. Count requests, not an unknowable missing answer population. Explicit known binding survives digest opt-out or omitted message body. |

Actual-loss counters are `invalid_input`, `invalid_scope`, `unavailable_boundary`, `capacity`, `duplicate_completion`, `prerequisite_lost`, `writer_failed`, `closed`, `turn_update_lost` when positive in persisted health. `partial`, `unresolved`, `other` make health unknown absent a known failure. `body_omitted` alone may be intentional retention and does not fail/unknown health. `finalize_busy`/`finalize_timeout` alone concern portability and do not erase observations. A missing/invalid required reason-count entry is unknown. Known failures still outrank missing diagnostics.

Zero artifact/request/operation/link populations yield not_applicable only when the stored population is establishable; unavailable population cannot be asserted empty. Direct request with explicit direct occurrence links can expose those occurrences, but unavailable full membership remains unknown. For message_binding/2, validate the first witness's handle membership and rebased pointer when its manifest is available. If that manifest alone is unavailable, links reports unknown witness shape; later requests with complete manifests retain their explicit linked occurrences and conclusive current answer-binding units. The unavailable witness request itself is unknown. A known contradictory first-witness membership/pointer, kind, column placement or order invalidates the affected association: expose reader-derived association_state=invalid_binding, null comparison with reason, and failed affected link/request units. The unchanged SQL view is not sufficient to label a disproven binding linked. No stored row is rewritten. Inspect unresolved-answer diagnostics and their omitted-count fields, not only `answer_occurrences` joins. A missing decision must stay visible as `missing_decision`. Digest unavailability alone does not erase binding identity.

Check processing consumes every answer occurrence for complete statistics but retains only its association state in a per-request set. Answer-binding classification applies this first-match order: failed request structure; explicitly failed binding witness; `invalid_binding`; unknown request structure; any other non-linked state; unresolved/omitted association diagnostic; pass. Thus a known contradictory first witness fails even when the manifest population is unavailable and its current answer-occurrence join is empty.

`stats` keys: `turns, events, observed_events, model_calls, model_requests, model_responses, decisions, artifacts, operations, links, findings, request_members, answer_occurrences, linked_answers, unresolved_answers, text_equal, text_mismatch, text_unknown, forwarded_changed, forwarded_equal, forwarded_unknown`. All count stored rows/known observed facts in this run; the three text counts partition observed occurrences, the three forwarded counts partition explicit decision/dispatch pairs. `unresolved_answers` counts non-linked observed occurrence rows, not unexposed blocks. Per-request unknown units/diagnostics disclose missing populations. These counts are complete for retained facts, not proof of complete real-world capture.

All nine items and stats complete inside the check reader's single transaction and deadline. Timeout/storage failure returns `ok=false,data=null`, not partial PASS. Witness/output limits never change aggregates. Checks report recorded integrity and availability; they do not score behavior or write findings.

### Evidence gate

`check` describes integrity and availability; `gate` applies one complete-run policy under explicit host expectations. Doctor's bounded diagnostic page is not an input to this verdict.

```python
from mcp_flightrecorder import inspect

report = inspect.gate(
    "recording.sqlite", run_id=None,
    expect=["model", "host", "mcp"],
    min_activity={"turns": 1, "model_requests": 2, "model_responses": 2,
                  "decisions": 1, "tool_calls": 1, "answer_occurrences": 1},
    answers="all", timeout=10.0, max_output_bytes=65536,
)
```

`expect` must explicitly contain distinct `model` and `host` names; `mcp` is optional. Unlike `check`, missing/empty expectations are invalid. `min_activity` must be a nonempty ordinary dict over the supported count names in the example, with integers from 0 to 2**63−1 excluding bool. At least one **non-turn** minimum must be positive: empty turns alone cannot establish observed activity. CLI equivalents are `--expect model,host,mcp`, `--min-turns`, `--min-model-requests`, `--min-model-responses`, `--min-decisions`, `--min-tool-calls` and `--min-answer-occurrences`. Supply expectations that describe the actual application. Counts are observed facts, not estimates of unrecorded work.

One owned read transaction selects the sole run when `run_id` is omitted, validates coverage/health/integrity and complete captured turns, and visits every relevant answer occurrence and `tools/call`. This includes records beyond display pages and witness samples. `data.expectations` contains boundaries, supplied minima and the selected `answers` scope; `data.actual_activity` contains all six observed counts. A completed evaluation sets `evaluation_complete=true`, independently of displayed witnesses; bounded witnesses are only illustrations. Each invocation has one cooperative deadline and always uses no-content output.

Every submitted answer occurrence must have valid current and original-witness links. When a text comparison is required, compatible original raw text must match, or the exact occurrence must have fully validated declared preparation whose original prepared text matches the submitted text. The same-run endpoints, order, complete declared sources, selected source and original tokens must all be available and consistent. Explicit unresolved attribution or omitted relevant diagnostics cannot be promoted into positive evidence. Preparation preserves raw `text_equal`/`text_mismatch` and does not verify semantic equivalence.

`answers="all"` is the unchanged default. Explicit `answers="mcp"` (CLI `--answers mcp`) excludes only the local result-text comparison of an independently validated, unambiguous local occurrence. An explicit null result alone is insufficient: current/original witnesses, authoritative endpoints, complete relevant local dispatch and absence of competing dispatch/MCP evidence must agree. Malformed result identities produce FAIL (`invalid_result_identity`); missing selected MCP results and shared-decision retry ambiguity remain UNKNOWN. Other occurrence validation, coverage, health, lifecycle, integrity and attribution checks still run over the entire population. Raw `text_equal` and comparison reasons are unchanged; activity minima still count all observed activity.

`excluded_count` counts excluded answer occurrences, including replay. `excluded` and `answer_witnesses` are bounded samples with `excluded_omitted_count` and `answer_witnesses_omitted_count`, not reduced evaluation populations. `mcp_answer_count` counts demonstrably MCP-origin occurrences. Report those exclusions as unverified local comparisons, not successes. Under explicitly selected MCP scope, no demonstrably MCP-origin answer comparisons gives UNKNOWN `no_mcp_answers_in_scope`, unless an independent failure already establishes FAIL. Default-scope model-only behavior is unchanged.

`gate --text` renders the same completed result without reopening the recording. It preserves verdict, chosen scope, exclusions and cutoff notices inside the existing output budget. Event chains, preparation and byte counts appear only when supported; missing facts remain unknown. JSON remains the default. Do not narrow policy or declare preparation merely to obtain PASS.

Each `tools/call` must be attributed through consistent authoritative evidence to its actual model decision or an explicitly declared host operation. A non-null ID or `link_basis` string is insufficient. `tools/list` discovery is exempt from that attribution requirement. A faithfully and completely recorded tool error is not failed merely because the tool returned an error.

The envelope uses `format="mfr.inspect/1"`, `operation="gate"`. A completed evaluation has `ok=true` and `data.status` of `pass`, `fail` or `unknown`, producing CLI exits **0**, **2** or **4** respectively; fail outranks unknown. Operational errors use the existing `ok=false` error envelope and exits **1** or **3**. Timeout or required-output failure cannot return a successful gate. There is no score, policy language or metadata waiver. Missing comparisons and integrity unknowns remain unverified, even when retained bodies were intentionally omitted. This policy checks captured host-integrated evidence; it cannot certify answer quality, semantic equivalence or absence of unobserved calls. MCP-only coverage cannot silently become complete model/host coverage.

See the [complete runnable command](mcp-inspection.md#use-the-evidence-gate-in-ci) for a prepared-answer recording and [MCP inspection](mcp-inspection.md) for bounded agent-facing investigation without a gate tool.

## 6. Slice: submitted-input ancestry

`slice.data={"root_request_event_id":...,"runs":[],"turns":[],"events":[],"operations":[],"links":[],"artifacts":[],"members":[],"answers":[],"complete":bool,"frontier":[]}`. Root must be an authoritative model.request. `answers` uses the Answer projection. `frontier` holds up to 20 `{kind,id,depth,reason}` items; omitted diagnostics are counted. Complete means no unresolved required endpoint/manifest or traversal/output cutoff in the slice; it does not claim full capture or retained content.

`answers` contains occurrences for fully expanded requests only; contextual witness requests contribute members, not a second expansion of their answers. Use deterministic breadth-first traversal of explicit incoming links and IDs, with visited state distinguishing full expansion from witness-only context. A later full encounter may upgrade a contextual visit. Retained run, turn, event and link rows are projected only once per output identity. Every turn visit still checks its retained prompt reference at the current depth. Raw event/link reads, membership, relation classification, context upgrades, frontier diagnostics and traversal continue on every required visit; projection deduplication does not suppress ancestry. Root depth is zero; each followed event/operation association advances one. Artifact/turn/run metadata attached to an included record does not consume extra graph depth. Bound pending work as well as output; a cap must not first materialize an unbounded graph. No inferred name/argument/time edges.

Root answer occurrences lead to their explicit decisions and first witnesses. Decisions/dispatches lead to their explicit operations; operations include lifecycle and their producing model request/response context. Include explicitly prepared context only when an actual incoming insertion relates it to the selected request. Context-only earlier requests contribute their own request/catalog/member metadata but do **not** expand their historical answer operations or prepared-context insertion edges. Do not pull in later model responses/decisions merely because they share the root's model_call_id. The installed [`_inspect_slice.py`](../src/mcp_flightrecorder/_inspect_slice.py) implements this traversal for both public `inspect.slice` and `fr slice`; ordinary pytest cases assert the expected graphs and bounds independently.

All emitted table-like rows count against `limit`: run, turn, event, operation, link, artifact, member and answer rows. Scope and diagnostic/frontier entries are separately bounded. Preserve repeated membership positions; deduplicate artifacts and entity identities. Use stable selection/traversal; returned arrays sort events by sequence/ID, operations by start sequence/ID, links by witness sequence/ID, artifacts by ID, turns by started_at/ID, members by request sequence/index, answers by request sequence/index/target pointer/link ID. Missing referenced ordering sequences sort after known sequences, then by full identity. Root must be included for successful slice; insufficient required output is limit_exceeded. Cutoffs and missing evidence cannot become silently complete slices.

## 7. Payload and arbitrary read-only SQL

Payload data is `{artifact:Artifact, offset:int, returned_bytes:int, total_bytes:int, text:string, next_offset:int|null}`. Explicit payload `text` uses its own content budget, not the 512-byte metadata display cap. Total/offset refer to **retained UTF-8 bytes**, not characters/original observed bytes. Offset must be 0..length and a code-point boundary; inside a multibyte character is invalid_request, not rounded. At length return empty text/next_offset null. Omission is payload_omitted; missing row is unknown_artifact; a retained empty string succeeds. Return whole characters within both content and envelope budgets. If a next character cannot fit, limit_exceeded; otherwise next_offset advances exactly by returned_bytes or is null at end. Truncated/redacted retained content stays labelled; no original reconstruction. For example, `Aé中😀Z` occupies 11 UTF-8 bytes, with valid offsets 0, 1, 3, 6, 10 and 11.

The reader first tests the full payload window against the whole encoded envelope. On overflow, it binary-searches only proper positive character prefixes with diagnostics fixed, preserving the longest fitting prefix and exact continuation bytes. If no prefix fits, it retains one code point before attempting to remove whole diagnostics. Text does not regrow after diagnostic removal. The full-window check is separate because a terminal `next_offset=null` can encode smaller than a proper prefix with an integer continuation.

After the shared timeout check, both Python and the scoped CLI validate artifact ID, offset as a non-bool integer in `0..2**63-1`, then max_bytes before policy, run inference or storage. This order also applies when offset and max_bytes are jointly invalid. Stored length and UTF-8 code-point boundary validation remains after successful artifact retrieval.

Query data is `{columns:[string...], rows:[[JSON_scalar...]], returned_rows:int, has_more:bool}`. Row arrays preserve duplicate column names. Empty results retain columns. Params are None, list/tuple of SQLite scalar bindings or a string-keyed mapping: null, string, bytes, bool/int in signed 64-bit range, finite float. Bytes may be bound but a retained BLOB result is rejected; use explicit `hex` conversion. Bad param shape is invalid_request; SQLite binding mismatch is sql_rejected. SQL must be one SELECT or WITH statement (after optional whitespace/comments); VALUES, EXPLAIN, PRAGMA and transaction controls are excluded. A leading-keyword check and SQLite execution/authorization enforce this boundary without a separate SQL parser.

Trusted setup, schema checks and BEGIN precede query-specific policy. After the leading-keyword check succeeds and before installing the authorizer or preparing caller SQL, the query route sets its owned connection's `SQLITE_LIMIT_LENGTH` to 8,388,608 bytes. This fixed limit applies to SQLite strings, BLOBs and encoded rows, independently of the output envelope and other reader budgets. It is not a caller option. The authorizer allows only SQLite SELECT, READ, FUNCTION and RECURSIVE actions; all other actions, attachments, writes/DDL, extension loading and unlisted functions are denied. Authorization remains active through execution, stepping, reprepare and statement cleanup. Finally blocks close statements, remove the authorizer, finish the read transaction and close the connection. The query path neither calls `executescript` nor registers a UDF. Fixed permitted builtins (case-insensitive):

`abs, avg, count, sum, total, min, max, round, coalesce, ifnull, nullif, length, lower, upper, substr, substring, trim, ltrim, rtrim, replace, instr, printf, format, typeof, hex, quote, unicode, char, like, glob, json, json_valid, json_extract, json_type, json_array_length, json_array, json_object, json_group_array, json_group_object, ->, ->>`.

The shipped views' JSON table access uses the same allowed actions. SQL may read ordinary schema metadata but cannot mutate/attach. Denial and SQL syntax/binding errors are `sql_rejected`; established progress/deadline interruption is `query_timeout`; actual storage failure is `recording_unreadable`. These causes remain distinct.

A native `SQLITE_TOOBIG` (primary code 18) from preparation, binding, execution, stepping or lookahead returns `limit_exceeded`, reason `sql_value_limit`, with the fixed message “The SQL value or encoded row exceeds the 8,388,608-byte query limit.” The error has `ok=false` and `data=null`, with no partial rows or columns, fits 1,024 encoded bytes, and gives CLI exit 1. An expired deadline takes precedence and returns `query_timeout/deadline_expired`. Statement cleanup retains the authorizer until the cursor is closed; the owned read transaction and connection are then released normally.

Depending on the SQLite version and build, an oversized native `printf` or `format` request can return SQL NULL successfully or raise `SQLITE_TOOBIG`. Preserve that `None` / JSON `null` in successful `rows`; it neither proves that the requested string was produced nor creates a limit error or clipping diagnostic. This successful result differs from an error envelope's `data=null`. Other expressions such as `hex`, bindings and stored values can raise the native too-big error. A legitimately retained large artifact may fail even through `length` or `substr`; these functions do not guarantee a bypass. A count or separate small metadata row can remain readable. The cap belongs only to `query`: recording bytes, capture limits, other readers and payload prefix access are unchanged. A small returned payload window does not establish bounded internal payload memory. The SQLite cap does not bound all query allocations, temporary sorting or process RSS and provides no hostile-SQL process isolation.

Retained BLOB or non-finite REAL cells reject the entire query with sql_rejected/reason unsupported_sql_value. No implicit null/base64/tag format. Callers may request hex/typeof/length/substr/printf explicitly. String cells/column display labels have the 512-byte limit and cutoffs; generic aliases do not create semantic ID fields. Inspect only the bounded retained prefix plus lookahead. Has_more is true when a further row or byte-trimmed row is known; never claim an exact total query population. No partial data on timeout/error. Query remains explicit content access and is unavailable under no-content.

## 8. CLI and local snapshot

All options follow the subcommand. The original commands default to exactly one compact JSON value plus newline on stdout, without progress text. `turns --text`, `explain --text` and `gate --text` opt into readable output; `doctor` defaults to text and offers `--json`. Parse failures return invalid_request JSON/exit 1. Explicit help/version may use conventional text. `--acceptance` remains unsupported. `fr mcp` is the optional stdio inspection server described in the [MCP guide](mcp-inspection.md); its stdout is MCP protocol traffic.

Common read options: `--max-output-bytes N`, `--no-content`. `runs`, the scoped readers, gate and doctor additionally accept `--timeout SECONDS` (default 10, positive finite numeric value at most 30). Timeout validation precedes implicit run selection. Run-scoped CLI commands accept `--run ID`; omit it only when actual DB cardinality is exactly one (not when a bounded first page happens to show one). Successful inference and the selected operation use separate read transactions and deadlines; `explain --last` and `doctor --last` add latest-turn selection. Zero/multiple runs yields invalid_request with `fr runs` guidance. Python `diagnose` and `gate` support singleton inference within their own snapshots; the other run-scoped readers take an explicit run_id. CLI gate forwards selection to the same owned transaction without an independent lookup.

| Command | Additional arguments |
|---|---|
| `fr runs DB` | `--limit`, `--cursor`, `--timeout` |
| `fr check DB` | `--run`, `--timeout`, `--expect model,host,mcp` (nonempty CSV, no duplicates) |
| `fr turns DB` | `--run`, `--timeout`, seven §4 filter flags in kebab-case; booleans `true|false`; `--limit`, `--cursor`, `--last`, `--text` |
| `fr explain DB --turn ID` | `--last` instead of `--turn`; `--run`, `--timeout`, `--model-call ID`, `--text` |
| `fr doctor DB` | `--run`, `--timeout`, `--turn ID` or `--last`, `--limit`, `--json` |
| `fr payload DB ARTIFACT_ID` | `--run`, `--timeout`, `--offset`, `--max-bytes` |
| `fr slice DB REQUEST_EVENT_ID` | `--run`, `--timeout`, `--limit`, `--max-depth` |
| `fr sql DB SQL` | `--params JSON` array/object, `--limit`, `--timeout`; no run selector |
| `fr schema` | No DB/options beyond help |
| `fr snapshot DB OUT` | `--timeout` default 10; no run/content/output-budget options |
| `fr gate DB` | `--run`, `--timeout`, `--expect`, named `--min-*` activity flags, `--answers all\|mcp`, `--text`; always no-content |
| `fr mcp DB` | Optional `[mcp]` dependency; `--allow-content` enables explicit payload calls |

The `DB` input of `runs`, `check`, `turns`, `explain`, `doctor`, `payload`, `slice`, `sql`, `gate` and snapshot source also accepts a directory. One shared CLI resolver selects only direct regular non-symlink `.sqlite` children by `(max(database_mtime_ns, wal_mtime_ns), filename)`; SHM does not affect ordering. It resolves once, reports the absolute selection on stderr and within text budgets, and does not fall back if the selected file fails. Use the explicit selected file for follow-ups and CI. Directory browsing does not freeze a live file. Python readers, `fr mcp` and snapshot output retain explicit-file semantics.

For a recording with no turns, plain unfiltered `turns --text` provides a bounded operation view with an MCP-only scope banner and actual operation IDs/times/statuses. Explicit `--run` and `--limit` are supported; `--last`, cursors and turn filters are not silently ignored or reinterpreted. The JSON turns result is unchanged. No host turns, decisions or model-request ancestry are synthesized. Doctor's unattributed-operation guidance and gate output retain that scope limitation.

`python -m mcp_flightrecorder`, `fr` and `mcp-flightrecorder` use the same CLI. CLI output/exit follows Python data/error semantics, with run inference only as described. SQL CLI cannot represent bytes in JSON params; Python may bind them. Stdout JSON has no paths/SQL/content echoed into errors unnecessarily.

Cyclic source or destination symlinks are rejected as `invalid_request/invalid_path`; ordinary nonexistent source recordings remain `recording_unreadable/storage_failure`, while fresh nonexistent destination paths are allowed. This is validation-time classification, not a guarantee against later filesystem changes.

`inspect.snapshot(path, output, *, timeout=10.0)` and `fr snapshot` share one local full-file copy implementation and result envelope. The installed [`_snapshot.py`](../src/mcp_flightrecorder/_snapshot.py) implements it without a source checkout. Open source mode=ro and force an actual read before claiming it is available. Refuse self-copy or existing destination main/WAL/SHM/rollback-journal names (`OUT`, `OUT-wal`, `OUT-shm`, `OUT-journal`), including dangling symlinks. Back up under its cooperative deadline into a **private same-parent temporary staging directory**, where SQLite may create/delete destination backup sidecars. Finalize that staged backup to DELETE, reopen it mode=ro and verify its schema/run states. Prepare the bounded result and close the source **before** publishing. Use a single exclusive `os.link` to publish the verified main file atomically without overwrite. There is no public placeholder during backup and **no cleanup path ever unlinks a public destination pathname**. The backup preserves recorded rows and observed run state, including open/incomplete; it does not intentionally modify source main content or mark capture clean. Never use immutable fallback, change source journal mode or delete source sidecars. SQLite acquisition from a WAL source may create or update source sidecars, including SHM. A read-only source connection does not promise that its fileset remains byte-for-byte untouched. This acquisition behavior differs from the MCP inspector, which requires a stable portable file and refuses WAL/SHM filesets before SQLite access. On failure/interruption remove only the private stage after closing connections; an unremovable private stage yields `output_cleanup_failed`. A competitor arriving before publication is refused without deleting its main or sidecars. Immediately after a successful link, any visibly present destination sidecar yields `recording_unreadable/destination_finalize` rather than a false verified-success response; the public main may already exist and is never unlinked. An uncertain publication likewise preserves every public name. This post-link check is best effort: shared-directory writers can mutate names after the check, and snapshot does not claim an atomic multi-name lock on SQLite suffixes.

Snapshot data is `{output_path:string, journal_mode:"delete", verified:true, source_run_states:[{run_id,close_state}...]}`. `output_path` is the resolved absolute output path, including resolution of symlinked parent directories; it is not necessarily the exact path spelling supplied by the caller. The path is an intact output identity. Derive source_run_states from the verified destination snapshot, not a later reread of the changing source. Use the default envelope limit; if many run states exceed it, omit trailing states with cutoff while still reporting verified copy, not a fabricated total. Destination refusal is invalid_request, timeout query_timeout, storage failure recording_unreadable. recording_open_wal requires established WAL mode and related read failure; journal mode WAL by itself is not an error. If WAL status could not be established, keep generic unreadable. The installed CLI follows the error table above.

`schema.data` is a static dictionary: `api_format`, `database_schema_version=6`, `package_version`, `operations` (function/CLI spelling, required args, optional defaults, helper/local-copy designation), `limits` (table §1; cursor scalar `cursor_chars=8192`), `errors` (code/exit table), `check_statuses` (pass/fail/unknown/not_applicable; warn reserved), `expect_boundaries`, `content_policy`. It does not open a recording. Gate metadata describes its explicit required expectations/activity and status exits, and snapshot names its public Python entry. The additive fixed scalar `limits.query_value_bytes = 8388608` describes the query value/encoded-row policy without a caller option. `limits.scoped_reader_timeout` exposes `{default:10.0,min_exclusive:0,max:30}`; `runs` and the scoped operations expose `timeout:10.0` in their optional defaults. Existing `db_deadline_seconds` and `query_snapshot_timeout` keys remain.

## 9. Implementation map

The public entry points are in [`inspect.py`](../src/mcp_flightrecorder/inspect.py) and [`cli.py`](../src/mcp_flightrecorder/cli.py). Shared envelopes, budgets, validation and read sessions live in [`_inspect_common.py`](../src/mcp_flightrecorder/_inspect_common.py). Record and relation projections are in `_inspect_records.py` and `_inspect_relations.py`; the check, navigation, slice and SQL modules implement their respective operations. [`_snapshot.py`](../src/mcp_flightrecorder/_snapshot.py) owns snapshot acquisition, private staging and exclusive destination publication.

[`sql/schema.sql`](../sql/schema.sql) defines the stored facts. [Architecture](architecture.md) explains writer ownership and identity; [Capture](capture.md) explains the observations those readers interpret; [Testing](testing.md) explains how executable fixtures are generated without stored recordings.


## Operational diagnostics

Reader and snapshot module loggers use the standard `mcp_flightrecorder` namespace described in the [logging contract](capture.md#6-operational-logging). Logs contain fixed operations, error codes and reasons, never selected IDs, paths, SQL, parameters or retained bodies. Read transactions and connections close before ordinary outcome logging. Snapshot output cleanup failures are reported after cleanup attempts, including during interruption. Library defaults preserve the CLI's single JSON stdout result and exit codes; applications own any logging destinations.

Comparison output preserves current producer reasons such as `invalid_raw_arguments`, `conflicting_arguments` and `text_paths_unavailable`. These explain unavailable comparisons and do not establish equality or provenance. Unrecognized reasons continue to use the safe `digest_unavailable` fallback.
