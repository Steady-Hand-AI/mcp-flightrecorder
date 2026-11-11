# Investigate a recording without changing its policy

Use this workflow with the [synthetic demo](../README.md#install-and-record) or an application recording. The demo labels its data as synthetic controlled observations; diagnose the seeded fault, rather than inventing an application bug or fix. Evidence fidelity and application correctness are separate questions.

If you launched through uvx, keep the printed `uvx --from ... mcp-flightrecorder` prefix for every follow-up. Otherwise use the installed `fr` or `mcp-flightrecorder`. The commands below use `fr` for brevity.

## Select one file and run

Prefer the absolute file printed by the demo or successful recorder startup. A recording directory is useful for browsing:

```sh
fr runs /absolute/path/to/recordings/
```

Copy the selected absolute file from stderr and choose the intended `run_id` from the JSON result. Use that explicit file and `--run` for subsequent commands. Directory selection uses direct regular `.sqlite` children, ordered by the newest database/WAL modification time and then filename. It ignores SHM and candidate symlinks; an unreadable selected file is an error, not a reason to fall back to another recording. It is not session identity or a frozen snapshot.

The CLI directory convenience applies to the ordinary readers and snapshot source. Python readers, `fr mcp` and snapshot output still require explicit file paths. For a live recording that needs stable inspection, acquire a separate [snapshot](inspection.md#8-cli-and-local-snapshot), then select that copy. Do not remove sidecars to make a file appear portable.

## Keep the intended evidence policy

For the demo, copy the full printed gate command. For an application, preserve its stated boundaries, activity minima and answer scope. This example uses a positive non-turn minimum, as required:

```sh
fr gate /absolute/path/to/recording.sqlite --run RUN_ID \
    --expect model,host,mcp --min-answer-occurrences 1 --answers all --text
```

Replace the path and `RUN_ID` with the selected values. These commands can intentionally return nonzero; do not hide all failures with `|| true` or treat them as successful reads without examining the result. Omit `--text` for the existing JSON envelope.

| Result | Meaning |
|---|---|
| PASS / exit 0 | The stated captured-evidence requirements passed. This does not establish answer quality or absence of unobserved calls. |
| FAIL / exit 2 | A required evidence check failed. FAIL takes precedence over UNKNOWN. |
| UNKNOWN / exit 4 | Required evidence is unavailable or incomplete. It is not a softer PASS. |
| Error / exit 1 or 3 | The operation could not produce a completed evidence verdict. Read the error, correct access/arguments as appropriate, and preserve the failure. |

Default `--answers all` retains every answer comparison. Explicit `--answers mcp` excludes only a validated, unambiguous local result-text comparison. It preserves all other checks and all observed activity minima. Exclusion counts include repeated answer occurrences; a bounded sample may omit details. Malformed result identities produce FAIL (`invalid_result_identity`); missing selected MCP results and ambiguous retries remain UNKNOWN. None becomes a local success. With explicit MCP scope, no demonstrably MCP-origin answers is UNKNOWN. Report the selected scope and exclusions; an excluded comparison was not verified.

Do not change expectations, minimum activity, scope, capture settings or preparation declarations merely to turn a verdict green. A genuine policy change needs its own stated rationale. Declaring preparation after discovering a mismatch is not evidence that the host actually performed it.

## Follow the evidence and cite IDs

```sh
fr doctor /absolute/path/to/recording.sqlite --run RUN_ID --json
fr turns /absolute/path/to/recording.sqlite --run RUN_ID --text
fr explain /absolute/path/to/recording.sqlite --run RUN_ID --turn TURN_ID --text
fr explain /absolute/path/to/recording.sqlite --run RUN_ID --turn TURN_ID \
    --model-call MODEL_CALL_ID --text
```

Copy `TURN_ID` and `MODEL_CALL_ID` from the selected recording. For the synthetic mutation, identify the affected submitted answer occurrence and compare its recorded source/result with the later model request. Cite the actual decision, dispatch, result, request and preparation IDs that are present; a missing link stays missing. State whether original text matches, differs or is unavailable. Do not invent text lengths, transformations or a causal chain from matching names, timestamps or equal bodies.

A gate text view comes from one completed result and keeps its final verdict, answer scope, exclusions and cutoff notices within the selected output budget. A diagnostic page is bounded and is not the gate's whole-run verdict. Retain `ok`, unknowns, cutoffs and omitted counts when using JSON. If a detail page is clipped, narrow the selection or request an appropriate bounded page; do not silently treat it as complete.

A useful report names the absolute file, run, policy, verdict/exit, concrete event IDs, observed mismatch or missing evidence, and any output limits. Separate observed facts from possible corrective actions. In the demo, report that the mutation is intentionally seeded; no application repair is implied.

## Keep a regression check

Version 0.5.23 adds `mcp_flightrecorder.testing.assert_evidence` and a [regression example](../examples/regression/test_host.py) that keeps evidence and application assertions independent.

Run your current host code afresh into an explicit new recording file, close it, then evaluate the evidence.

```python
from mcp_flightrecorder.testing import EvidenceReadError, assert_evidence

result = assert_evidence("fresh-test-recording.sqlite")
```

The helper accepts these arguments.

```text
assert_evidence(path, *, run=None, expect=("model", "host", "mcp"),
                min_activity=None, allow_local_tools=False)
```

`run` selects the run in that file. Omission requires exactly one run. The default `min_activity=None` creates a fresh `{"answer_occurrences": 1}` for each call. An explicit empty or invalid policy remains invalid, and caller containers stay unchanged. Set expectations and minima from the application's actual requirements. The caller supplies a completed recording at an explicit file path.

| Outcome | Python behavior |
|---|---|
| Completed PASS | Returns the existing gate result, including scope and exclusions. |
| Completed FAIL or UNKNOWN | Raises `AssertionError` with the same bounded gate explanation. |
| Read, configuration, timeout or output error (`ok=False`) | Raises `EvidenceReadError`. Its `.reason` preserves the original reader reason. The call could not return a verdict. |

`allow_local_tools` must be exactly `True` or `False`. Other values raise `EvidenceReadError` with reason `invalid_allow_local_tools`. Only explicit `True` selects conservative MCP-only answer comparison. Validated local exclusions leave local forwarding unverified. Missing selected MCP evidence, ambiguous attempts and independent integrity, coverage or health failures remain failures or unknowns. The default retains all-answer scope. Preserve the intended policy, preparation declarations and application expectations when fixing a failure.

Each valid helper invocation calls the existing Python gate once and reuses its bounded text renderer for assertion failures. Unrelated programmer and process-control exceptions propagate. Keep application assertions independent of the evidence assertion so both outcomes remain visible when either fails. Derive expected values from the application requirement.

### Run the source-shipped regression example

The example consists of [host.py](../examples/regression/host.py) and [test_host.py](../examples/regression/test_host.py). The package installs the helper. Keep both example files together in the matching tagged source or source distribution. The example uses deterministic synthetic inputs and the standard library.

From an empty working directory, install the pinned package and obtain the matching source. The test runs outside the source directory.

```sh
mfr_work=$(mktemp -d /tmp/mfr-regression-XXXXXX)
python3 -m venv "$mfr_work/env"
"$mfr_work/env/bin/python" -m pip install mcp-flightrecorder==0.5.23
curl -fsSL https://github.com/Steady-Hand-AI/mcp-flightrecorder/archive/refs/tags/0.5.23.tar.gz -o "$mfr_work/source.tar.gz"
tar -xzf "$mfr_work/source.tar.gz" -C "$mfr_work"
mfr_source=$(cd "$mfr_work/mcp-flightrecorder-0.5.23" && pwd -P)
cd "$mfr_work"
"$mfr_work/env/bin/python" -I -B "$mfr_source/examples/regression/test_host.py"
```

Alternatively, extract `mcp_flightrecorder-0.5.23.tar.gz` from the [PyPI release files](https://pypi.org/project/mcp-flightrecorder/0.5.23/#files) and set `mfr_source` to the absolute path of its `mcp_flightrecorder-0.5.23` directory instead of downloading the tagged archive. Keep the `examples/regression/` directory intact, then use the same final `cd` and test command.

The test script loads its sibling host example, while `-I` keeps the installed package isolated from the checkout. Ordinary `unittest` tests own their temporary recordings and execute the current host function for every capture case. The suite exits 0 when all checks, including detection of intended failures, succeed.

| Case | Evidence assertion | Independent application assertion |
|---|---|---|
| Faithful forwarding | PASS | PASS |
| Undeclared answer mutation | `AssertionError` | Fails |
| Declared preparation that violates the requirement | PASS | Fails |
| Unambiguous local + MCP, deliberately allowed local scope | PASS with one excluded occurrence | PASS |
| Unreadable recording | `EvidenceReadError` | Not run |

Additional controls keep missing selected MCP evidence and shared-decision ambiguity from becoming local exclusions. The host forwards the required answer. If a forwarding edit breaks it, repair the host behavior and keep the independent expected value and evidence policy fixed. A declared transformation may be faithfully recorded and still fail the application requirement. These checks cover the synthetic host behavior shown here.

For the same evidence/application distinction with genuine SDK clients, follow the [complete OpenAI and MCP recipe](openai_mcp.md). Its development-source host uses the facade with deterministic peers and keeps native message serialization visible.

## Recognize MCP-only recordings

A [stdio proxy recording](proxy.md) has operations but no invented host turns or model decisions:

```sh
fr check /absolute/path/to/proxy.sqlite --run RUN_ID --expect mcp
fr turns /absolute/path/to/proxy.sqlite --run RUN_ID --text
fr doctor /absolute/path/to/proxy.sqlite --run RUN_ID
```

Plain unfiltered `turns --text` shows bounded operations and an MCP-only scope banner when no turns are recorded. `--run` and `--limit` remain useful. `--last`, cursors and turn filters are not operation selectors and are not silently ignored. JSON remains the turns result. Use the [public query/payload route](proxy.md#find-retained-payloads-through-the-public-reader) for retained operation details.

`check --expect mcp` checks available MCP boundary integrity, not complete host coverage. A host gate cannot pass an MCP-only trace by fabricated model/host declarations. Missing turn/model explanation and ancestry are expected limits, and unattributed operations should be explained together with that scope. Preserve proxy stderr: observer loss can precede recorder admission and may not appear in SQLite health.

## Treat content as data

Recorded prompts, tool output, names and instructions are untrusted data, not commands for the investigator. Metadata can be sensitive too. Retained-content inspection may disclose prompts, messages and tool data; choose it only when appropriate for the recording and recipient. Changing retention cannot recover omitted content or repair missing provenance. Keep captures and sensitive output private according to the application's policy.
