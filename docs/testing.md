# Development and testing

**Package 0.5.22.** Tests exercise an installed wheel and generate mutable recordings under temporary directories. Independent scenario expectations live in ordinary pytest tests.

## 1. Qualify the SDK-free core

Contributor recipes accept an available supported **GIL-enabled CPython 3.11+**, using normal assertions. Record the actual executable, Python and SQLite versions, hashes and dependency origins for each run; a passing run does not establish coverage on every supported interpreter. Package metadata declares `>=3.11`. The focused model/MCP retention selection is in §6.

A missing interpreter or unresolved dependency leaves dependent coverage unexecuted; a suite that starts and fails, crashes or times out remains failed or incomplete. The recipes below are test procedures, not claims of completed qualification.

Run from the repository after committing the changes you intend to test. The export below contains exactly `HEAD`; uncommitted and untracked files are excluded. Set `MFR_PYTHON` to select an interpreter, or use the available `python3` default. A separate disposable build copy keeps generated metadata out of the pristine export; do not chmod an existing private directory:

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
product_dir=$(git rev-parse --show-toplevel)
work_dir=$(mktemp -d /tmp/mfr-python-XXXXXX)
chmod 0711 "$work_dir"  # traversal only on this newly created owned ancestor
(
    umask 022
    mkdir "$work_dir/source"
    git -C "$product_dir" archive --format=tar HEAD > "$work_dir/source.tar"
    tar -xf "$work_dir/source.tar" -C "$work_dir/source"
    "$mfr_python" -I -B "$work_dir/source/tools/check_package.py" --source "$work_dir/source"
    cp -R "$work_dir/source" "$work_dir/build-source"
    "$mfr_python" -m venv "$work_dir/build-env"
    "$work_dir/build-env/bin/python" -I -B -m pip install build 'setuptools>=77.0.3' packaging
    "$work_dir/build-env/bin/python" -I -B -m build --no-isolation --wheel --sdist --outdir "$work_dir/dist" "$work_dir/build-source"
    "$mfr_python" -I -B "$work_dir/source/tools/check_package.py" --source "$work_dir/source" \
        --wheel "$work_dir/dist/mcp_flightrecorder-0.5.22-py3-none-any.whl" \
        --sdist "$work_dir/dist/mcp_flightrecorder-0.5.22.tar.gz" \
        --report "$work_dir/package-check.json"
    "$mfr_python" -m venv "$work_dir/core-env"
    "$work_dir/core-env/bin/python" -m pip install "$work_dir/dist/mcp_flightrecorder-0.5.22-py3-none-any.whl"
    "$work_dir/core-env/bin/python" -m pip install -r "$work_dir/source/requirements/test.txt"
)
"$work_dir/core-env/bin/python" -m pip check
"$work_dir/core-env/bin/fr" --version
"$work_dir/core-env/bin/python" -I -B - <<'PYTHON'
from pathlib import Path
import mcp_flightrecorder as m, sys
assert Path(m.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
print(m.__version__, m.__file__)
PYTHON
cd "$work_dir/source"

```

Run the following **25-module SDK-free suite** after the build and installation above. It includes the facade, process/lifecycle ownership and diagnostic-reader, complete-run gate and public snapshot API tests. Six other inspection modules use the real OpenAI/MCP-backed integration recording fixture and run with the native adapter, facade and roundtrip tests in the **separate integration environment** in §2. These full selections match `tools/ci.sh`; do not skip modules or substitute mocked SDKs.

```sh
env -u PYTHONPATH -u PYTHONHOME -u PYTHONOPTIMIZE -u PYTEST_DISABLE_PLUGIN_AUTOLOAD -u PYTEST_ADDOPTS PYTHONDONTWRITEBYTECODE=1 TMPDIR="$work_dir" \
    "$work_dir/core-env/bin/python" -I -B -m pytest -n 4 -p no:cacheprovider \
    tests/test_admission.py tests/test_async_shutdown.py tests/test_bindings.py \
    tests/test_capture_values.py tests/test_comparisons.py tests/test_contract.py \
    tests/test_core.py tests/test_deferred.py tests/test_duplicate_transition.py \
    tests/test_easy.py tests/test_lifecycle_ownership.py tests/test_inspection_diagnostics.py \
    tests/test_failures.py tests/test_finalization.py tests/test_fixture.py \
    tests/test_inspection_logging.py tests/test_inspection_query.py \
    tests/test_inspection_snapshot.py tests/test_inspection_snapshot_api.py \
    tests/test_inspection_gate.py tests/test_logging.py tests/test_proxy.py \
    tests/test_scope_validation.py tests/test_shutdown.py tests/test_scale_regressions.py
```

Check that `mcp_flightrecorder.__file__` points to the installed venv, never `src/`. The SDK-specific adapter/fixture modules are `test_integration_fixture.py`, `test_integration_history.py`, `test_mcp_client.py` and `test_openai_chat.py`; the integration selection also includes `test_easy_sdk.py`, `test_tool_roundtrip.py` the genuine stdio client/server `test_mcp_inspection.py`, and `test_proxy_sdk.py`. Collection alone does not qualify them.

The new disposable root's mode `0711` and `umask 022` allow the ordinary-user snapshot-media subprocess to traverse the newly created environment. Its chosen Python executable and all ancestors must also be executable by the ordinary user. A root-run permission case must be permitted to drop UID/GID; otherwise its explicit capability skip records a limitation, **not** a passing case. Never loosen the media-directory permissions that the case is testing, chmod an existing private virtualenv, or convert import/snapshot failures into skips. Use the per-command `TMPDIR` so its media fixture lives below a traversable parent.

The SDK-free quickstart can be exercised from outside the exported source with the installed CLI and a disposable destination:

```sh
recording_dir=$(mktemp -d)
(
    cd "$recording_dir"
    "$work_dir/core-env/bin/python" -I -B "$work_dir/source/examples/quickstart.py" "$recording_dir/quickstart.sqlite"
    "$work_dir/core-env/bin/fr" turns "$recording_dir/quickstart.sqlite"
    "$work_dir/core-env/bin/fr" snapshot "$recording_dir/quickstart.sqlite" "$recording_dir/copy.sqlite"
    "$work_dir/core-env/bin/fr" runs "$recording_dir/copy.sqlite"
)
```

Snapshot checks require the copied main database to be independently readable, portable and in SQLite DELETE mode. The closed portable quickstart source must remain byte-identical. Live acquisition can interact with source SQLite sidecars, including SHM; it does not promise byte-identical live sidecars. A partial quickstart may have unknown coverage; a portable snapshot does not invent complete observations.

## 2. SDK-specific main tests and optional compatibility environments

The **14-module integration selection** runs in a separate fresh environment created with the same selected interpreter. It comprises four adapter/fixture modules, native facade tests, the complete tool-roundtrip example tests, the MCP stdio inspector, the genuine SDK proxy comparisons, and six SDK-backed inspection modules. Install the same wheel first, then `requirements/test.txt` together with `requirements/integration.txt` (direct dependencies, resolved by pip):

```sh
"$mfr_python" -m venv "$work_dir/integration-env"
"$work_dir/integration-env/bin/python" -m pip install "$work_dir/dist/mcp_flightrecorder-0.5.22-py3-none-any.whl"
"$work_dir/integration-env/bin/python" -m pip install -r requirements/test.txt -r requirements/integration.txt
"$work_dir/integration-env/bin/python" -m pip check
env -u PYTHONPATH -u PYTHONHOME -u PYTHONOPTIMIZE -u PYTEST_DISABLE_PLUGIN_AUTOLOAD -u PYTEST_ADDOPTS PYTHONDONTWRITEBYTECODE=1 TMPDIR="$work_dir" \
    "$work_dir/integration-env/bin/python" -I -B -m pytest -n 4 -p no:cacheprovider \
    tests/test_integration_fixture.py tests/test_integration_history.py \
    tests/test_tool_roundtrip.py tests/test_mcp_inspection.py \
    tests/test_easy_sdk.py tests/test_proxy_sdk.py \
    tests/test_mcp_client.py tests/test_openai_chat.py \
    tests/test_inspection_checks.py tests/test_inspection_commands.py \
    tests/test_inspection_content.py tests/test_inspection_inputs.py \
    tests/test_inspection_navigation.py tests/test_inspection_slice.py
```

Each optional compatibility profile uses a **separate fresh venv** created with the same selected interpreter, the built wheel first, and *only* its own requirements file. These direct requirements each include `-r test.txt`; do not add the main integration profile or merge incompatible legacy and modern MCP API families.

| Profile | Install requirements | Execute actual tests |
|---|---|---|
| Native / AnyLLM | `requirements/native.txt` | `compatibility_tests/native` |
| Pydantic recipes | `requirements/recipes.txt` | `compatibility_tests/recipes` |
| Fast-agent | `requirements/fast-agent.txt` | `compatibility_tests/fast_agent` |
| FastMCP released API | `requirements/fastmcp.txt` | `compatibility_tests/fastmcp` |
| Legacy MCP 1.x | `requirements/legacy.txt` | `compatibility_tests/legacy` |

For example, run legacy alone with a new environment:

```sh
"$mfr_python" -m venv "$work_dir/legacy-env"
"$work_dir/legacy-env/bin/python" -m pip install "$work_dir/dist/mcp_flightrecorder-0.5.22-py3-none-any.whl"
"$work_dir/legacy-env/bin/python" -m pip install -r requirements/legacy.txt
"$work_dir/legacy-env/bin/python" -m pip check
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$work_dir" \
    "$work_dir/legacy-env/bin/python" -m pytest -n 4 -p no:cacheprovider compatibility_tests/legacy
```

For another profile, use a new venv and substitute the corresponding requirements and test directory. Dependency resolution uses current released distributions, **not** an exact FastMCP GitHub archive. Lower SDK bounds are known exercised baselines, not a guarantee that the latest host APIs remain compatible. If an optional SDK cannot install or its actual native tests fail, record the resolved SDK versions and the failure; do not pin a replacement silently. Optional failure does not invalidate a separately passing SDK-free core, nor count as an SDK success.

The main `run_sdk_async` watchdog defaults to 10 seconds, queued async shutdown waits use two seconds, and executing cancellation uses four. Native/recipe/legacy fixtures use 30 seconds; fast-agent uses 45; FastMCP uses a 30-second default for imported session-contract tests and 45 seconds for its four attachment scenarios. The integration producer has a 40-second coroutine watchdog and a 50-second parent subprocess deadline; the legacy host has a 50-second coroutine watchdog, while its long-lived peer terminates under host control. Shared fixtures and producer entrypoints use standard asyncio. Explicit caller timeouts still apply. Test data, peer IPC records and logs are generated in disposable directories.

## 3. Independent scenario assertions

[`tests/producers/record_core.py`](../tests/producers/record_core.py) produces the core scenario through the installed public API. [`tests/producers/minimal_host.py`](../tests/producers/minimal_host.py) produces the SDK integration scenario. The seven small files under `tests/fixtures/integration/` are declarative inputs and independent expectations.

[`test_fixture.py`](../tests/test_fixture.py) checks the core timeline, associations and totals. [`test_integration_fixture.py`](../tests/test_integration_fixture.py) checks all ten integration scenario groups, including native host observations, exact links/occurrences, retained-body integrity, request reconstruction and canonical SQL results. The acceptance tests execute copied test producers in isolated subprocesses, copy only stopped main databases, and verify that fresh read-only checks leave database contents and directory entries unchanged. Parsed summaries stay in memory. The integration native/no-recorder comparison and existing-file refusal remain separate tests. `test_quickstart_runs_from_outside_the_checkout` also checks that the small public example runs against the installed package and writes one event.

[`fixture_checks.py`](../tests/fixture_checks.py) contains six plain shared functions for typed JSON equality, retained bodies, decoded pointer replacement, request reconstruction, a named canonical SQL query and token assertions. Nested booleans, integers and floats remain distinct. Expected values come from literals or the declarative inputs; artifact-integrity and before/after database hashing remain independently checked.

Run these fixture cases through pytest:

```sh
PYTHONDONTWRITEBYTECODE=1 "$work_dir/core-env/bin/python" -m pytest -p no:cacheprovider tests/test_fixture.py
PYTHONDONTWRITEBYTECODE=1 "$work_dir/integration-env/bin/python" -m pytest -p no:cacheprovider tests/test_integration_fixture.py
```

[`test_tool_roundtrip.py`](../tests/test_tool_roundtrip.py) runs the source-shipped [complete example](tool_roundtrip.md) from outside the source directory against the installed package. Independent transport and tool observations specify its two model requests and single selected tool call. Capture-off, healthy capture and a controlled rejected terminal observation must preserve the same host work. The tests check explicit answer/source/placement evidence, the documented CLI investigation, destination refusal, and host-owned exceptions/cancellation. They do not require a live service or add SDK imports to the core quickstart.

The installed inspection contract is exercised by `test_inspection_commands.py` for API/CLI envelopes and parity; navigation tests for schema/discovery/paging/associations; checks for integrity; slice tests for graph ancestry/caps; content/query tests for payload/SQL/errors; and snapshot tests for portable copying and path/permission failures. `test_inspection_snapshot_api.py` checks the shared public Python/CLI entry. `test_inspection_gate.py` checks explicit coverage/activity, full-population attribution/comparisons and conservative fail/unknown behavior. `test_mcp_inspection.py` launches the installed subprocess through the genuine SDK and checks discovery, content opt-in, portable input, bounds and useful errors. The [public MCP walkthrough](mcp-inspection.md) exercises the source-shipped client against an installed package.

For manual exploration, use the quickstart and read-only CLI with a new temporary destination (a coverage check on this partial trace is expected to be `unknown`):

```sh
recording_dir=$(mktemp -d)
"$work_dir/core-env/bin/python" -I -B examples/quickstart.py "$recording_dir/quickstart.sqlite"
"$work_dir/core-env/bin/fr" turns "$recording_dir/quickstart.sqlite"
"$work_dir/core-env/bin/fr" snapshot "$recording_dir/quickstart.sqlite" "$recording_dir/copy.sqlite"
```

[`test_scale_regressions.py`](../tests/test_scale_regressions.py) adds SDK-independent checks for NULL-safe closure, per-capture connection ownership/fresh statements, snapshot-cache bounds and eviction, occurrence-specific comparison/diagnostic parity, first-witness authority and five timeout API/CLI routes. Focused cases verify that counting members preserves selection, order, deadlines and display diagnostics without building unused projections, and that link validation runs before operation lookups can evict cached membership facts. Further cases compare answer totals, mismatch flags and repeated diagnostics across both complete answer passes without discarded public records, and verify that current-request validation is reused only for the exact run and witness without adding cache retention. The member-guard cases compare complete typed occurrence rows and diagnostics against the original SQL for malformed/omitted bodies, scalar path types, selectors, limits and damaged duplicate artifacts. Invalid/nonmanifest-body cases also compare complete public check/find_turns envelopes. A four-request selector regression checks exact ordered request/decision/result identities and pointers, complete typed rows and diagnostics for list, one-shot generator and repeated decision IDs, empty/nonexistent selections, and combined request/decision selectors with LIMIT. A companion native-Context regression lowers SQLite's variable limit to eight and checks one run ID plus two request IDs plus four decision IDs plus LIMIT with list and generator selectors, along with an empty decision selector; only this regression records a capability limitation when the runtime lacks the limit API. The admission batching regression verifies ready FIFO drain after deadline expiry, separate item/byte caps and exact committed event order; existing sibling cases retain reservation, rollback and stop/drain coverage. Barrier-controlled tests verify commit visibility and cleanup without sleep-based races. Machine timings are not unit-test assertions; cache accounting and process RSS are separate measurements.

[`test_capture_values.py`](../tests/test_capture_values.py) extends public request-path coverage from a dict root to a list root with shared ancestors and escaped tokens. Literal expected template, field and message bytes check key order, scalar types, masks, hashes, lengths, catalog identity and ordered membership; equal originals with complete empty bindings share one handle while retaining both occurrences. The existing writer-barrier case also submits a distinct retained request, checks both caller trees are unchanged by capture, mutates originals, retained descendants, policy and annotations before releasing the writer in `finally`, and verifies pre-mutation durable bytes, original observed sizes, policy metadata, membership, completeness and health after clean portable close. Distinct original messages remain separate handles even when their retained bodies are equal.

The forty-request sharing case in [`test_bindings.py`](../tests/test_bindings.py) requires loss-free request receipts and waits on the existing recorder condition after each complete producer round until ordinary slots and pending identities drain. Its five-second bound checks fixture liveness, with no sleep, queue-limit change or throughput assertion. Clean, stopped, portable close is required; all original expectations remain: 40 requests, 158 events, 78 answer links, 2,340 members, 1,560 linked answer occurrences, per-request history sizes and the stored-byte sharing bound.

Capture-history tests use public Recorder observations and independent SQLite assertions. Slice and snapshot tests exercise the installed Python API and `fr` command. Compatibility tests retain the six JSON files actually consumed across test/subprocess boundaries, plus peer/server JSONL and SQLite fixtures; all such output stays in temporary directories.

## 4. Source-package hygiene

The source package contains current code, meaningful tests, the seven declarative JSON inputs, examples, requirements, build configuration, package checks and documentation. Generated SQLite recordings/sidecars, run summaries, logs, test reports, coverage output, screenshots, environments, caches, wheels, benchmark data and nested release archives are excluded.

[`tools/public_files.txt`](../tools/public_files.txt) is the explicit source inventory. [`tools/check_package.py`](../tools/check_package.py) rejects missing or unexpected source and archive members, unsafe archive paths, private file classes and recognizable credential or personal-path content. It checks wheel/sdist metadata, canonical project links, license/attribution files and the installed schema. Content refusals do not print matched values. This is a bounded package check, not a guarantee that arbitrary sensitive text can be recognized. When adding an intended public file, review the inventory and build manifest together; `.gitignore` does not enforce the package boundary.

The command in §1 checks the pristine export and actual distributions. [`tools/test_package_guard.py`](../tools/test_package_guard.py) exercises valid built artifacts and deliberately invalid cases generated in temporary directories outside source control:

```sh
MFR_GUARD_SOURCE="$work_dir/source" \
MFR_GUARD_WHEEL="$work_dir/dist/mcp_flightrecorder-0.5.22-py3-none-any.whl" \
MFR_GUARD_SDIST="$work_dir/dist/mcp_flightrecorder-0.5.22.tar.gz" \
    "$mfr_python" -I -B tools/test_package_guard.py
```

The [GitHub Actions workflow](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/.github/workflows/ci.yml) is configured to run on `push` and `pull_request` with read-only repository permissions. Linux SDK-free jobs use CPython 3.11 and 3.14; a separate CPython 3.14 job installs the declared main integration requirements. All selected pytest suites use four workers with normal assertions. The jobs export the checked-out commit, build wheel and sdist in an external copy, run package checks, install the wheel normally, and verify its import origin. They record Python, SQLite and resolved dependency versions, run `pip check`, and exercise `fr --version`, quickstart, inspection and snapshot from outside the source tree. The core 3.11 job additionally rebuilds a wheel from a fresh sdist extraction and smoke-tests that installation.

To run one complete CI lane locally from the repository, choose a supported interpreter and a disposable work parent outside the checkout:

```sh
MFR_PYTHON=python3.11 MFR_CI_PARENT=/tmp bash tools/ci.sh core
MFR_PYTHON=python3.14 MFR_CI_PARENT=/tmp bash tools/ci.sh integration
```

The helper exports committed `HEAD` and leaves its temporary work directory for inspection. It does not include uncommitted changes. The automated smoke helper asserts installed package origin, schema availability, successful JSON inspection, source preservation and a readable DELETE-mode snapshot.

The workflow contains no deployment or upload step. Optional compatibility profiles in §2 are separate from this selected CI matrix. Consult actual Actions runs for observed Linux outcomes; configured jobs and local checks are not evidence of a hosted pass. Version control retains project history; source packages contain the current implementation and public documentation.


Release preparation uses a separate manual workflow. Its focused checks cover exact source/CI identity, the four-file candidate bundle, strict Twine validation, original/rebuilt wheel parity, registry conflict and resume decisions, fresh downloaded-wheel installation and downstream ordering. Service responses are synthetic in local tests; no registry publication is implied. The [release guide](releasing.md) documents setup, operator commands and live-stage verification.

## 5. Operational logging regressions

The main suite exercises standard logging through actual Recorder, adapter, reader and CLI behavior. `test_logging.py` covers default silence/root-configuration ownership, concurrent bounded capacity warnings, content omission versus metadata policy, and failed application handlers. Existing writer/finalization cases also verify incomplete-closure warnings and that public portability is not logged before the retry owner exits. OpenAI adapter cases preserve exact native returns, exceptions, cancellation and consumed chunks with capture/logging failures. `test_inspection_logging.py` covers safe fields, deadline precedence, released connections, final snapshot cleanup outcomes and producer-to-reader comparison reasons.

These cases collect log records in memory or use temporary subprocess output. They do not check in emitted logs or assert private logger method call counts. Slow handler destinations remain an application concern; tests use bounded handlers and existing asynchronous watchdogs.

## 6. Focused selective-retention tests

Use two profiles with one normally installed wheel: an SDK-free core and a combined MCP/OpenAI/AnyLLM/Ollama environment. The latter uses the integration requirements plus AnyLLM, Ollama and httpx. MCP Client/session checks remain in this selection because they share the retention-policy guard with model adapters. The full main integration suite and separate optional host profiles are documented in §2.

After committing source, test or document changes, export that commit into a new disposable directory, then build, install and test that wheel. Keep environments, build copies, JUnit and temporary peer/recording data outside the checkout. Reuse the supported GIL-enabled CPython selected and checked in §1, recording its actual Python and SQLite versions. Only a newly created owned work ancestor gets traversal permission for the existing unprivileged tests; their fixture/database permissions remain unchanged.

After the fresh external build and core setup in §1, create the one current SDK profile:

```sh
"$mfr_python" -I -B -m venv "$work_dir/current-env"
"$work_dir/current-env/bin/python" -I -B -m pip install -r requirements/test.txt -r requirements/integration.txt 'any-llm-sdk[ollama]>=1.29.0' 'ollama>=0.6.3' 'httpx>=0.28.1'
"$work_dir/core-env/bin/python" -I -B -m pip install --force-reinstall --no-deps "$work_dir/dist/mcp_flightrecorder-0.5.22-py3-none-any.whl"
"$work_dir/current-env/bin/python" -I -B -m pip install --force-reinstall --no-deps "$work_dir/dist/mcp_flightrecorder-0.5.22-py3-none-any.whl"
"$work_dir/core-env/bin/python" -I -B -m pip check
"$work_dir/current-env/bin/python" -I -B -m pip check
mkdir -p "$work_dir/results/tmp"
"$work_dir/current-env/bin/python" -I -B -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); s.close()'
```

The following focused selection runs 21 core modules and seven SDK modules from the exported source directory, sequentially with four workers. It covers retention and shared lifecycle behavior; it does not replace the full 25-module core suite in §1 or the 14-module main integration suite in §2. Both commands use normal assertions and the installed pytest/xdist/pytest-check plugins' ordinary autoload. Unset `PYTEST_DISABLE_PLUGIN_AUTOLOAD`; do not also load pytest-check under a second plugin name. Clear `PYTEST_ADDOPTS` so inherited selection, plugin or worker options cannot alter the selection.

```sh
env -u PYTHONPATH -u PYTHONHOME -u PYTHONOPTIMIZE -u PYTEST_DISABLE_PLUGIN_AUTOLOAD -u PYTEST_ADDOPTS PYTHONDONTWRITEBYTECODE=1 TMPDIR="$work_dir/results/tmp" \
  "$work_dir/core-env/bin/python" -I -B -m pytest -n 4 -p no:cacheprovider \
  --junitxml="$work_dir/results/core.xml" \
  tests/test_admission.py tests/test_async_shutdown.py tests/test_bindings.py \
  tests/test_capture_values.py tests/test_comparisons.py tests/test_contract.py \
  tests/test_core.py tests/test_deferred.py tests/test_duplicate_transition.py \
  tests/test_failures.py tests/test_finalization.py tests/test_fixture.py \
  tests/test_inspection_logging.py tests/test_inspection_query.py \
  tests/test_inspection_snapshot.py tests/test_inspection_snapshot_api.py \
    tests/test_inspection_gate.py tests/test_logging.py tests/test_scope_validation.py \
  tests/test_shutdown.py tests/test_scale_regressions.py

env -u PYTHONPATH -u PYTHONHOME -u PYTHONOPTIMIZE -u PYTEST_DISABLE_PLUGIN_AUTOLOAD -u PYTEST_ADDOPTS PYTHONDONTWRITEBYTECODE=1 TMPDIR="$work_dir/results/tmp" \
  "$work_dir/current-env/bin/python" -I -B -m pytest -n 4 -p no:cacheprovider \
  --junitxml="$work_dir/results/current-sdk.xml" \
  tests/test_mcp_client.py tests/test_openai_chat.py \
  tests/test_integration_history.py tests/test_integration_fixture.py \
  tests/test_inspection_navigation.py \
  compatibility_tests/native/test_session.py compatibility_tests/native/test_anyllm.py
```

Record command exits, JUnit outcomes, worker/plugin versions and installed package origins. Preserve parametrized test identities and report failures, errors, crashes and skips separately. Keep malformed-model serializer warnings visible. Collection is not execution, and focused counts overlap aggregate counts.

The shared detailed model-policy checks live in [`test_openai_chat.py`](../tests/test_openai_chat.py), using the genuine OpenAI client and existing test transport. They distinguish three independent body selections, exact invocation context, native/callback ownership, literal retained bytes/hash/lengths, original per-part observed lengths, available original decision comparisons, request membership and original binding/insertion evidence. The bounded topology matrix covers null, missing messages/tools, non-array messages, changed count and recognizable reordering alongside valid per-position replacement. Intentional/failure omission retains structural request references and source completeness; JSON-null decision/response content is distinct from omission.

Model lifecycle controls cover ordinary error, callback cancellation and callback `None` with fixed omission and one bounded loss/log per invocation, plus valid/invalid-policy metadata precedence, factory validation, lazy work, captured scope, MISSING and overlapping context. Separate process-control checks exercise request, decision and terminal interruption, including a streamed decision. The complete and early-close stream cases preserve consumed chunk identity/order and original partial state without draining unread chunks; existing projection/overflow/error/cancellation tests remain neighboring controls. Catching and resuming after process-control interruption does not acquire an automatic-cleanup or at-most-once guarantee.

[`native/test_anyllm.py`](../compatibility_tests/native/test_anyllm.py) owns the focused second-factory route using the genuine AnyLLM/Ollama provider and existing transport/conversion fixtures. It checks original positional/keyword/options conversion, native normal/stream identities, independent retained request/decision/response bodies, context and attribution, never-awaited work and consumed-stream/early-close cleanup. It does not duplicate the full OpenAI structural/failure matrix.

Existing core retention coverage in [`test_capture_values.py`](../tests/test_capture_values.py), shared MCP policy coverage in [`test_mcp_client.py`](../tests/test_mcp_client.py), and the current [`native/test_session.py`](../compatibility_tests/native/test_session.py) remain part of the two aggregates. The MCP module owns the detailed nine-mechanism generic mapping-error matrix. Model events retain their model-call associations; they are not MCP operation reservations.

The stream budget cases cover separate raw/fragment accounting, exact and one-byte-over admission, 64/65 state bounds, released fragment reuse, late complete decisions on and after raw overflow, and continued conservative handling of real gaps, EOF, close, error and cancellation. Content and metadata modes preserve exact policy inputs, native chunk identity/order, and decision availability before the finishing read returns; a delegating encoding spy proves that later discarded full projections are not serialized again. The genuine AnyLLM/Ollama case covers the same late-decision behavior through native provider conversion.

[`test_inspection_query.py`](../tests/test_inspection_query.py) covers below-cap analysis with a small envelope, real SQLite too-big API/CLI errors at both output limits, native `printf`/`format` behavior (successful NULL or `SQLITE_TOOBIG`, depending on SQLite), a legitimately retained large body with independent small metadata and payload-prefix reads, and fetch-time failure after an observed row with no partial data, deadline precedence and cleanup. [`test_inspection_navigation.py`](../tests/test_inspection_navigation.py) asserts the fixed discovery limit; its eager integration fixture requires the SDK profile. Existing accepted scope and lifecycle regressions remain in the selections.

These checks use controlled local transports and require no live provider credentials or model downloads. They establish the selected retention behavior; they do not measure throughput or memory capacity under a production host workload. Run the separate optional profiles in §2 when validating those integrations.

## 7. Stdio proxy qualification

[`test_proxy.py`](../tests/test_proxy.py) belongs to the SDK-free core selection. It launches the installed CLI and raw peers to compare exact bytes, request correlation, bounded observation loss, backpressure and process cleanup. Capture-failure controls inject deterministic faults into the installed core through a test-only bootstrap; these are not CLI options. [`test_proxy_sdk.py`](../tests/test_proxy_sdk.py) runs genuine modern SDK peers directly and through the proxy, including discovery, native errors, input-required results, explicit client continuation and cancellation. The isolated legacy profile exercises its own SDK family and startup/reverse-request path. All native calls remain owned by the client and server.

The [proxy walkthrough](proxy.md) separately records metadata-default and explicitly content-enabled examples, discovers an operation artifact through the existing public query path, and opens the closed recording with the MCP inspector. MCP-only recordings have no host turns or model evidence; a passing boundary check does not prove complete wire capture or satisfy the host-evidence gate. Retain observer stderr when evaluating capture loss.
