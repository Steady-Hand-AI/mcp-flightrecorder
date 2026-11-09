# Changelog

## 0.5.18

- Add a deterministic SDK-backed model → selected MCP tool → prepared answer example with capture-on/off comparison and a guided `fr turns` / `fr explain` investigation. Preserve the SDK-free quickstart and existing capture/inspection contracts.

## 0.5.17

- Add opt-in OpenAI response usage capture for provider-reported prompt, cache-read and cache-write counts independently of body retention, including metadata mode. Preserve unknowns as null and explicit zeros, with a timeline query through the existing `fr sql` command.
- Confirm registry and GitHub release visibility with bounded read-only retries after uploads and publication, preserving exact identities and downloaded-byte checks without repeating mutations.

## 0.5.16

- Add canonical repository, issue-tracker and documentation links; clarify source installation and public test recipes.
- Exclude local planning, agent instructions, credentials and generated evidence from source distributions.
- Make snapshot-path and SQL-formatting tests portable across macOS path aliases and SQLite versions without changing runtime behavior.
- Require Python 3.11+ and preserve host exceptions and scope attribution when turn/dispatch cleanup runs in a foreign context.
- Separate stream raw-chunk and pending-tool retention into independent 1,048,576-byte pools, preserving complete tool observations after raw-body overflow.
- Cap query-only SQLite values and encoded rows at 8,388,608 bytes, with explicit native too-big errors and successful formatting NULL preserved.

- Replace closure's correlated response lookup with a run-scoped NULL-safe set query, retaining duplicate and unscoped unfinished requests.
- Reuse one lazy calling-thread identity reader per capture preparation, with fresh autocommit statements and cleanup before admission; preserve the 512-entry committed cache and receipt semantics.
- Bound per-read immutable-fact caching at 8 MiB / 4,096 entries / 1 MiB per entry; keep occurrence-specific completeness, repeated diagnostics and authoritative first-witness fallback after minimum eviction.
- Avoid discarded request-member projections in turn totals while preserving row order and display diagnostics; validate links before operation checks displace membership-prefix cache facts.
- Count turn answers through the shared occurrence traversal without discarded public dictionaries; reuse the current request structure only for matching witness validation, preserving both answer passes, diagnostics and cache bounds.
- Expose a validated per-read timeout on check, find_turns, explain, payload and slice, forwarding it through both CLI reads when run selection is implicit.

- Move the eight technical control-flow diagrams into the architecture documentation and remove repository agent instructions from source distributions.
- Correct sequence-diagram message punctuation so all eight detailed flows render with Mermaid without changing control-flow meaning.
- Compare recorded and producer SDK versions independently against installed distribution metadata.
- Replace sandbox-specific selector polling in test support with standard asyncio while preserving existing watchdogs, native outcomes and cleanup.
- Show direct capture, independent snapshot publication and per-invocation reader ownership in the architecture guides; keep the offline diagram readable and keyboard-scrollable on narrow screens.
- Use one selected interpreter in development commands, distinguish the 18 core and ten integration modules, and describe current public contracts without obsolete assignment or pinned-SDK wording.
- Preserve library behavior, public interfaces, SQLite schema 6 and `mfr.inspect/1`.

## 0.5.15

- Remove exact build-backend and public optional-SDK version pins. Keep the dependency-free core and the deliberate MCP 1.x `<2` boundary for the legacy session adapter.
- Replace seven large, hash-locked transitive requirements files with small direct-dependency lists; optional host profiles remain isolated and require genuine SDK testing.
- Document a latest-stable, GIL-enabled CPython 3.15 core qualification procedure separately from SDK-specific tests, without imposing a Python 3.12 development gate. Preserve runtime behavior, SQLite schema 6 and `mfr.inspect/1`.

## 0.5.14

- Classify cyclic source and destination symlinks consistently as `invalid_request/invalid_path` on supported Python versions, while preserving missing source and fresh destination behavior.
- Keep existing snapshot private staging, exclusive publication, no-public-unlink ownership, schema 6 and `mfr.inspect/1` unchanged.

## 0.5.13

- Detect a destination SQLite sidecar visible immediately after successful exclusive snapshot publication. Return `recording_unreadable/destination_finalize` instead of a false verified-success status; the public main may already exist and no public path is deleted.
- Add deterministic `-wal`, `-shm` and `-journal` interleavings at the native-link boundary. The final sidecar check is best effort for shared-directory writers, not an atomic multi-path lock. Keep schema 6, `mfr.inspect/1`, and the SDK boundaries unchanged.

## 0.5.12

- Remove the public main-file cleanup TOCTOU: finish SQLite backup, DELETE conversion, read-only verification and bounded result preparation in private same-parent staging, then publish the verified main with one exclusive hard link. No unsuccessful or interrupted path unlinks a public output pathname.
- Preserve competing main-file and destination suffix bytes at backup, interruption and publication boundaries. Keep foreign sidecars, source bytes, error envelopes and timeouts; add deterministic race tests, plus updated snapshot/logging cleanup assertions.
- Update M07 and inspection/architecture contracts for late publication. No change to schema 6, `mfr.inspect/1`, SDK adapters or dependency pins.

## 0.5.11

- Fix snapshot cleanup ownership: SQLite backs up/finalizes in a private staging directory, while the public output main is created exclusively and published through its retained descriptor. Public `-wal`, `-shm` and `-journal` names are never cleanup targets without ownership proof.
- Preserve independently created sidecars after main reservation on copy failure or interruption; retain no-clobber, live-source backup, verified portable DELETE output, deadlines and error envelopes. Cover all three suffix races and genuine private SQLite sidecar cleanup with installed-path regression tests.
- Update the agent map and snapshot contract to distinguish the owned output inode and private staging files from unrelated public sidecars. No SQLite schema, reader-format or SDK changes.

## 0.5.10

- Keep `examples/` focused on concise host-attachment recipes and an SDK-free `quickstart.py`.
- Move long deterministic recording producers to `tests/producers/` without changing their recording scenarios.
- Refresh README, testing instructions, integration links and HTML architecture navigation for the new layout.
- No capture, storage, inspection or SDK adapter behavior changes.

## 0.5.9

- Add standard Python module logging for recorder lifecycle, capture loss, writer failures, adapter evidence losses, inspection errors and snapshot outcomes. Keep application configuration and output ownership external; omit captured content and raw exceptions.
- Bound repeated capture warnings and unchanged close results. Contain ordinary application-handler failures, emit outside admission/retry locks and read transactions, and distinguish portable storage from complete capture.
- Give capture one result/logging boundary and consolidate reader/snapshot SQLite error translation without changing deadline precedence.
- Preserve the recorded comparison reasons `invalid_raw_arguments`, `conflicting_arguments` and `text_paths_unavailable` in inspection output instead of replacing them with `digest_unavailable`.
- Add behavioral logging regressions for content privacy, handler failure, concurrent loss reporting, ownership, cleanup and native result/error/cancellation preservation. Align the Markdown and HTML architecture guides with the new logging boundary.

## 0.5.8

- Restore recursive validation in original-comparison encoding, correcting the deep raw-argument token/reason regression introduced by the 0.5.7 copy removal; add two public regression cases.
- Correct five published-wheel hashes in four compatibility locks without changing package versions or the FastMCP source pins.
- Remove remaining unused native-test explanation queries, duplicate hashing/copying and report metadata, plus three redundant integration-fixture assertions.
- Make the newly created disposable testing environment readable by the ordinary-user media probe while preserving its UID-drop capability check and the actual media-permission boundary.

## 0.5.7

- Remove the unused capture, slice and snapshot reference implementations and migrate five useful cases to the installed public API/CLI tests.
- Move independent core/integration scenario assertions into ordinary pytest tests and six small shared helpers; remove the three standalone verification programs and their obsolete interface tests.
- Encode and hash already-owned JSON directly. This introduced the deep raw-argument comparison regression corrected in 0.5.8; retention and digest framing remained unchanged.
- Remove 61 unread compatibility-report writes and their dead support code; preserve the six consumed IPC files and native behavioral checks.
- Replace the stream detector self-test with cancellation before the first consumed chunk; remove the unsupported high-level FastMCP failure requirement.
- Reuse existing async fixtures while preserving the two-, four- and 45-second case watchdogs and FastMCP's 30-second default.
- Update current source maps and installed-wheel testing instructions. Public signatures, SQLite schema 6, inspection format `mfr.inspect/1` and SDK dependency pins remain unchanged.

## 0.5.6

- Reduce the distribution to current source, useful examples, meaningful tests, dependency profiles and current documentation.
- Remove accumulated handoffs, audit ledgers, generated recordings, run logs, stored reports, experiment scripts, vendored wheels and nested release artifacts.
- Generate test recordings in temporary directories from the source examples and small declarative integration inputs.
- Simplify the test harness and keep incompatible SDK compatibility profiles in separate environments.
- Replace historical navigation with current capture, inspection, integration and testing guidance, plus matching Markdown and HTML architecture maps.

The public capture and inspection interfaces and SQLite schema version 6 remain unchanged.
