# MCP Flight Recorder 0.5.20

Version 0.5.20 adds optional MCP inspection and an explicit CI evidence gate over the existing recording readers.

- `fr mcp FILE` serves one operator-selected stable portable recording over stdio. It exposes diagnosis, runs, turns, explanation and ancestry, plus one schema resource. Payload retrieval is available only when the operator enables `--allow-content` and the client explicitly requests a bounded window.
- `inspect.snapshot(path, output, timeout=...)` exposes the existing portable-copy implementation used by `fr snapshot`. Acquisition remains a separate operation; the MCP server refuses WAL/SHM filesets and does not copy or recover live sources.
- `inspect.gate` and `fr gate` require explicit expected boundaries and minimum observed activity. They evaluate the entire relevant run population in one read transaction, checking capture integrity, complete turns, answer bindings, required comparisons and tool-call attribution. Bounded witness samples do not truncate evaluation. Pass, fail, unknown and operational errors remain distinct.

Validated host preparation can explain a raw text mismatch without changing that mismatch or certifying semantic equivalence. Missing evidence, unavailable comparisons and incomplete evaluation cannot earn a passing gate result. The gate cannot prove answer quality or the absence of unobserved calls. Metadata-only output remains potentially sensitive, untrusted recording data.

Python 3.11+ is required. Core capture, inspection, snapshot and gate remain dependency-free; the stdio inspector uses the optional `[mcp]` extra. SQLite schema 6, capture contracts, `mfr.inspect/1` and the separate `mfr.diagnose/1` diagnosis envelope are preserved. Host clients, routing, retries, policies and stream consumption remain application-owned. This release does not add an MCP recording proxy.

Start with the [PyPI quickstart](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/README.md#install-and-record), then follow the [MCP subprocess walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/mcp-inspection.md) and [gate contract](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/inspection.md#evidence-gate). Earlier features remain in the [0.5.19 release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.19/docs/release-notes.md) and [changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/CHANGELOG.md).
