# MCP Flight Recorder 0.5.19

Version 0.5.19 adds a public integration facade and bounded diagnostics for existing model and MCP hosts.

- `AgentRecorder` manages caller-owned conversations, explicit selected attempts, stable replay keys and prepared text snapshots. The host keeps control of native selection, execution, retries, concurrency and client/stream lifetimes, including when recording is disabled.
- `fr doctor` and `inspect.diagnose` report recorded gaps, locations and corrective hints. `fr turns --text`, `fr explain --text` and `--last` support readable investigation while preserving existing JSON defaults.
- Preparation explanations validate recorded endpoints, ordering, all declared sources and original text tokens. They preserve raw mismatch flags and unknown evidence; matching prepared text does not establish semantic correctness.
- Foreign-process guards refuse inherited recorder use before owned locks or storage access. Failed construction cleans up only files still proven to belong to that initialization; fresh workers and recording paths remain required.

The integration guide covers JSON/OpenAI calls, modern MCP client/session calls, replay, retries, parallel choices, explicit local work and consumed SDK streams. Bindings and prepared snapshots are bounded. Local tool results have model association but no MCP-result text comparison. Overlapping turns on one conversation and unavailable evidence remain unattributed; use separate conversations for concurrent turns. Doctor is not a whole-run success gate. This release adds no generic multimodal handling, automatic storage recovery or threaded-fork safety.

Python 3.11+ is required. The core remains dependency-free, SQLite schema 6 and inspection format `mfr.inspect/1` are unchanged, and diagnosis uses the separate `mfr.diagnose/1` envelope. Accepted capture still means queue admission, not durable storage.

Start with the [complete PyPI quickstart](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.19/README.md#install-and-record), then use the [integration guide](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.19/docs/easy-integration.md). Earlier changes remain in the [0.5.18 release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.18/docs/release-notes.md) and [changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.19/CHANGELOG.md).
