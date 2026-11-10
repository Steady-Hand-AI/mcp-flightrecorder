# MCP Flight Recorder 0.5.18

Version 0.5.18 adds a complete, deterministic model → selected MCP tool → prepared answer example and an investigation guide.

- Run two nonstreaming OpenAI SDK calls with a local mock transport, one MCP discovery and one selected tool call against an in-process server. No external service or credential is needed.
- Compare capture enabled and disabled while preserving the host's submitted inputs, native results and selected tool execution.
- Follow the prepared string `Tool result: 42` through the existing `fr turns` and both model-call `fr explain` views. Explicit source, answer-binding and insertion evidence preserves the intentional literal difference from the tool text `42`.

The SDK-free quickstart, recorder and adapter behavior, inspection CLI, SQLite schema 6 and inspection format `mfr.inspect/1` remain unchanged. Python 3.11+ is required. The core remains dependency-free.

See the [roundtrip walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/tool_roundtrip.md) for installation and commands. Earlier changes remain in the [0.5.17 release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.17/docs/release-notes.md) and [changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/CHANGELOG.md).
