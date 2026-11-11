# MCP Flight Recorder 0.5.24

Version 0.5.24 adds a complete OpenAI Chat Completions → MCP tool → submitted answer example using the existing `AgentRecorder` facade.

- Run native OpenAI and MCP clients against owned deterministic peers. The host selects the tool call, executes it, submits its answer and closes recording with explicit client ownership.
- Preserve a single text string, ordered text parts and tool-error text. An example-local formatter supports an intended string transform, propagates callback exceptions and refuses unsupported result content.
- Check evidence and actual serialized requests independently. Four faithful cases pass both checks. An undeclared join fails both, while unsupported content is refused before the second model request and cannot meet the complete-flow evidence requirements.

The [complete guide](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.24/docs/openai_mcp.md) installs the pinned package and SDKs, downloads matching source examples, and runs outside the source directory. The examples use deterministic local peers and make no live-provider calls. A passing evidence check does not establish answer quality. The library APIs, capture behavior, evidence policy, SQLite schema 6 and zero core runtime dependencies are preserved.

Earlier changes remain in the [0.5.23 release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.23/docs/release-notes.md) and [changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.24/CHANGELOG.md).
