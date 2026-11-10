# MCP Flight Recorder 0.5.21

Version 0.5.21 adds one stdio MCP recording proxy for users who can change a server launch command without editing host code.

- `fr proxy --record FILE -- SERVER_COMMAND ARGUMENT...` forwards one POSIX client's and one child server's native bytes in order. It preserves opaque methods, fields, IDs, notifications and errors; it does not negotiate, retry, route calls or reconstruct SDK messages.
- A separate bounded observer records selected client `tools/list` and `tools/call` requests and their explicit-ID responses through the existing recorder. Metadata retention is the default; `--capture content` explicitly permits payload retention. Omitting `--record` forwards without creating a recording.
- Capture initialization refusal, capacity loss and writer failures do not suppress or repeat native calls. Observer losses are reported separately on stderr. Unavailable or ambiguous correlation remains incomplete rather than borrowing another invocation's result. Oversized frames still forward incrementally.
- The child and pipe lifecycle has a finite shutdown grace after observed EOF or interruption. Child stderr stays off protocol stdout, and normal child exit status is preserved. The implementation uses POSIX pipes; it does not claim Windows qualification or arbitrary-host compatibility.

This is MCP-boundary evidence, not model or host tracing. Recordings declare MCP-only coverage and contain no invented turns or dispatches. Modern input-required results remain partial RPC evidence; the client owns continuation. Legacy reverse requests forward without becoming model observations. `fr check --expect mcp` checks retained boundary evidence; it cannot account for bytes the observer could not admit. The host evidence gate is not waived for proxy recordings. Metadata is not anonymization, and content may contain sensitive data.

Python 3.11+ is required. The proxy and core readers have no third-party runtime dependencies. The genuine SDK example and optional MCP inspector use the `[mcp]` extra. SQLite schema 6, the existing capture API, `mfr.inspect/1` and `mfr.diagnose/1` are unchanged.

Follow the [proxy recording and inspection walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.21/docs/proxy.md), the [PyPI quickstart](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.21/README.md#install-and-record), and the [inspection contract](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.21/docs/inspection.md). Earlier features are documented in the [0.5.20 release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.20/docs/release-notes.md) and [changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.21/CHANGELOG.md).
