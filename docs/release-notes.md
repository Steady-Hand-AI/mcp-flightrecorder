# MCP Flight Recorder 0.5.23

Version 0.5.23 adds a small evidence assertion for ordinary tests and a complete regression example.

- Call `mcp_flightrecorder.testing.assert_evidence` on a completed recording. PASS returns the existing gate result. Completed FAIL or UNKNOWN raises a bounded `AssertionError`. Read or configuration errors raise `EvidenceReadError` with the original reader reason.
- Preserve the default all-answer policy. Explicit `allow_local_tools=True` excludes validated local comparisons while keeping local forwarding unverified and missing or ambiguous MCP evidence unresolved.
- Run the source-shipped host and standard-library tests against the installed package. Fresh recordings and independent application expectations distinguish faithful forwarding, undeclared mutation, declared preparation, mixed local/MCP scope and unavailable evidence.

A faithful recording can contain an incorrect application answer. The helper reuses the existing gate and renderer. Capture behavior, evidence policy, SQLite schema 6 and existing inspection formats stay unchanged. The core remains dependency-free, and the synthetic example uses no SDK or provider.

Follow the [regression walkthrough](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.23/docs/agent-workflow.md#keep-a-regression-check) for the helper contract and complete pinned-package/source recipe. The [installed demo](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.23/README.md#install-and-record) remains the starting point for investigating a recording. Earlier changes remain in the [0.5.22 release notes](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.22/docs/release-notes.md) and [changelog](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/0.5.23/CHANGELOG.md).
