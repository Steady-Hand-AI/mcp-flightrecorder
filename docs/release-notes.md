# MCP Flight Recorder 0.5.17

Version 0.5.17 adds opt-in token counts to terminal OpenAI Chat `model.response` events.

- With the default `record_usage=False`, the wrapper omits the normalized `details_json.usage` field. An ordinary provider response body retained under the configured content capture and retention policy can still contain native usage fields. Use the capture mode and retention policy to omit or redact body content.
- Set `record_usage=True` on the wrapper to preserve provider-reported prompt, cache-read and cache-write token counts in `details_json.usage`, independently of body retention. The counts remain available even when a policy replaces or omits the body, or the recorder uses metadata mode.
- Missing or invalid counts remain null independently, and explicit zeros survive. These are provider reports, not inferred cache hits, verified bills or measured savings.
- For streaming calls, the last eligible aggregate consumed by the host supplies the counts. The host continues to own request options, stream consumption and cleanup.
- Inspect response counts with the documented timeline query through the existing `fr sql` command. SQLite schema 6 and inspection format `mfr.inspect/1` remain unchanged.
- Release publication uses bounded read-only confirmation of registry and GitHub visibility after uploads and publication, retaining exact release identities and downloaded asset-byte checks without repeating mutations.

Version 0.5.17 requires Python 3.11+ and is licensed under Apache-2.0; copyright 2026 Volodymyr Barylyak. An accepted capture receipt is admission to the queue, not a durability guarantee. A portable recording can still contain incomplete capture.

See [Integrations](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/docs/integrations.md#opt-in-response-usage) for the opt-in and SQL recipe, and the [README](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/README.md) for installation and usage.
