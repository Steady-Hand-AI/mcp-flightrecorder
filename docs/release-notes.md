# MCP Flight Recorder 0.5.16

MCP Flight Recorder captures explicit model, host and MCP observations in portable SQLite recordings. The Python library and `fr` CLI support bounded capture, read-only inspection, evidence relationships and independent snapshots.

- The core uses the Python standard library, a bounded queue and one capture-time writer thread.
- Optional OpenAI Chat, AnyLLM/Ollama and MCP adapters observe supported SDK calls while the host owns clients, routing, retries and stream consumption.
- Inspection preserves unknown, incomplete and unavailable evidence rather than inferring provenance from equal text, names or timestamps.
- Package checks validate source and distribution inventories, metadata, license files and the installed schema. CI exercises installed wheels in separate SDK-free and integration environments.
- The manual release workflow validates one wheel/sdist pair and promotes the same approved bytes through TestPyPI, PyPI and GitHub, with downloaded-byte and installation verification between stages.

Version 0.5.16 requires Python 3.11+ and uses SQLite schema 6. It is licensed under Apache-2.0; copyright 2026 Volodymyr Barylyak. An accepted capture receipt is admission to the queue, not a durability guarantee. A portable recording can still contain incomplete capture.

See the [README](https://github.com/Steady-Hand-AI/mcp-flightrecorder/blob/main/README.md) for source installation, supported usage and documentation. These notes describe the version; registry availability is established by the verified published release.
