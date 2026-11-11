"""Owned deterministic peers for openai_mcp.py; no remote service is contacted."""
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
import runpy

import httpx2
from mcp import Client
from mcp.server import Server
import mcp_types
from openai import AsyncOpenAI


# Reuse the existing example's model replies, discovery and native tool validation.
_roundtrip = runpy.run_path(str(Path(__file__).with_name("tool_roundtrip.py")))
MODEL, QUESTION = _roundtrip["MODEL"], _roundtrip["QUESTION"]
CASES = ("single", "multiple", "tool-error", "joined", "prepared", "unsupported")


class ExamplePeers(_roundtrip["LocalServices"]):
    """Independent observations at the SDK transports, including actual request bytes."""

    def __init__(self, case="single", *, result=None):
        super().__init__()
        if case not in CASES:
            raise ValueError("Unknown example case")
        self.serialized_requests = []
        if result is None:
            texts = ["4", "2"] if case in ("multiple", "joined", "prepared") else ["42"]
            if case == "tool-error":
                texts = ["Tool unavailable"]
            content = [mcp_types.TextContent(type="text", text=text) for text in texts]
            if case == "unsupported":
                content.append(mcp_types.ImageContent(type="image", data="AA==", mime_type="image/png"))
            result = mcp_types.CallToolResult(content=content, is_error=case == "tool-error")
        self.result = result

    async def provider(self, request):
        self.serialized_requests.append(bytes(request.content))
        response = await super().provider(request)
        if len(self.provider_requests) == 2:
            payload = response.json()
            payload["choices"][0]["message"]["content"] = "Tool response received."
            self.provider_responses[-1] = deepcopy(payload)
            response = httpx2.Response(200, json=payload)
        return response

    async def call_tool(self, context, params):
        await super().call_tool(context, params)
        self.tool_results[-1] = self.result.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return self.result

    def observations(self):
        return {**super().observations(),
                "provider_request_bodies": [body.decode("utf-8") for body in self.serialized_requests]}


@asynccontextmanager
async def local_clients(peers):
    """Own the model client and supply an inert MCP client for the host's scope."""
    server = Server("facade-example", on_list_tools=peers.list_tools, on_call_tool=peers.call_tool)
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(peers.provider), trust_env=False)
    async with AsyncOpenAI(api_key="local-example-placeholder", base_url="https://roundtrip.invalid/v1",
                           http_client=http, max_retries=0) as provider:
        yield provider, Client(server)
