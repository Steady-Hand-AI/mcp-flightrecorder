"""Run a no-network Pydantic/OpenAI recording with a fixed mock response.

Usage: `python -I -B examples/pydantic_openai.py NEW.sqlite`.
Attach the actual client callable; per-run client clones require their own wrap.
"""

import argparse
import asyncio
from importlib.metadata import version
import json
from pathlib import Path

import httpx2
from openai import AsyncOpenAI
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

from mcp_flightrecorder import Recorder, inspect
from mcp_flightrecorder.integrations.openai_chat import wrap_model

PROMPT = "Explain the café result: ✓"
INSTRUCTION = "Reply using the supplied fixture outcome."
ANSWER = "ready ✓"
COVERAGE = {
    "producers": {"model": "recipe.openai_chat", "host": "recipe.pydantic", "mcp": None},
    "model_complete": False,
}


def provider_reply(request):
    """The transport owns a fixed native response and cannot fall through to HTTP."""
    return httpx2.Response(200, json={
        "id": "recipe-completion", "object": "chat.completion", "created": 1,
        "model": "gpt-4o", "choices": [{"index": 0,
            "message": {"role": "assistant", "content": ANSWER}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 9, "completion_tokens": 4, "total_tokens": 13},
    })


async def record_example(path):
    """Execute the fixed example; the only returned report is the existing reader envelope."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(f"Use a new recording path: {path}")
    recorder = Recorder(path, capture="content", coverage=COVERAGE, environment={
        "recipe": "pydantic_openai", "transport": "in_process_mock_no_network",
        "boundary": "submitted OpenAI arguments and normalized SDK result",
        "versions": {name: version(name) for name in ("pydantic-ai-slim", "openai", "httpx2")},
    })
    run_id = recorder.capture_scope().run_id
    try:
        async with AsyncOpenAI(
            api_key="fixture-only-not-a-secret", base_url="https://fixture.invalid/v1",
            max_retries=0,
            http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(provider_reply), trust_env=False),
        ) as client:
            # Configure the native client first; wrap the callable used by this model.
            client.chat.completions.create = wrap_model(
                recorder, client.chat.completions.create, instance_id="pydantic-openai"
            )
            model = OpenAIChatModel("gpt-4o", provider=OpenAIProvider(openai_client=client))
            agent = Agent(model, system_prompt=INSTRUCTION)
            with recorder.turn(PROMPT):
                result = await agent.run(PROMPT)
    finally:
        closed = recorder.close(timeout=3)
    if not closed.writer_stopped or not closed.portable:
        raise RuntimeError("The example recording did not finish as a portable SQLite file.")
    if result.output != ANSWER:
        raise RuntimeError("The controlled native host did not return its fixed fixture output.")
    return inspect.check(path, run_id, expect=("model",))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording", type=Path, help="A new SQLite recording path")
    args = parser.parse_args()
    envelope = asyncio.run(record_example(args.recording))
    print(json.dumps(envelope, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
