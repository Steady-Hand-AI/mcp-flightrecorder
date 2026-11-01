"""Attach OpenAI capture to a fast-agent-owned LLM factory (fast-agent 0.10.47).

Use `await agent.attach_llm(make_recorded_llm_factory(recorder, model,
instance_prefix="agent"))`. Reattach when the host creates a model-changing
clone; no MCP, cached results or bypassed client calls are observed.
"""
import uuid

from fast_agent.llm.model_factory import ModelFactory

from mcp_flightrecorder.integrations.openai_chat import wrap_model


def _validate_prefix(value):
    """Reserve 33 UTF-8 bytes for the separator and per-client UUID label."""
    if type(value) is not str or not value:
        raise ValueError("instance_prefix must be a nonempty UTF-8 string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise ValueError("instance_prefix must be a valid UTF-8 string") from error
    if len(encoded) > 223:
        raise ValueError("instance_prefix must encode to at most 223 UTF-8 bytes")


def make_recorded_llm_factory(recorder, model_spec, *, instance_prefix):
    """Observe each native client made by this explicitly owned LLM factory."""
    _validate_prefix(instance_prefix)
    native_factory = ModelFactory.create_factory(model_spec)

    def factory(*args, **kwargs):
        llm = native_factory(*args, **kwargs)
        native_client_factory = llm._openai_client

        def client_factory():
            client = native_client_factory()
            client.chat.completions.create = wrap_model(
                recorder,
                client.chat.completions.create,
                instance_id=instance_prefix + ":" + uuid.uuid4().hex,
            )
            return client

        llm._openai_client = client_factory
        return llm

    return factory
