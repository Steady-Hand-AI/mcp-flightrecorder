"""Current host behavior with deterministic, synthetic model/tool boundaries.

The model and tool functions below are deterministic in-process stubs.
The recorded MCP boundary observations are synthetic.
"""
from mcp_flightrecorder import Recorder


def forward_answer(text):
    """The application's requirement is to forward the tool text unchanged."""
    return text


class SyntheticServices:
    def __init__(self):
        self.submitted = []

    def choose(self, prompt):
        return {"name": "local" if prompt == "Use the local tool" else "answer", "arguments": {}}

    def tool(self, name, arguments):
        if arguments != {} or name not in {"answer", "local"}:
            raise ValueError("Unsupported synthetic tool invocation")
        return "42" if name == "answer" else "local value"

    def submit(self, messages):
        self.submitted.extend(message["content"] for message in messages)
        return {"content": "Received the supplied tool answer"}


def _accepted(receipt):
    if receipt.status != "accepted":
        raise RuntimeError("Synthetic capture unavailable: " + str(receipt.reason))
    return receipt


def _observe(recorder, kind, payload, **facts):
    return _accepted(recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts))


def _choose(recorder, services, prompt):
    messages = [{"role": "user", "content": prompt}]
    request = _observe(recorder, "model.request", {"messages": messages}, messages=[{"bindings": []}])
    response = services.choose(prompt)
    decision = _observe(recorder, "model.tool_call", response,
                        name=response["name"], model_call_id=request.model_call_id)
    _observe(recorder, "model.response", response, model_call_id=request.model_call_id)
    return response, decision


def _submit(recorder, services, decision, result_id, text, preparation=None):
    messages = [{"role": "tool", "content": text}]
    binding = {"decision_event_id": decision.event_id, "result_event_id": result_id,
               "pointer": "/content", "text_paths": [""]}
    insertions = [] if preparation is None else [{"source_event_id": preparation.event_id,
        "target_pointer": "/messages/0/content", "content": text}]
    request = _observe(recorder, "model.request", {"messages": messages},
                       messages=[{"bindings": [binding]}], insertions=insertions)
    response = services.submit(messages)
    _observe(recorder, "model.response", response, model_call_id=request.model_call_id)


def run_host(path, *, scenario="faithful"):
    """Execute afresh into one explicit new file and return what the model received.

    The named negative scenarios deliberately exercise a broken host or missing
    evidence. They are expected failures in the accompanying ordinary tests.
    """
    if scenario not in {"faithful", "mutated", "prepared_wrong", "mixed", "missing_result", "ambiguous"}:
        raise ValueError("Unknown synthetic scenario")
    services = SyntheticServices()
    recorder = Recorder(path, capture="content", coverage={
        "producers": {"model": "regression.model", "host": "regression.host", "mcp": "regression.mcp"},
        "model_complete": True,
    })
    try:
        with recorder.turn("Synthetic MCP answer"):
            choice, decision = _choose(recorder, services, "Use the answer tool")
            start = _accepted(recorder.start_operation(scope=recorder.capture_scope(),
                client_id="synthetic", method="tools/call", name=choice["name"],
                arguments=choice["arguments"], decision_id=decision.decision_id, link_basis="propagated"))
            original = services.tool(choice["name"], choice["arguments"])
            # The missing-result control deliberately leaves this operation incomplete.
            result = None if scenario == "missing_result" else _accepted(recorder.finish_operation(
                start.ticket, status="ok", result=original, text_paths=[""]))
            submitted = forward_answer(original)
            if scenario in {"mutated", "prepared_wrong"}:
                submitted = "Tool result: " + submitted
            preparation = None
            if scenario == "prepared_wrong":
                preparation = _observe(recorder, "host.tool_result", submitted,
                                       sources=[{"event_id": result.event_id}], text_paths=[""])
            _submit(recorder, services, decision, result.event_id if result else None, submitted, preparation)
        if scenario in {"mixed", "ambiguous"}:
            with recorder.turn("Synthetic local answer"):
                choice, decision = _choose(recorder, services, "Use the local tool")
                for _ in range(2 if scenario == "ambiguous" else 1):
                    with recorder.dispatch(action="local", name=choice["name"],
                                           arguments=choice["arguments"], decision_event_ids=[decision.event_id]):
                        original = services.tool(choice["name"], choice["arguments"])
                        _observe(recorder, "host.tool_result", original, sources=[], text_paths=[""])
                _submit(recorder, services, decision, None, forward_answer(original))
    finally:
        closed = recorder.close()
    if not closed.writer_stopped or not closed.portable:
        raise RuntimeError("Synthetic recording did not close portably")
    return tuple(services.submitted)
