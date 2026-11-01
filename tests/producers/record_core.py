"""Create the deterministic core recording used by the acceptance tests."""

import argparse
import json
from pathlib import Path

from mcp_flightrecorder import Recorder


COVERAGE = {
    "producers": {"model": "fixture.model", "host": "fixture.host", "mcp": "fixture.mcp"},
    "model_complete": True,
}
CATALOG = [{
    "name": "echo", "description": "Returns the supplied text.",
    "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}},
                    "required": ["text"], "additionalProperties": False},
}]
MESSAGES = {
    "U": {"role": "user", "content": "Echo the selected text."},
    "A": {"role": "assistant", "tool_calls": [{
        "id": "provider-call-1", "type": "function",
        "function": {"name": "echo", "arguments": '{"text":"selected"}'},
    }]},
    "B": {"role": "tool", "tool_call_id": "provider-call-1", "content": "submitted"},
    "P1": {"role": "system", "content": "Synthetic retained history A."},
    "P2": {"role": "system", "content": "Synthetic retained history B."},
    "F0m": {"role": "assistant", "content": "First follow-up complete."},
    "U2": {"role": "user", "content": "Use retained context; do not select another tool."},
    "F1m": {"role": "assistant", "content": "No new tool call."},
    "F2m": {"role": "assistant", "content": "Retained context checked."},
}


def accepted(receipt):
    """Fail a fixture run before consuming an unavailable prerequisite ID."""
    if receipt.status != "accepted":
        raise RuntimeError(f"fixture observation rejected: {receipt.reason}")
    return receipt


def observe(recorder, kind, payload, **facts):
    return accepted(recorder.observe(kind, scope=recorder.capture_scope(),
                                     payload=payload, facts=facts))


def submit_request(recorder, symbols, *, decision_event=None, result_event=None):
    """Declare each actual occurrence, with explicit binding context."""
    annotations = []
    for symbol in symbols:
        bindings = []
        if symbol == "B":
            bindings = [{"decision_event_id": decision_event, "pointer": "/content",
                         "result_event_id": result_event, "text_paths": [""]}]
        annotations.append({"bindings": bindings, "unresolved": [], "complete": True})
    payload = {"model": "fixture-model", "system": "Synthetic evidence fixture.",
               "tools": CATALOG, "messages": [MESSAGES[symbol] for symbol in symbols]}
    return observe(recorder, "model.request", payload, message_path="/messages",
                   field_paths=["/system", "/tools"], catalog_path="/tools",
                   messages=annotations)


def close_dict(result):
    return {key: getattr(result, key) for key in
            ("writer_stopped", "run_close_state", "portable", "reason")}


def record_fixture(output):
    """Produce the twelve controlled observations described in docs/testing.md, in order."""
    recorder = Recorder(output, capture="content", coverage=COVERAGE,
                        environment={"synthetic": True, "fixture": "core_acceptance"})
    summary = {"run_id": recorder.capture_scope().run_id, "turns": {}, "events": {},
               "model_calls": {}}
    events, calls = summary["events"], summary["model_calls"]
    try:
        with recorder.turn("Echo the selected text.", conversation_id="fixture-conversation",
                           conditions={"case": "core_acceptance", "variant": "rewrite"}) as turn:
            summary["turns"]["T1"] = accepted(turn.receipt).turn_id
            request = submit_request(recorder, ["U"])
            events["R0"], calls["M0"] = request.event_id, request.model_call_id
            decision = observe(recorder, "model.tool_call", {"arguments": {"text": "selected"}},
                               name="echo", model_call_id=calls["M0"], provider_ref="provider-call-1")
            events["D"], summary["decision_id"] = decision.event_id, decision.decision_id
            events["Z0"] = observe(recorder, "model.response", MESSAGES["A"],
                                    model_call_id=calls["M0"], state="complete").event_id
            with recorder.dispatch(action="execute", name="echo", arguments={"text": "executed"},
                                   decision_event_ids=[decision.event_id]) as dispatch:
                events["H"] = accepted(dispatch.receipt).event_id
                start = accepted(recorder.start_operation(
                    scope=recorder.capture_scope(), client_id="fixture-client",
                    server_key="fixture-server", method="tools/call", name="echo",
                    arguments={"text": "executed"}, decision_id=decision.decision_id,
                    link_basis="host_hook"))
                events["S"], summary["operation_id"] = start.event_id, start.operation_id
                result = {"content": [{"type": "text", "text": "executed"}], "isError": False}
                terminal = accepted(recorder.finish_operation(
                    start.ticket, status="ok", resolution="remote", input_rounds=0,
                    result=result, text_paths=["/content/0/text"]))
                events["E"] = terminal.event_id
            request = submit_request(recorder, ["U", "A", "B"],
                                     decision_event=decision.event_id, result_event=terminal.event_id)
            events["W0"], calls["M1"] = request.event_id, request.model_call_id
            events["F0"] = observe(recorder, "model.response", MESSAGES["F0m"],
                                    model_call_id=calls["M1"], state="complete").event_id
        accepted(turn.exit_receipt)
        with recorder.turn("Use retained context; do not select another tool.",
                           conversation_id="fixture-conversation",
                           conditions={"case": "core_acceptance", "variant": "no_tool"}) as turn:
            summary["turns"]["T2"] = accepted(turn.receipt).turn_id
            request = submit_request(recorder, ["P1", "U", "A", "B", "F0m", "U2"],
                                     decision_event=decision.event_id, result_event=terminal.event_id)
            events["W1"], calls["M2"] = request.event_id, request.model_call_id
            events["F1"] = observe(recorder, "model.response", MESSAGES["F1m"],
                                    model_call_id=calls["M2"], state="complete").event_id
            request = submit_request(recorder, ["P1", "P2", "U", "A", "B", "F0m", "U2", "F1m"],
                                     decision_event=decision.event_id, result_event=terminal.event_id)
            events["W2"], calls["M3"] = request.event_id, request.model_call_id
            events["F2"] = observe(recorder, "model.response", MESSAGES["F2m"],
                                    model_call_id=calls["M3"], state="complete").event_id
        accepted(turn.exit_receipt)
    finally:
        summary["close"] = close_dict(recorder.close())
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(record_fixture(args.output), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
