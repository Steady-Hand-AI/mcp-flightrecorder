"""Generate four credential-free recordings from synthetic controlled observations."""

import argparse
from pathlib import Path
import shlex
import tempfile

from . import Recorder, __version__


_COVERAGE = {
    "producers": {"model": "demo.model", "host": "demo.host", "mcp": "demo.mcp"},
    "model_complete": True,
}


def _accepted(receipt):
    if receipt.status != "accepted":
        raise RuntimeError(f"Synthetic observation unavailable: {receipt.reason}")
    return receipt


def _observe(recorder, kind, payload, **facts):
    return _accepted(recorder.observe(kind, scope=recorder.capture_scope(), payload=payload, facts=facts))


def _decision(recorder, name):
    request = _observe(recorder, "model.request", {"messages": []}, messages=[])
    decision = _observe(recorder, "model.tool_call", {}, name=name, model_call_id=request.model_call_id)
    _observe(recorder, "model.response", {}, model_call_id=request.model_call_id)
    return decision


def _submit(recorder, decision, result_id, text):
    message = {"role": "tool", "content": text}
    binding = {"pointer": "/content", "decision_event_id": decision.event_id,
               "result_event_id": result_id, "text_paths": [""]}
    request = _observe(recorder, "model.request", {"messages": [message]}, messages=[{"bindings": [binding]}])
    _observe(recorder, "model.response", {}, model_call_id=request.model_call_id)


def _record(path, case):
    recorder = Recorder(path, capture="content", coverage=_COVERAGE, comparisons=case != "incomplete")
    try:
        with recorder.turn("Synthetic MCP answer") as turn:
            _accepted(turn.receipt)
            decision = _decision(recorder, "answer")
            start = _accepted(recorder.start_operation(
                scope=recorder.capture_scope(), client_id="demo", method="tools/call", name="answer",
                arguments={}, decision_id=decision.decision_id, link_basis="propagated",
            ))
            result = _accepted(recorder.finish_operation(start.ticket, status="ok", result="42", text_paths=[""]))
            _submit(recorder, decision, result.event_id, "43" if case == "mutated" else "42")
        if case == "mixed":
            with recorder.turn("Synthetic local answer") as turn:
                _accepted(turn.receipt)
                decision = _decision(recorder, "local_answer")
                with recorder.dispatch(action="local", name="local_answer", arguments={},
                                       decision_event_ids=[decision.event_id]) as dispatch:
                    _accepted(dispatch.receipt)
                    _observe(recorder, "host.tool_result", "local value", text_paths=[""])
                _submit(recorder, decision, None, "local value")
    finally:
        closed = recorder.close()
    if not closed.writer_stopped or not closed.portable:
        raise RuntimeError(f"Synthetic recording did not close portably: {closed.reason}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, help="Existing parent for a fresh recording directory.")
    args = parser.parse_args(argv)
    try:
        parent = args.output_dir.resolve(strict=True) if args.output_dir is not None else Path(tempfile.gettempdir()).resolve()
        if not parent.is_dir() or any(ord(char) < 32 or 127 <= ord(char) < 160 for char in str(parent)):
            parser.error("Choose an existing output directory without control characters in its path.")
        folder = Path(tempfile.mkdtemp(prefix="mfr-demo-", dir=parent))
    except (OSError, RuntimeError):
        parser.error("Could not create a fresh recording directory under the requested parent.")
    cases = (("faithful", "PASS", 0), ("mutated", "FAIL", 2), ("incomplete", "UNKNOWN", 4), ("mixed", "UNKNOWN", 4))
    for case, _, _ in cases:
        _record(folder / (case + ".sqlite"), case)
    print("Synthetic controlled observations. No provider or credentials are used.")
    print("Recordings: " + shlex.quote(str(folder)))
    print("The mutated fixture changes submitted text from 42 to 43. The incomplete fixture disables comparisons.")
    uvx = ["uvx", "--from", f"mcp-flightrecorder=={__version__}", "mcp-flightrecorder"]
    for case, status, exit_code in cases:
        command = ["gate", str(folder / (case + ".sqlite")), "--expect", "model,host,mcp",
                   "--min-answer-occurrences", "1", "--text"]
        print(f"\n{case}: expected {status} (exit {exit_code})")
        print(shlex.join(["fr", *command]))
        print(shlex.join([*uvx, *command]))
        if case == "mixed":
            print("Mixed, MCP comparison scope: expected PASS (exit 0), one local comparison excluded.")
            print(shlex.join(["fr", *command, "--answers", "mcp"]))
            print(shlex.join([*uvx, *command, "--answers", "mcp"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
