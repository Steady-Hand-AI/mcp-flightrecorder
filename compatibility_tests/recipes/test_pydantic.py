"""A1: actual Pydantic routing, the clone gap, and explicitly expected zero work."""
from functools import wraps
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import httpx2
from openai import AsyncOpenAI
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder, inspect
from mcp_flightrecorder.integrations.openai_chat import ModelEvidence, wrap_model

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("pydantic_recipe", ROOT / "examples/pydantic_openai.py")
recipe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recipe)


def public_rows(path, sql):
    envelope = inspect.query(path, sql, limit=100)
    assert envelope["ok"], f"The existing public read-only query must succeed before comparison: {envelope.get('error')}."
    data = envelope["data"]
    assert not data["has_more"], "The small recipe oracle requires the complete selected row population."
    return [dict(zip(data["columns"], row)) for row in data["rows"]]


def public_body(path, run_id, artifact):
    envelope = inspect.payload(path, run_id, artifact, max_bytes=65536)
    assert envelope["ok"], f"The content fixture requires an available retained artifact: {envelope.get('error')}."
    data = envelope["data"]
    assert data["next_offset"] is None, "Each bounded recipe artifact must fit the complete public payload read."
    return json.loads(data["text"])


def reconstructed(path, event):
    manifest = public_body(path, event["run_id"], event["payload_ref"])
    assert manifest["format"] == "request_refs/2", "A projectable request must use the public flat manifest before reconstruction."
    body = public_body(path, event["run_id"], manifest["template_ref"])
    body["messages"] = [public_body(path, event["run_id"], ref) for ref in manifest["message_refs"]]
    for field in manifest["field_refs"]:
        assert field["pointer"] == "/tools", "The recipe has only the supported tool catalog extraction, if any."
        body["tools"] = public_body(path, event["run_id"], field["artifact_id"])
    return body


async def native_route(*, recorder=None, clone=False, invoke=True):
    """Test-owned native input/output ledger, independent of recorded events."""
    ledger = {"http_requests": [], "observer_invocations": 0, "output": None}

    def handler(request):
        ledger["http_requests"].append(json.loads(request.content))
        return recipe.provider_reply(request)

    async with AsyncOpenAI(api_key="fixture-only-not-a-secret", base_url="https://fixture.invalid/v1",
                           max_retries=0, http_client=httpx2.AsyncClient(
                               transport=httpx2.MockTransport(handler), trust_env=False)) as client:
        native = client.chat.completions.create
        if recorder is not None:
            observed = wrap_model(recorder, native, instance_id="pydantic-openai")

            @wraps(native)
            def create(**kwargs):
                ledger["observer_invocations"] += 1
                return observed(evidence=ModelEvidence(), **kwargs)

            client.chat.completions.create = create
        model = OpenAIChatModel("gpt-4o", provider=OpenAIProvider(openai_client=client))
        agent = Agent(model, system_prompt=recipe.INSTRUCTION)
        if invoke:
            options = {"model_settings": {"max_retries": 0}} if clone else {}
            ledger["output"] = (await agent.run(recipe.PROMPT, **options)).output
    return ledger


def test_positive_recipe_exact_route(evidence_path, run_sdk_async, monkeypatch):
    path = evidence_path / "recording.sqlite"
    native = run_sdk_async(native_route())
    actual = {"http_requests": [], "host_inputs": [], "host_outputs": []}
    reply = recipe.provider_reply
    native_run = Agent.run

    def observe_transport(request):
        actual["http_requests"].append(json.loads(request.content))
        return reply(request)

    @wraps(native_run)
    async def observe_host(self, *args, **kwargs):
        actual["host_inputs"].append({"args": list(args), "kwargs": kwargs})
        result = await native_run(self, *args, **kwargs)
        actual["host_outputs"].append(result.output)
        return result

    monkeypatch.setattr(recipe, "provider_reply", observe_transport)
    monkeypatch.setattr(Agent, "run", observe_host)
    envelope = run_sdk_async(recipe.record_example(path))
    check.equal(actual["http_requests"], native["http_requests"], msg="The executable recipe submits exactly the native Pydantic/OpenAI body without changing settings or messages.")
    check.equal(len(actual["http_requests"]), 1, msg="The fixture deliberately disables retries, so one native logical call has one HTTP attempt.")
    check.equal(actual["host_outputs"], ["ready ✓"], msg="The actual host output remains the fixed Unicode native result.")
    check.equal(native["output"], "ready ✓", msg="The undecorated host independently validates the same fixture result.")
    check.equal(actual["host_inputs"], [{"args": [recipe.PROMPT], "kwargs": {}}], msg="The positive route does not insert a clone-triggering per-run settings override.")
    events = public_rows(path, "SELECT event_id,run_id,turn_id,model_call_id,kind,payload_ref FROM events ORDER BY sequence")
    check.equal([row["kind"] for row in events], ["model.request", "model.response"], msg="One positive call produces one request, one response, and zero fabricated decisions.")
    assert len(events) == 2, "Exact identity and body comparisons require the admitted request/response pair."
    check.equal(reconstructed(path, events[0]), actual["http_requests"][0], msg="The public manifest/payload readers reconstruct the complete submitted values in this deliberately no-retry fixture.")
    check.equal({(e["run_id"], e["turn_id"], e["model_call_id"]) for e in events},
                {(events[0]["run_id"], events[0]["turn_id"], events[0]["model_call_id"])},
                msg="The model request and response retain the same run, invocation turn and model-call identity.")
    check.is_true(all(events[0][name] for name in ("run_id", "turn_id", "model_call_id")), msg="The positive recipe exposes actual admitted identities rather than matching null values.")
    turns = public_rows(path, "SELECT run_id,turn_id,prompt_ref,coverage_json FROM turns")
    assert len(turns) == 1, "The setup recipe has exactly one native host turn."
    check.equal(public_body(path, turns[0]["run_id"], turns[0]["prompt_ref"]), recipe.PROMPT, msg="The original host input is retained independently of submitted provider messages.")
    check.is_false(json.loads(turns[0]["coverage_json"])["model_complete"], msg="The smoke recipe explicitly declines universal model-route coverage.")
    assert envelope["ok"], "A successful example returns the existing inspection envelope for this recording."
    checks = {item["id"]: item for item in envelope["data"]["items"]}
    check.equal(checks["coverage.model"]["status"], "pass", msg="Expect-model checks the declared producer, separately from route-call counts.")
    check.equal(envelope["data"]["status"], "unknown", msg="Successful native execution does not promote model_complete=false to an overall reader PASS.")


@pytest.mark.parametrize("mode,expected_calls", [("clone", 1), ("zero", 0)])
def test_clone_and_expected_zero_controls(evidence_path, run_sdk_async, mode, expected_calls):
    path = evidence_path / "recording.sqlite"
    recorder = Recorder(path, capture="content", coverage=recipe.COVERAGE)
    run_id = recorder.capture_scope().run_id
    invoke = mode != "zero"

    async def scenario():
        native = await native_route(clone=mode == "clone", invoke=invoke)
        with recorder.turn(recipe.PROMPT):
            observed = await native_route(recorder=recorder, clone=mode == "clone", invoke=invoke)
        return native, observed

    try:
        native, observed = run_sdk_async(scenario())
    finally:
        closed = recorder.close(timeout=3)
        assert closed.writer_stopped and closed.portable, "Even negative/zero-call controls must retain a stopped portable recording."
    envelope = inspect.check(path, run_id, expect=("model",))
    check.equal(len(observed["http_requests"]), expected_calls, msg="The independently declared scenario distinguishes one successful clone call from intentionally absent native work.")
    check.equal(observed["http_requests"], native["http_requests"], msg="Observation leaves clone/zero route submission unchanged.")
    check.equal(observed["output"], "ready ✓" if invoke else None, msg="The clone succeeds natively; the zero-call control does not manufacture an output.")
    check.equal(observed["output"], native["output"], msg="Both controls preserve the actual native outcome.")
    check.equal(observed["observer_invocations"], 0, msg="Neither a derived cloned callable nor deliberately uninvoked work enters the original bound-method observer.")
    check.equal(public_rows(path, "SELECT kind FROM events"), [], msg="Absent observer calls create no fabricated model requests, responses or decisions.")
    assert envelope["ok"], "The public reader can inspect both valid route-control recordings."
    checks = {item["id"]: item for item in envelope["data"]["items"]}
    check.equal(checks["coverage.model"]["status"], "pass", msg="A producer declaration can pass even for the clone gap; route correctness is proved by the independent call ledger.")
    check.equal(envelope["data"]["status"], "unknown", msg="Neither clone bypass nor expected zero work asserts complete universal instrumentation.")


def test_example_cli_uses_existing_envelope(evidence_path):
    path = evidence_path / "recording.sqlite"
    argv = [sys.executable, "-I", "-B", str(ROOT / "examples/pydantic_openai.py"), str(path)]
    result = subprocess.run(argv, text=True, capture_output=True, timeout=30, check=False)
    (evidence_path / "stdout.log").write_text(result.stdout)
    (evidence_path / "stderr.log").write_text(result.stderr)
    check.equal(result.returncode, 0, msg="The documented isolated example command must complete successfully in the supplied recipe profile.")
    assert result.returncode == 0, "Only successful execution can provide the emitted public envelope for comparison."
    envelope = json.loads(result.stdout)
    runs = public_rows(path, "SELECT run_id FROM runs")
    assert len(runs) == 1, "The executable creates one recording before its printed report is inspected."
    expected = inspect.check(path, runs[0]["run_id"], expect=("model",))
    check.equal(envelope, expected, msg="The example prints the existing reader JSON envelope exactly, with no new installation-verifier result schema.")
    check.equal(envelope["data"]["stats"]["model_requests"], 1, msg="The executable command itself records one real native request.")
    check.equal(envelope["data"]["stats"]["model_responses"], 1, msg="The executable command itself records one normal response.")
    check.equal(envelope["data"]["stats"]["decisions"], 0, msg="The fixed text response does not fabricate tool selections.")
