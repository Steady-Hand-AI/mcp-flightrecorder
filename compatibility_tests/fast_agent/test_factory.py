"""Public native host routes plus labelled construction-boundary doubles."""
import json
import re
from types import SimpleNamespace

import pytest
import pytest_check as check

from .host_fixture import (BASE_MODEL, CLONE_MODEL, example, reconstructed_requests,
                           run_case, history_without_runtime_timing)


@pytest.mark.parametrize("route", ["normal", "detached", "reattached"])
def test_native_factory_routes_capture_each_actual_submission(route, tmp_path, monkeypatch, evidence_path, run_sdk_async):
    monkeypatch.chdir(tmp_path)
    plain, plain_ledger = run_case(route, evidence_path / "plain", False, run_sdk_async)
    observed, ledger = run_case(route, evidence_path / "observed", True, run_sdk_async)
    database = json.loads((evidence_path / "observed/database.json").read_text())
    requests = [row for row in database["events"] if row["kind"] == "model.request"]
    responses = [row for row in database["events"] if row["kind"] == "model.response"]
    selections = [row for row in database["events"] if row["kind"] == "model.tool_call"]
    check.equal(history_without_runtime_timing(observed), history_without_runtime_timing(plain), msg="The native answer and every history field except execution timestamps and the two named runtime timing channels must remain identical.")
    check.equal(observed["results"], ["owned-complete"], msg="The native host must return the independently scripted terminal answer.")
    check.equal([x["body"] for x in ledger["http"]], [x["body"] for x in plain_ledger["http"]], msg="Plain and recorded routes must submit identical provider HTTP bodies in the same order.")
    check.equal(len(ledger["http"]), 2, msg="The one-tool fixture must make its tool-choice request and result-followup request at the actual HTTP boundary.")
    check.equal(len(requests), 2, msg="The selected native factory must record both actual submissions exactly once, including after explicit clone reattachment.")
    check.equal(len(responses), 2, msg="Both consumed native streams must have exactly one terminal capture.")
    check.equal(len(selections), 1, msg="The one scripted tool selection must remain one occurrence.")
    check.equal(reconstructed_requests(database, requests), [x["body"] for x in ledger["http"]], msg="Recorded prompt, tool schemas and subsequent tool context must reconstruct every actual request byte-for-byte as JSON.")
    check.equal([row["state"] for row in responses], ["complete", "complete"], msg="Both fixture streams expose their terminal finish reason before EOF.")
    check.equal([row["name"] for row in selections], ["owned__echo"], msg="The selected tool must retain the native host's model-facing namespace.")
    selected_model = CLONE_MODEL if route == "reattached" else BASE_MODEL
    check.equal([x["body"]["model"] for x in ledger["http"]], [selected_model] * 2, msg="Factory delegation must keep the owner's selected native model after cloning.")
    check.equal(len(ledger["client_construction"]), 2, msg="Each native completion request constructs a fresh client through the selected factory.")
    check.equal(ledger["distinct_client_instances"], 2, msg="Two different actual client instances must be observed, preventing a stale bound-create attachment from passing.")
    check.equal(ledger["clients_closed"], [True, True], msg="The native host must retain context-manager ownership and close each constructed client.")
    check.equal(ledger["client_construction"], plain_ledger["client_construction"], msg="Recording must not change native client construction multiplicity or SDK/LLM types.")
    instance_ids = [json.loads(row["details_json"])["instance_id"] for row in requests]
    check.equal(len(set(instance_ids)), 2, msg="Each newly constructed observed client must receive its own occurrence label.")
    check.is_true(all(re.fullmatch(r"owned-model:[0-9a-f]{32}", value) for value in instance_ids), msg="Generated instance labels must use the owner prefix plus one UUID hex suffix.")
    server = [json.loads(line) for line in (evidence_path / "observed/server.jsonl").read_text().splitlines()]
    calls = [row for row in server if row["method"] == "tools/call"]
    check.equal(calls, [{"method": "tools/call", "name": "echo", "arguments": {"value": "same-α"}}], msg="The independent MCP server must witness exactly the selected tool and unchanged arguments.")
    tool_messages = [message for item in ledger["http"] for message in item["body"]["messages"] if message["role"] == "tool"]
    check.equal([message["content"] for message in tool_messages], ["fixture-echo:same-α"], msg="The second model submission must contain the actual server result, not a fixture-induced substitute error.")
    check.equal(database["operations"], [], msg="This maintained recipe captures model methods only and must not fabricate an MCP operation.")
    check.equal(database["links"], [], msg="Equal provider IDs or returned content cannot create missing host dispatch/result relationships.")
    check.equal(database["integrity_check"], "ok", msg="The recording must remain a valid SQLite database after native shutdown.")
    check.equal(database["foreign_key_check"], [], msg="Model capture must preserve all stored identity references.")
    check.equal(database["runs"][0]["dropped_events"], 0, msg="This bounded fixture must not silently drop captured events.")
    check.is_true(database["reader_check"]["ok"], msg="The installed public reader must inspect the completed recording successfully.")
    check.equal(database["reader_check"]["data"]["required_failures"], [], msg="A successful read envelope must not conceal a known evidence failure.")
    check.is_true(bool(database["reader_check"]["data"]["required_unknowns"]), msg="A successful native tool loop without host binding receipts cannot earn complete-chain evidence.")
    check.is_true(all(item["ok"] for item in database["reader_explain"]), msg="Each recorded host turn must remain explainable through the existing public reader.")
    coverage = json.loads(database["runs"][0]["coverage_json"])
    check.is_none(coverage["producers"]["mcp"], msg="The recipe must declare MCP unobserved rather than promoting the test server ledger into recorder coverage.")
    check.is_true(json.loads((evidence_path / "observed/close.json").read_text())["portable"], msg="The recording must close as a standalone portable file.")


def test_unreattached_model_clone_remains_an_honest_negative(tmp_path, monkeypatch, evidence_path, run_sdk_async):
    monkeypatch.chdir(tmp_path)
    plain, plain_ledger = run_case("unreattached", evidence_path / "plain", False, run_sdk_async)
    observed, ledger = run_case("unreattached", evidence_path / "observed", True, run_sdk_async)
    database = json.loads((evidence_path / "observed/database.json").read_text())
    check.equal(history_without_runtime_timing(observed), history_without_runtime_timing(plain), msg="The attachment gap must preserve native results/history apart from independent execution timestamps and measured timing channels.")
    check.equal(observed["results"], ["owned-complete"], msg="The unobserved clone must still complete the independently scripted native tool loop.")
    check.equal(len(ledger["http"]), 2, msg="The external HTTP ledger must prove that both unobserved model submissions actually occurred.")
    check.equal([x["body"] for x in ledger["http"]], [x["body"] for x in plain_ledger["http"]], msg="Omitting reattachment must preserve the actual native request sequence.")
    check.equal(ledger["client_construction"], [], msg="The model-changing clone replaces the old factory and never calls its construction seam.")
    check.equal([row for row in database["events"] if row["kind"].startswith("model.")], [], msg="The recorder must not claim requests or responses from a route which never invokes its observer.")
    check.equal(database["operations"], [], msg="A model-only recipe cannot observe the native MCP calls in this control.")
    check.equal(database["links"], [], msg="Successful native execution supplies no missing causal receipt.")
    check.is_false(json.loads(database["runs"][0]["coverage_json"])["model_complete"], msg="The known unreattached route must explicitly declare incomplete model coverage.")
    check.equal(database["integrity_check"], "ok", msg="A truthful empty model observation remains a structurally valid recording.")
    check.is_true(database["reader_check"]["ok"], msg="The reader must return its normal evidence envelope for the unobserved route.")
    check.is_true(bool(database["reader_check"]["data"]["required_unknowns"]), msg="Zero captured requests must not be misrepresented as proof that no model work occurred.")


def test_repeated_native_sends_keep_new_clients_attached(tmp_path, monkeypatch, evidence_path, run_sdk_async):
    monkeypatch.chdir(tmp_path)
    plain, plain_ledger = run_case("normal", evidence_path / "plain", False, run_sdk_async, sends=2)
    observed, ledger = run_case("normal", evidence_path / "observed", True, run_sdk_async, sends=2)
    database = json.loads((evidence_path / "observed/database.json").read_text())
    requests = [row for row in database["events"] if row["kind"] == "model.request"]
    responses = [row for row in database["events"] if row["kind"] == "model.response"]
    selections = [row for row in database["events"] if row["kind"] == "model.tool_call"]
    check.equal(history_without_runtime_timing(observed), history_without_runtime_timing(plain), msg="Repeated sends must preserve results and accumulated history apart from independent execution timestamps and measured timing channels.")
    check.equal(observed["results"], ["owned-complete", "owned-complete"], msg="Both separately scoped native sends must finish their one-tool fixture.")
    check.equal([x["body"] for x in ledger["http"]], [x["body"] for x in plain_ledger["http"]], msg="Recording must preserve the repeated host's exact submission order and history.")
    check.equal(len(ledger["http"]), 4, msg="Two one-tool sends must produce four real provider submissions.")
    check.equal((len(requests), len(responses), len(selections)), (4, 4, 2), msg="Repeated construction must not miss, duplicate or merge model occurrences.")
    check.equal(reconstructed_requests(database, requests), [x["body"] for x in ledger["http"]], msg="All later client instances must retain their own actual submitted context.")
    check.equal(ledger["distinct_client_instances"], 4, msg="The same decorated factory must observe four newly constructed SDK clients.")
    check.equal(ledger["clients_closed"], [True] * 4, msg="Every client remains owned and closed by the native host lifecycle.")
    check.equal(len({json.loads(row["details_json"])["instance_id"] for row in requests}), 4, msg="Distinct client constructions require distinct instance labels across repeated sends.")
    check.equal(len({row["decision_id"] for row in selections}), 2, msg="Equal repeated tool arguments remain separate selection occurrences.")
    check.equal([len([event for event in requests if event["turn_id"] == turn["turn_id"]]) for turn in database["turns"]], [2, 2], msg="The native sends must retain the outer host scope captured by each actual invocation.")


class StringSubclass(str):
    pass


@pytest.mark.parametrize("prefix", [
    pytest.param(None, id="null"), pytest.param("", id="empty"),
    pytest.param(23, id="integer"), pytest.param(b"name", id="bytes"),
    pytest.param(StringSubclass("name"), id="str-subclass"),
    pytest.param("\ud800", id="surrogate"), pytest.param("x" * 224, id="ascii-over-limit"),
    pytest.param("é" * 112, id="utf8-over-limit"),
])
def test_invalid_prefix_precedes_native_factory_construction(prefix, monkeypatch):
    """Label validation uses a labelled factory double; no native work is needed."""
    native_calls = []

    def unexpected(model_spec):
        native_calls.append(model_spec)
        raise RuntimeError("Native factory construction must not happen for an invalid prefix.")

    monkeypatch.setattr(example.ModelFactory, "create_factory", unexpected)
    with pytest.raises(ValueError, match="instance_prefix"):
        example.make_recorded_llm_factory(object(), BASE_MODEL, instance_prefix=prefix)
    check.equal(native_calls, [], msg="All invalid prefix forms must fail before any native factory or client construction.")


@pytest.mark.parametrize("prefix", [pytest.param("a", id="short"), pytest.param("a" * 223, id="ascii-limit"),
                                    pytest.param("é" * 111 + "a", id="utf8-limit")])
def test_valid_prefix_keeps_native_factory_result_and_boundaries(prefix, monkeypatch):
    """Label boundaries use explicit native construction doubles and a wrap spy."""
    clients = []
    captures = []
    original_create = lambda: None

    class NativeLLM:
        def _openai_client(self):
            client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=original_create)))
            clients.append(client)
            return client

    llm = NativeLLM()
    monkeypatch.setattr(example.ModelFactory, "create_factory", lambda model: lambda: llm)

    def capture(recorder, create, *, instance_id):
        captures.append((recorder, create, instance_id))
        return original_create

    monkeypatch.setattr(example, "wrap_model", capture)
    owner_recorder = object()
    produced = example.make_recorded_llm_factory(owner_recorder, BASE_MODEL, instance_prefix=prefix)()
    client = produced._openai_client()
    check.is_true(produced is llm, msg="Decorating an LLM must return the original native instance.")
    check.is_true(client is clients[0], msg="The construction recipe must return the exact native client.")
    check.equal(len(clients), 1, msg="Each decorated construction invokes the original method exactly once.")
    check.equal(len(captures), 1, msg="Each new client receives exactly one model observer.")
    check.is_true(captures[0][0] is owner_recorder and captures[0][1] is original_create, msg="The observer must receive the original owner and actual returned client's callable.")
    label = captures[0][2]
    check.is_true(label.startswith(prefix + ":"), msg="The owner prefix must be preserved without normalization.")
    check.is_true(bool(re.fullmatch(r"[0-9a-f]{32}", label[len(prefix) + 1:])), msg="The suffix is a UUID hex string, independent of prefix length.")
    check.equal(len(label.encode()), len(prefix.encode()) + 33, msg="UTF-8 label size must reserve exactly one separator and 32 UUID bytes.")
    check.less_equal(len(label.encode()), 256, msg="Accepted prefixes must never overflow the model facade's label limit.")


def test_factory_forwards_arguments_and_owns_each_bound_client_method(monkeypatch):
    """Labelled native doubles expose identity and invocation counts without inference."""
    llms = []
    native_calls = []
    factory_models = []
    captured = []
    clients = []

    class Completion:
        def create(self):
            return self

    class NativeLLM:
        def _openai_client(self):
            client = SimpleNamespace(chat=SimpleNamespace(completions=Completion()))
            clients.append(client)
            return client

    native_class_method = NativeLLM._openai_client

    def create_factory(model):
        factory_models.append(model)

        def native(*args, **kwargs):
            native_calls.append((args, kwargs))
            llm = NativeLLM()
            llms.append(llm)
            return llm

        return native

    def capture(recorder, create, *, instance_id):
        captured.append((create, instance_id))
        return create

    monkeypatch.setattr(example.ModelFactory, "create_factory", create_factory)
    monkeypatch.setattr(example, "wrap_model", capture)
    model = object()
    factory = example.make_recorded_llm_factory(object(), model, instance_prefix="owned")
    positional = object()
    request_params = object()
    options = {"retry_policy": object()}
    produced = [factory(positional, request_params=request_params, options=options) for _ in range(2)]
    returned = [llm._openai_client() for llm in produced for _ in range(2)]
    check.equal(factory_models, [model], msg="The owner model specification must be forwarded once to the native factory resolver.")
    check.equal(len(native_calls), 2, msg="Each returned factory invocation must call the native LLM factory once.")
    check.is_true(all(args == (positional,) and kwargs["request_params"] is request_params and kwargs["options"] is options for args, kwargs in native_calls), msg="Positional arguments and all owner-held keyword objects must be forwarded unchanged.")
    check.is_true(all(actual is expected for actual, expected in zip(produced, llms)), msg="The wrapper must return each corresponding native LLM instance by identity.")
    check.is_true(all(actual is expected for actual, expected in zip(returned, clients)), msg="Each constructed client must be returned exactly once and by identity.")
    check.equal(len(clients), 4, msg="Two LLMs with two constructions each must not create hidden extra clients.")
    check.is_true(all(call[0].__self__ is client.chat.completions for call, client in zip(captured, clients)), msg="Each observer must bind the create method of that newly returned client, never a stale previous client.")
    check.equal(len({label for _, label in captured}), 4, msg="Distinct client construction occurrences need distinct observer labels.")
    check.is_true(NativeLLM._openai_client is native_class_method, msg="The recipe's permitted instance decoration must not patch the native class.")


@pytest.mark.parametrize("stage", ["factory_creation", "llm_construction", "client_construction"])
def test_native_construction_exception_identity_is_preserved(stage, monkeypatch):
    """A labelled construction double verifies native errors are never wrapped or retried."""
    error = RuntimeError("owned-native-construction")
    reached = []

    def step(name):
        reached.append(name)
        if name == stage:
            raise error

    class NativeLLM:
        def _openai_client(self):
            step("client_construction")

    def create_factory(model):
        step("factory_creation")

        def construct():
            step("llm_construction")
            return NativeLLM()

        return construct

    monkeypatch.setattr(example.ModelFactory, "create_factory", create_factory)
    with pytest.raises(RuntimeError) as caught:
        example.make_recorded_llm_factory(object(), BASE_MODEL, instance_prefix="owned")()._openai_client()
    check.is_true(caught.value is error, msg="The native factory/client exception must propagate as the identical exception object.")
    check.equal(reached.count(stage), 1, msg="Recording must not retry a failed native construction.")
