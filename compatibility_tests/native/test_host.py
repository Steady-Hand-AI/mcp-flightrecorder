"""C1–C5/C8: native host scenarios with explicitly unknown receipt propagation."""
from collections import Counter
import json

import pytest
from pytest_check import check

from .host_evidence import artifact_body, events, operations, relations
from .host_fixture import OPTIONS, PROMPT, SYSTEM, run_pair


def native_calls(report):
    return [event for event in report["server_events"] if event["method"] == "tools/call"]


def unchanged_host(plain, report):
    check.equal(report["outputs"], plain["outputs"],
                msg="Packaged observers preserve the final output of the actual installed native host.")
    check.equal(report["wire"], plain["wire"],
                msg="Native Ollama HTTP requests and NDJSON replies are identical with and without the packaged observers.")
    check.equal(report["server_events"], plain["server_events"],
                msg="The independent server ledger preserves discovery, tool identities, arguments and invocation multiplicity.")
    check.equal(report["protocol"], plain["protocol"],
                msg="Actual ClientSession result projections and tool inputs match the unobserved native control.")
    check.equal(report["history"], plain["history"],
                msg="The observer preserves the host's own conversation-history policy and final answers.")
    check.equal(report["cache_reads"], plain["cache_reads"],
                msg="Observing the actual cache getter neither adds reads nor changes their ordered catalog values.")


def recording_contract(report):
    check.equal(report["close"], {"writer_stopped": True, "run_close_state": "incomplete",
                                  "portable": True, "reason": None},
                msg="Stock-host missing answer receipts remain visible as incomplete capture in a closed portable recording.")
    check.equal(report["database"]["integrity_check"], ["ok"],
                msg="The retained native evidence passes SQLite integrity after the writer closes.")
    check.equal(report["database"]["foreign_key_check"], [],
                msg="The native recording contains no dangling foreign keys.")
    check.equal(report["database"]["journal_mode"], "delete",
                msg="The delivered native recording reopens without a hidden WAL dependency.")
    check.equal(report["source_sha256_before"], report["source_sha256_after"],
                msg="Public inspection, content reconstruction and the installed snapshot command do not mutate the source recording.")
    check.is_true(report["snapshot"]["rows_equal"],
                  msg="The portable copy reopens with every source row and view unchanged.")
    check.equal(report["snapshot"]["database"]["integrity_check"], ["ok"],
                msg="The independently reopened portable copy passes integrity checks.")
    check.equal(report["snapshot"]["database"]["foreign_key_check"], [],
                msg="Snapshotting preserves all operation and artifact foreign keys.")
    check.equal(report["snapshot"]["check"], report["check"],
                msg="The public reader reports the same facts and explicit uncertainty on source and portable copy.")
    check.is_true(all(receipt["status"] == "accepted" for receipt in report["receipts"]),
                  msg="All bounded explicit host observations are admitted instead of silently rejected.")
    model_receipts = [receipt for item in report["model_evidence"] for receipt in (item["request"], item["response"])]
    check.is_true(all(receipt is not None and receipt["status"] == "accepted" for receipt in model_receipts),
                  msg="Every real native model invocation receives one admitted request and one terminal response.")
    operation_receipts = [receipt for item in report["operation_evidence"] for receipt in (item["start"], item["terminal"])]
    check.is_true(all(receipt is not None and receipt["status"] == "accepted" for receipt in operation_receipts),
                  msg="Both packaged session methods admit their original logical operation lifecycle.")
    check.equal(report["tables"]["runs"][0]["dropped_events"], 0,
                msg="Unknown provenance in this fixture is not an event-capacity loss.")
    assert report["check"]["ok"], "The public inspection reader must successfully consume the recording before evaluating its structural result."
    check.equal(report["check"]["data"]["required_failures"], [],
                msg="The native recording contains no contradictory lifecycle, identity or retained structure.")
    check.is_in("answer_bindings", report["check"]["data"]["required_unknowns"],
                msg="Public checks expose the stock host's absent result-receipt seam as unknown answer bindings.")
    check.equal([len(relations(report, relation)) for relation in
                 ("dispatch_source", "answers", "inserted_into", "transformed_from")], [0, 0, 0, 0],
                msg="Stock native execution creates no guessed dispatch, answer, transformation or insertion correspondence.")
    check.equal(report["tables"]["answer_occurrences"], [],
                msg="Generated provider UUIDs and equal content do not manufacture known submitted-answer occurrences.")
    check.is_true(all(row["decision_id"] is None and row["model_call_id"] is None
                      for row in report["tables"]["operations"]),
                  msg="Native operations retain null decision/model IDs because the host propagates no recorder receipt.")
    check.is_true(all(row["resolution"] == "unknown" and row["input_rounds"] is None
                      for row in report["tables"]["operations"]),
                  msg="The high-level method and witnessed cache getter do not infer operation resolution or internal input rounds.")
    check.equal(len(events(report, "model.request")), len(report["requests"]),
                msg="There is exactly one captured model request for every independently witnessed native invocation.")
    check.equal(len(events(report, "model.response")), len(report["requests"]),
                msg="Actual EOF plus repeated native cleanup produces exactly one response per model invocation.")
    check.is_true(all(row["state"] == "complete" for row in events(report, "model.response")),
                  msg="The native host actually exhausts these normalized streams; provenance uncertainty does not invent a stream error.")
    later_requests = [row for row in events(report, "model.request")
                      if "binding_not_supplied" in row["details_json"]]
    check.is_true(bool(later_requests),
                  msg="Submitted tool messages explicitly retain the adapter's unresolved binding diagnostic.")
    if report["capture"] == "content":
        check.equal(report["reconstructed_requests"], report["requests"],
                    msg="Documented public payload reads reconstruct every fully projectable request to the exact submitted native SDK values, including generated IDs.")
        check.is_true(all(artifact_body(report, row["payload_ref"])["format"] == "anyllm_ollama_stream/1"
                          for row in events(report, "model.response")),
                      msg="Consumed normalized output is labelled with its actual AnyLLM/Ollama stream boundary.")
        expected = Counter(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in report["protocol"])
        actual = []
        by_id = {row["event_id"]: row for row in report["tables"]["events"]}
        for row in report["tables"]["operations"]:
            start, terminal = by_id[row["start_event_id"]], by_id[row["end_event_id"]]
            actual.append({"method": row["method"], "server": row["server_key"], "name": row["name"],
                           "arguments": artifact_body(report, start["payload_ref"])["arguments"],
                           "result": artifact_body(report, terminal["payload_ref"])})
        check.equal(Counter(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in actual), expected,
                    msg="Independently logged native method inputs/results reconcile with the recorded operation bodies without creating provenance links.")
    else:
        check.is_true(all(row["body_text"] is None for row in report["tables"]["artifacts"]
                          if row["representation"] != "request_refs/2"),
                      msg="Metadata mode omits prompts, catalogs, arguments, results and consumed chunks from stored bodies.")
        check.is_true(all(not item["ok"] and item["error"]["code"] == "payload_omitted"
                          for item in report["metadata_template_reads"]),
                      msg="Public payload reads report configured omission instead of claiming reconstructable submitted bodies.")
        check.is_not_in(PROMPT, json.dumps(report["tables"], ensure_ascii=False),
                        msg="The omitted original prompt is not copied into metadata or diagnostic columns.")


@pytest.mark.parametrize("capture", ["content", "metadata"])
def test_native_host_capture_contract(evidence_path, run_sdk_async, capture):
    plain, report = run_sdk_async(run_pair(evidence_path, "basic", capture))
    unchanged_host(plain, report)
    recording_contract(report)
    check.equal(len(report["requests"]), 2,
                msg="The native loop submits its decision request and its actual tool-result follow-up exactly once each.")
    check.equal(report["requests"][0]["messages"], [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": PROMPT}],
                msg="The original system and user values reach the installed AnyLLM method unchanged.")
    catalog = report["requests"][0]["tools"]
    check.equal(catalog[0]["function"]["name"], "review_echo",
                msg="The model sees the host's real sanitized name rather than an invented adapter alias.")
    check.equal(catalog[0]["function"]["parameters"]["properties"]["x"]["type"], "integer",
                msg="The server's integer schema survives native discovery, cache and provider preparation.")
    check.equal([(row["name"], row["status"]) for row in operations(report)], [("echo", "ok")],
                msg="The actual native tool return retains its original logical operation outcome.")
    check.equal(len(events(report, "model.tool_call")), 1,
                msg="The SDK stream exposes exactly one selection, without a second post-parser decision event.")
    check.equal(len(events(report, "cache.served")), 3,
                msg="Three actual ToolManager getter reads are positively witnessed; absence of network traffic is not the cache evidence.")
    if capture == "content":
        check.equal(artifact_body(report, report["tables"]["turns"][0]["prompt_ref"]), PROMPT,
                    msg="Content mode retains the original turn input independently of later native message construction.")
        check.equal([artifact_body(report, row["catalog_ref"]) for row in events(report, "model.request")], [catalog, catalog],
                    msg="Both model requests retain the exact ordered catalog actually submitted by the host.")


def test_repeated_turn_uses_witnessed_cache_without_inferred_answers(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "repeated"))
    unchanged_host(plain, report)
    recording_contract(report)
    check.equal(len(operations(report, "tools/list")), 1,
                msg="Two real native turns use one outer discovery operation and do not fabricate schema refreshes.")
    check.equal(len(report["requests"]), 4,
                msg="Each real turn performs its decision and follow-up model calls, totaling four requests.")
    check.equal(len(events(report, "cache.served")), 6,
                msg="The repeated-turn fixture records all six actual cache getter observations.")
    check.equal(len(events(report, "model.tool_call")), 2,
                msg="Equal selected arguments across turns remain two independently observed selection occurrences.")
    check.is_false(any(item["role"] == "tool" for item in report["requests"][2]["messages"]),
                   msg="The native second-turn history really omits earlier tool messages; the observer cannot invent them.")
    check.equal(len({row["decision_id"] for row in events(report, "model.tool_call")}), 2,
                msg="Repeated values do not collapse distinct recorder decisions.")


def test_colliding_sanitized_names_keep_native_server_routes_distinct(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "collision"))
    unchanged_host(plain, report)
    recording_contract(report)
    check.equal([tool["function"]["name"] for tool in report["requests"][0]["tools"]],
                ["alpha_beta_echo", "alpha_beta_echo_2"],
                msg="The real host preserves both distinct model-facing names when sanitization collides.")
    check.equal([row["name"] for row in events(report, "model.tool_call")],
                ["alpha_beta_echo", "alpha_beta_echo_2"],
                msg="The adapter records the consumed native model names before the host maps them to server routes.")
    check.equal([row["name"] for row in events(report, "host.dispatch")],
                ["alpha.beta_echo", "alpha_beta.echo"],
                msg="Explicit execute observations retain the actual qualified host targets.")
    check.equal([(row["server_key"], row["name"]) for row in operations(report)],
                [("alpha", "beta_echo"), ("alpha_beta", "echo")],
                msg="Both actual MCP routes remain distinct: alpha/beta_echo and alpha_beta/echo.")
    check.equal([event["arguments"] for event in native_calls(report)], [{"x": 7}, {"x": 7}],
                msg="Equal argument objects reach two real server processes without merging occurrences.")


def test_native_refusal_preserves_selection_without_mcp_operation(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "skip"))
    unchanged_host(plain, report)
    recording_contract(report)
    check.equal(operations(report), [],
                msg="A decline at the actual host confirmation callback prevents native tool execution.")
    check.equal(native_calls(report), [],
                msg="The independent stdio server ledger confirms that no refused tool ran.")
    check.equal([event["state"] for event in events(report, "host.dispatch")], ["refuse"],
                msg="The observed user decline is refusal, not a fabricated remote error.")
    check.equal(len(events(report, "model.tool_call")), 1,
                msg="The consumed model selection survives the later host refusal.")
    check.equal(json.loads(report["outputs"][0])["contents"], ["Tool call was skipped by user"],
                msg="The unmodified host submits its own refusal explanation to the follow-up call.")


def test_native_tool_error_preserves_returned_error_flag(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "tool_error"))
    unchanged_host(plain, report)
    recording_contract(report)
    check.equal([row["status"] for row in operations(report)], ["tool_error"],
                msg="A returned native CallToolResult with isError remains a tool error rather than transport failure.")
    check.equal(native_calls(report)[0]["arguments"], {"x": -1},
                msg="The independent server receives the deliberately invalid positive-input value unchanged.")
    check.equal(json.loads(report["outputs"][0])["contents"], ["positive input required"],
                msg="The host submits the real error text without alteration by the observer.")


def test_structured_only_result_preserves_empty_content_without_provenance_guess(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "structured"))
    unchanged_host(plain, report)
    recording_contract(report)
    operation = operations(report)[0]
    terminal = next(row for row in report["tables"]["events"] if row["event_id"] == operation["end_event_id"])
    body = artifact_body(report, terminal["payload_ref"])
    check.equal(body["content"], [],
                msg="The actual SDK result exposes an explicit empty content list, not an omitted required field.")
    check.equal(body["structuredContent"], {"server": "review", "x": 7, "unit": "µ"},
                msg="The structured protocol value is retained independently of the host's later string conversion.")
    check.equal(json.loads(report["outputs"][0])["contents"], ['{"server":"review","x":7,"unit":"µ"}'],
                msg="The native host's compact Unicode serialization is the real submitted tool message.")


def test_native_empty_aliases_leave_all_answer_origins_unknown(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "empty_alias"))
    unchanged_host(plain, report)
    recording_contract(report)
    check.equal(len(operations(report)), 2,
                msg="Two same-name same-argument native selections still execute two actual logical operations.")
    check.equal(len(events(report, "model.tool_call")), 2,
                msg="The native outer SDK iterator exposes two distinct tool indices even with equal argument values.")
    check.equal(json.loads(report["outputs"][0])["contents"], ["", ""],
                msg="Both real empty results reach the model while receipt identity remains explicitly unavailable.")
    check.equal(events(report, "host.tool_result"), [],
                msg="The stock harness does not turn aliased empty strings or parser order into transformation receipts.")


def test_resource_read_retains_original_directive_and_submitted_context(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "resource"))
    unchanged_host(plain, report)
    recording_contract(report)
    reads = [row for row in report["server_events"] if row["method"] == "resources/read"]
    check.equal(reads, [{"method": "resources/read", "server": "review", "uri": "review://context"}],
                msg="The native inline-resource route performs one real read, not only resource discovery.")
    check.equal(len(operations(report, "resources/read")), 1,
                msg="The explicit resource observation retains the actual read operation and its exposed response.")
    check.equal(artifact_body(report, report["tables"]["turns"][0]["prompt_ref"]), PROMPT + " @review://context",
                msg="The turn preserves the original resource directive before native query parsing.")
    text = "Resource witness: amount=7; unit=µ; origin=review"
    check.is_true(any(text in message.get("content", "") for message in report["requests"][0]["messages"]),
                  msg="The actual AnyLLM request receives the host's fetched Unicode context.")
    wire = next(row["request"] for row in report["wire"] if row["path"] == "/api/chat")
    check.is_true(any(text in message.get("content", "") for message in wire["messages"]),
                  msg="The independent HTTP ledger confirms the same resource content reaches Ollama after native normalization.")
    check.equal(len(report["history"]), 2,
                msg="Successful native resource injection retains its context entry and completed query.")


def test_configured_ollama_options_are_submitted_and_reconstructed(evidence_path, run_sdk_async):
    plain, report = run_sdk_async(run_pair(evidence_path, "options"))
    unchanged_host(plain, report)
    recording_contract(report)
    check.equal([{key: row[key] for key in OPTIONS} for row in report["requests"]], [OPTIONS, OPTIONS],
                msg="Both native AnyLLM invocations receive exactly the configured num_ctx, top_k and num_predict values.")
    wire_options = [row["request"]["options"] for row in report["wire"] if row["path"] == "/api/chat"]
    check.equal([{key: row[key] for key in OPTIONS} for row in wire_options], [OPTIONS, OPTIONS],
                msg="The actual installed Ollama conversion forwards those supported provider kwargs into both HTTP request option objects.")
