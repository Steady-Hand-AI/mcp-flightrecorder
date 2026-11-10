"""Small text views of existing bounded inspection results, with IDs intact."""
import json


def _label(value):
    # Recording labels are untrusted; escape controls, including terminal ESC.
    return json.dumps(value, ensure_ascii=False)


def render(result, max_output_bytes=65536):
    lines = []
    if not result["ok"]:
        error = result["error"]
        lines.append("Inspection unavailable: " + error["code"] + " (" + error["reason"] + ")")
        lines.append(error["message"])
    else:
        data = result["data"]
        lines.append("Run " + _label(result["scope"].get("run_id")))
        if result["operation"] == "diagnose":
            lines.append("Recorded observations — not a whole-run verdict or proof of answer correctness.")
            lines.append("Events inspected: " + str(data["evaluated_events"]) + "; event limit: " + str(data["event_limit"]))
            if not data["diagnostics"]:
                lines.append("No diagnostic items in this bounded view; this does not establish complete capture.")
            for item in data["diagnostics"]:
                lines.extend([item["code"] + " at " + _label(item["location"]),
                              "  Observed: " + item["observed"], "  Try: " + item["hint"]])
            for item in data["preparations"]:
                lines.append("Preparation " + _label(item["prepared_event_id"]) + " -> request " +
                             _label(item["request_event_id"]) + " at " + _label(item["target_pointer"]))
                if item["state"] == "matches":
                    lines.append("  Host declared preparation; prepared text matches submitted text.")
                elif item["state"] == "differs":
                    lines.append("  Host declared preparation; prepared text differs from submitted text.")
                else:
                    lines.append("  Preparation comparison unknown: " + _label(item["reason"]))
                lines.append("  Raw result text_equal=" + _label(item["raw_text_equal"]) + "; semantic equivalence not verified.")
            if data["events_remaining"] or data["diagnostics_omitted"] or data["preparations_omitted"]:
                lines.append("CUTOFF: more events or diagnostic items exist; select a turn or raise the bounded limit.")
        elif result["operation"] == "find_turns":
            for index, item in enumerate(data["items"], 1):
                turn = item["turn"]
                lines.append("Turn " + str(index) + " (page label) " + _label(turn["turn_id"]) +
                             "  started=" + _label(turn["started_at"]) + "  capture=" + _label(turn["capture_state"]))
                lines.append("  Counts " + _label(item["counts"]) + "  Flags " + _label(item["flags"]))
            if not data["items"]:
                lines.append("No matching recorded turns.")
            if data["has_more"]:
                lines.append("CUTOFF: more turns exist; continue with --cursor " + _label(data["next_cursor"]))
        elif result["operation"] == "explain":
            lines.append("Turn " + _label(data["turn"]["turn_id"]) + "  capture=" + _label(data["turn"]["capture_state"]))
            lines.append("Counts " + _label(data["counts"]) + "  Flags " + _label(data["flags"]))
            for index, call in enumerate(data["calls"], 1):
                lines.append("Model call " + str(index) + " " + _label(call["model_call_id"]) + "  complete=" + _label(call["complete"]))
            detail = data["detail"]
            if detail is not None:
                for event in detail["events"]:
                    lines.append("  " + str(event["sequence"]) + " " + _label(event["kind"]) + " " + _label(event["event_id"]))
                for answer in detail["input_answers"]:
                    lines.append("  Answer at " + _label(answer["target_pointer"]) + " raw text_equal=" + _label(answer["text_equal"]) + " reason=" + _label(answer["comparison_reason"]))
                for link in detail["links"]:
                    lines.append("  " + _label(link["relation"]) + " " + _label(link["source_event_id"]) + " -> " + _label(link["target_event_id"]))
            lines.append("Use --model-call ID for call detail; doctor --turn ID explains recorded gaps and preparation.")
        for item in result["unknowns"]:
            lines.append("UNKNOWN: " + _label(item["code"]) + " " + _label(item["ids"]))
        for item in result["cutoffs"]:
            lines.append("CUTOFF: " + _label(item["code"]) + " at " + _label(item["path"]))
        if any(result["diagnostics_omitted"].values()):
            lines.append("CUTOFF: additional reader diagnostics omitted " + _label(result["diagnostics_omitted"]))
    text = "\n".join(lines) + "\n"
    if len(text.encode("utf-8")) > max_output_bytes:
        notice = "\nCUTOFF: text byte budget reached; use JSON or a narrower selection.\n"
        budget = max(0, max_output_bytes - len(notice.encode("utf-8")))
        text = text.encode("utf-8")[:budget].decode("utf-8", errors="ignore") + notice
    return text
