"""Read-only evidence consumers for the native host and explicit receipt controls.

All reconstruction below uses public payload reads and the documented manifest.
No producer helper is used to manufacture expected submitted requests.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

from mcp_flightrecorder import inspect as reader


TABLES = ("runs", "turns", "events", "operations", "links", "artifacts",
          "request_members", "answer_occurrences")


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def receipt_dict(value):
    if value is None:
        return None
    return {key: getattr(value, key) for key in
            ("status", "reason", "event_id", "turn_id", "model_call_id",
             "decision_id", "operation_id", "losses")}


def read_database(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        tables = {}
        for name in TABLES:
            order = " ORDER BY sequence" if name == "events" else ""
            tables[name] = [dict(row) for row in connection.execute("SELECT * FROM " + name + order)]
        integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
        foreign_keys = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]
        mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    return tables, {"integrity_check": integrity, "foreign_key_check": foreign_keys,
                    "journal_mode": mode}


def retained_json(path, run_id, artifact_id):
    envelope = reader.payload(path, run_id, artifact_id, max_bytes=65536,
                              max_output_bytes=1048576)
    assert envelope["ok"], "Request reconstruction requires the documented public payload reader to expose this retained artifact."
    assert envelope["data"]["next_offset"] is None, "The bounded fixture must fit one complete payload read; a truncated prefix is not reconstructed JSON."
    return json.loads(envelope["data"]["text"])


def set_pointer(value, pointer, replacement):
    """Apply the documented manifest's explicit path; never discover identity."""
    parts = [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]
    target = value
    for part in parts[:-1]:
        target = target[int(part) if isinstance(target, list) else part]
    target[int(parts[-1]) if isinstance(target, list) else parts[-1]] = replacement


def reconstruct_request(path, run_id, event):
    manifest = retained_json(path, run_id, event["payload_ref"])
    assert manifest["format"] == "request_refs/2", "Fully available submitted requests must expose the documented request_refs/2 representation."
    result = retained_json(path, run_id, manifest["template_ref"])
    messages = [retained_json(path, run_id, reference) for reference in manifest["message_refs"]]
    set_pointer(result, manifest["message_path"], messages)
    for field in manifest["field_refs"]:
        set_pointer(result, field["pointer"], retained_json(path, run_id, field["artifact_id"]))
    return result


def inspect_recording(path, *, capture):
    """Read, reconstruct and snapshot a closed recording without changing source."""
    path = Path(path).resolve()
    before = file_hash(path)
    tables, database = read_database(path)
    assert len(tables["runs"]) == 1, "Every test-owned recording has one run before its explicit run ID is inspected."
    run_id = tables["runs"][0]["run_id"]
    checks = reader.check(path, run_id, max_output_bytes=1048576)
    requests = [row for row in tables["events"] if row["kind"] == "model.request"]
    request_slices = [reader.slice(path, run_id, row["event_id"], max_output_bytes=1048576)
                      for row in requests]
    reconstructed = ([reconstruct_request(path, run_id, row) for row in requests]
                     if capture == "content" else None)
    omitted_reads = []
    if capture == "metadata":
        for row in requests:
            manifest = retained_json(path, run_id, row["payload_ref"])
            omitted_reads.append(reader.payload(path, run_id, manifest["template_ref"]))
    destination = path.with_name("portable-copy.sqlite")
    command = [str(Path(sys.prefix) / "bin" / "fr"), "snapshot", str(path), str(destination)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=20, check=False)
    assert result.returncode == 0, f"The installed snapshot CLI must successfully copy the closed test recording before copy assertions: {result.stderr}"
    envelope = json.loads(result.stdout)
    assert envelope["ok"], "The snapshot command must report a verified portable copy, not merely exit successfully."
    copy_tables, copy_database = read_database(destination)
    snapshot = {"database": copy_database, "rows_equal": copy_tables == tables,
                "check": reader.check(destination, run_id, max_output_bytes=1048576)}
    after = file_hash(path)
    return {"tables": tables, "database": database, "check": checks,
            "request_slices": request_slices,
            "reconstructed_requests": reconstructed,
            "metadata_template_reads": omitted_reads, "snapshot": snapshot,
            "source_sha256_before": before, "source_sha256_after": after}


def events(report, kind):
    return [row for row in report["tables"]["events"] if row["kind"] == kind]


def operations(report, method="tools/call"):
    return [row for row in report["tables"]["operations"] if row["method"] == method]


def relations(report, relation):
    return [row for row in report["tables"]["links"] if row["relation"] == relation]


def artifact_body(report, artifact_id):
    row = next(row for row in report["tables"]["artifacts"] if row["artifact_id"] == artifact_id)
    return None if row["body_text"] is None else json.loads(row["body_text"])
