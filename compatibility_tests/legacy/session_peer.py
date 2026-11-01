"""Owned deterministic JSON-RPC peer for the actual installed SDK1 client.

Normal responses are complete protocol bodies. --malformed is a separately
labelled malformed wire control, not a returned native model double.
"""
import argparse
import json
from pathlib import Path
import sys


SCHEMA = {"type": "object", "properties": {
    "text": {"type": "string"}, "case": {"type": "string"}}, "additionalProperties": True}


def send(value):
    sys.stdout.write(json.dumps(value, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def append(path, value):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")


def run(options):
    for line in sys.stdin:
        request = json.loads(line)
        if "id" not in request:
            continue
        method, params = request["method"], request.get("params")
        if method == "initialize":
            result = {"protocolVersion": params["protocolVersion"], "capabilities": {"tools": {}},
                      "serverInfo": {"name": "Owned SDK1 protocol peer", "version": "1"}}
        elif method == "tools/list":
            second = params is not None and params.get("cursor") == "page-two"
            names = ["last"] if second else ["echo", "refuse", "wait"]
            result = {"tools": [{"name": name, "description": "Ordered " + name,
                                 "inputSchema": SCHEMA} for name in names],
                      "nextCursor": "page-two" if options.paged and not second else None,
                      "_meta": {"page": 2 if second else 1, "hint": "catalog café"},
                      "ttlMs": 1000, "cacheScope": "private"}
            append(options.ledger, {"method": method, "params": params, "result": result})
        elif method == "tools/call":
            arguments = params.get("arguments") or {}
            case = arguments.get("case", "return")
            append(options.ledger, {"method": method, "params": params})
            if case in ("timeout", "cancelled"):
                continue
            if case == "transport-close":
                return
            if case == "protocol-error":
                error = {"code": -32602, "message": "native protocol explanation café"}
                append(options.ledger, {"method": "error", "error": error})
                send({"jsonrpc": "2.0", "id": request["id"], "error": error})
                continue
            token = (params.get("_meta") or {}).get("progressToken")
            if token is not None:
                send({"jsonrpc": "2.0", "method": "notifications/progress",
                      "params": {"progressToken": token, "progress": 1, "total": 2,
                                 "message": "entered ✓"}})
            content = [] if case == "empty" else [
                {"type": "text", "text": arguments.get("text", "default ✓")},
                {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"},
                {"type": "text", "text": "second Ω"}]
            result = ({"isError": False} if options.malformed else
                      {"content": content, "isError": case == "tool-error",
                       "structuredContent": {"case": case},
                       "_meta": {"retained": "result Ω"}})
            append(options.ledger, {"method": "result", "result": result})
        else:
            result = {}
        send({"jsonrpc": "2.0", "id": request["id"], "result": result})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("ledger", type=Path)
    parser.add_argument("--paged", action="store_true")
    parser.add_argument("--malformed", action="store_true")
    run(parser.parse_args())
