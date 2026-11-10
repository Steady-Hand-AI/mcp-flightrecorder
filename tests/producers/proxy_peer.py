"""Deterministic raw peer and test-only installed CLI fault bootstrap."""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import time


def record(path, value):
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")


def bootstrap(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument("--fault", required=True, choices=(
        "start-rejected", "first-rejected", "capacity", "writer-failed",
        "init-failed", "held-start",
    ))
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--release", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(arguments)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("missing installed CLI arguments")

    import mcp_flightrecorder
    from mcp_flightrecorder import Recorder, _storage

    record(args.evidence, {"kind": "bootstrap", "fault": args.fault,
                           "installed_origin": mcp_flightrecorder.__file__})
    original_init = Recorder.__init__
    original_start = Recorder.start_operation
    original_finish = Recorder.finish_operation
    original_close = Recorder.close
    calls = 0

    def initialize(self, *positional, **keywords):
        if args.fault == "capacity":
            keywords["limits"] = {**(keywords.get("limits") or {}), "completion_bytes": 1}
        return original_init(self, *positional, **keywords)

    def start(self, **keywords):
        nonlocal calls
        calls += 1
        number = calls
        if args.fault == "held-start" and number == 1:
            record(args.evidence, {"kind": "start-held"})
            deadline = time.monotonic() + 15
            while args.release is not None and not args.release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("test admission handshake expired")
                time.sleep(0.01)
        if args.fault == "start-rejected" or (args.fault == "first-rejected" and number == 1):
            keywords["method"] = ""
        result = original_start(self, **keywords)
        record(args.evidence, {"kind": "start", "number": number,
                               "status": result.status, "reason": result.reason,
                               "ticket": result.ticket is not None})
        return result

    def finish(self, *positional, **keywords):
        result = original_finish(self, *positional, **keywords)
        record(args.evidence, {"kind": "finish", "status": result.status,
                               "reason": result.reason})
        return result

    def close(self, *positional, **keywords):
        result = original_close(self, *positional, **keywords)
        health = self.health()
        record(args.evidence, {"kind": "close", "result": dataclasses.asdict(result),
                               "health": {"state": health.state, "dropped_events": health.dropped_events,
                                          "writer_errors": health.writer_errors,
                                          "reason_counts": dict(health.reason_counts),
                                          "open_operations": health.open_operations,
                                          "ordinary": dataclasses.asdict(health.ordinary),
                                          "completion": dataclasses.asdict(health.completion)}})
        return result

    Recorder.__init__ = initialize
    Recorder.start_operation = start
    Recorder.finish_operation = finish
    Recorder.close = close
    if args.fault == "writer-failed":
        def fail_write(self, *positional, **keywords):
            record(args.evidence, {"kind": "injected-storage-error",
                                   "hook": "Recorder._persist_bundle"})
            raise sqlite3.OperationalError("controlled test writer failure")
        Recorder._persist_bundle = fail_write
    elif args.fault == "init-failed":
        def fail_connect(*positional, **keywords):
            record(args.evidence, {"kind": "injected-initialization-error",
                                   "hook": "_storage.connect"})
            raise sqlite3.OperationalError("controlled test initialization failure")
        _storage.connect = fail_connect
    sys.argv = ["fr", *command]
    from mcp_flightrecorder.cli import main
    return main()


def peer(arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument("--response", type=Path, required=True)
    parser.add_argument("--observed", type=Path, required=True)
    parser.add_argument("--pid", type=Path, required=True)
    parser.add_argument("--ready", type=Path)
    parser.add_argument("--input-bytes", type=int)
    parser.add_argument("--tail-input-bytes", type=int)
    parser.add_argument("--tail-response", type=Path)
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--label")
    parser.add_argument("--read-size", type=int, default=16384)
    parser.add_argument("--write-size", type=int, default=16384)
    parser.add_argument("--stderr-bytes", type=int, default=0)
    parser.add_argument("--exit-code", type=int, default=0)
    parser.add_argument("--signal-exit", type=int)
    parser.add_argument("--exit-before-input", action="store_true")
    parser.add_argument("--output-before-input", action="store_true")
    parser.add_argument("--close-output", action="store_true")
    parser.add_argument("--hang", action="store_true")
    parser.add_argument("--ignore-term", action="store_true")
    args = parser.parse_args(arguments)
    if args.ignore_term:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    args.pid.write_text(str(os.getpid()))
    if args.identity:
        args.identity.write_text(json.dumps({"cwd": str(Path.cwd()), "label": args.label,
                                             "environment": os.environ.get("PROXY_PEER_LABEL")}))

    def emit(path):
        payload = path.read_bytes()
        for offset in range(0, len(payload), args.write_size):
            remaining = memoryview(payload)[offset:offset + args.write_size]
            while remaining:
                written = os.write(1, remaining)
                remaining = remaining[written:]

    if args.output_before_input or args.exit_before_input:
        emit(args.response)
    if not args.exit_before_input:
        with args.observed.open("wb") as stream:
            remaining = args.input_bytes
            while remaining is None or remaining > 0:
                block = os.read(0, min(args.read_size, remaining)
                                if remaining is not None else args.read_size)
                if not block:
                    break
                stream.write(block)
                stream.flush()
                if remaining is not None:
                    remaining -= len(block)
        if not args.output_before_input:
            emit(args.response)
        if args.tail_input_bytes is not None:
            if args.ready:
                args.ready.write_text("ready")
            remaining = args.tail_input_bytes
            with args.observed.open("ab") as stream:
                while remaining:
                    block = os.read(0, min(args.read_size, remaining))
                    if not block:
                        break
                    stream.write(block)
                    stream.flush()
                    remaining -= len(block)
            if args.tail_response:
                emit(args.tail_response)
    if args.stderr_bytes:
        message = (b"NATIVE-STDERR\n" * ((args.stderr_bytes // 14) + 2))[:args.stderr_bytes]
        while message:
            written = os.write(2, message)
            message = message[written:]
    if args.close_output:
        os.close(1)
    if args.ready and not args.ready.exists():
        args.ready.write_text("ready")
    if args.hang:
        while True:
            signal.pause()
    if args.signal_exit:
        os.kill(os.getpid(), args.signal_exit)
    return args.exit_code


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in {"peer", "bootstrap"}:
        raise SystemExit("expected peer or bootstrap")
    raise SystemExit((bootstrap if sys.argv[1] == "bootstrap" else peer)(sys.argv[2:]))
