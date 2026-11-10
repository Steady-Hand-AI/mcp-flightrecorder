"""One POSIX stdio bridge; bounded observation never owns native delivery."""
import argparse
import json
import os
import queue
import selectors
import signal
import subprocess
import sys
import threading
import time

from . import MISSING, Recorder


CHUNK_BYTES = 16_384
BUFFER_BYTES = 65_536
FRAME_BYTES = 65_536
QUEUE_ITEMS = 32
MAX_DEPTH = 32
MAX_NODES = 4096
MAX_ID_BYTES = 256
MAX_IN_FLIGHT = 128
SHUTDOWN_GRACE = 2.0
TERMINATE_GRACE = 0.5
CAPTURE_CLOSE_TIMEOUT = 1.0
_POISONED = object()
_COVERAGE = {"producers": {"model": None, "host": None, "mcp": "mfr.proxy"},
             "model_complete": False}


class _Diagnostics:
    """Fixed coalesced labels only; a blocked stderr cannot stall the bridge."""
    def __init__(self):
        self.pending = queue.Queue(maxsize=32)
        self.seen = set()
        self.lock = threading.Lock()
        self.worker = threading.Thread(target=self._write, daemon=True, name="mfr-proxy-diagnostics")
        self.worker.start()

    def note(self, reason):
        with self.lock:
            if reason in self.seen:
                return
            self.seen.add(reason)
        try:
            self.pending.put_nowait(reason)
        except queue.Full:
            pass

    def _write(self):
        while True:
            reason = self.pending.get()
            if reason is None:
                return
            try:
                # Do not hold Python's buffered stderr lock: an inherited full
                # pipe may block this daemon beyond interpreter shutdown.
                os.write(sys.stderr.fileno(), ("mfr.proxy: " + reason + "\n").encode("ascii"))
            except (OSError, ValueError):
                return

    def close(self):
        try:
            self.pending.put_nowait(None)
        except queue.Full:
            pass
        self.worker.join(timeout=0.2)


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result or len(key.encode("utf-8")) > MAX_ID_BYTES:
            raise ValueError("ambiguous or oversized key")
        result[key] = value
    return result


def _frame_json(frame):
    # Check nesting before the JSON decoder allocates nested containers. Frame
    # bytes already bound every string; the second walk bounds projected nodes.
    depth = 0
    quoted = escaped = False
    for byte in frame:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            if depth > MAX_DEPTH:
                raise ValueError("nesting limit")
        elif byte in (93, 125):
            depth -= 1
    def nonfinite(_):
        raise ValueError("nonfinite JSON")
    value = json.loads(frame, object_pairs_hook=_object, parse_constant=nonfinite)
    pending, count = [value], 0
    while pending:
        node = pending.pop()
        count += 1
        if count > MAX_NODES:
            raise ValueError("projection limit")
        if type(node) is dict:
            pending.extend(node.values())
        elif type(node) is list:
            pending.extend(node)
    if type(value) is not dict or value.get("jsonrpc") != "2.0":
        raise ValueError("unsupported message")
    return value


def _identity(value):
    if type(value) is str and len(value.encode("utf-8")) <= MAX_ID_BYTES:
        return (str, value)
    if type(value) is int and -(2 ** 63) <= value < 2 ** 63:
        return (int, value)
    raise ValueError("unsupported request ID")


class _Observer:
    """Only this worker owns frame copies, the ID map, scope and core tickets.

    A gap with an unknown request ID makes future correlation unsafe. Quarantine
    the connection's observer instead of guessing; forwarding remains intact.
    Accepted tickets then reach existing core incomplete-close semantics.
    """
    def __init__(self, path, capture, diagnostics):
        self.path, self.capture, self.diagnostics = path, capture, diagnostics
        self.pending = queue.Queue(maxsize=QUEUE_ITEMS)
        self.gap = threading.Event()
        self.stopping = threading.Event()
        self.frames = {"client": bytearray(), "server": bytearray()}
        self.calls = {}
        self.disabled = False
        self.recorder = None
        self.scope = None
        self.worker = threading.Thread(target=self._run, daemon=True, name="mfr-proxy-observer")
        self.worker.start()

    def offer(self, direction, data):
        if self.gap.is_set():
            return
        try:
            self.pending.put_nowait((direction, data))
        except queue.Full:
            self.gap.set()

    def _disable(self, reason):
        self.diagnostics.note(reason)
        self.diagnostics.note("correlation_disabled")
        self.disabled = True
        self.calls.clear()
        for frame in self.frames.values():
            frame.clear()

    def _feed(self, direction, data):
        frame = self.frames[direction]
        while data and not self.disabled:
            end = data.find(b"\n")
            part = data if end < 0 else data[:end]
            if len(frame) + len(part) > FRAME_BYTES:
                self._disable("frame_limit")
                return
            frame.extend(part)
            if end < 0:
                return
            try:
                message = _frame_json(frame)
                frame.clear()
                self._message(direction, message)
            except (ValueError, UnicodeError, RecursionError):
                self._disable("unsupported_observation")
            data = data[end + 1:]

    def _message(self, direction, message):
        # Legacy reverse requests and their client responses are never tickets
        # for client-to-server operations, including colliding numeric IDs.
        if direction == "client" and "method" in message:
            if "id" not in message:
                return
            key = _identity(message["id"])
            if key in self.calls:
                self.calls[key] = _POISONED
                self.diagnostics.note("duplicate_live_id")
                return
            if len(self.calls) >= MAX_IN_FLIGHT:
                self._disable("in_flight_limit")
                return
            # Even opaque/rejected requests reserve their ID until a response;
            # otherwise a supported request could borrow their response.
            self.calls[key] = None
            method = message["method"]
            if method not in ("tools/list", "tools/call"):
                return
            params = message.get("params", {})
            if type(params) is not dict:
                self.diagnostics.note("unsupported_projection")
                return
            name = params.get("name") if method == "tools/call" else None
            arguments = params.get("arguments", {}) if method == "tools/call" else params
            if method == "tools/call" and (type(name) is not str or not name or
                    len(name.encode("utf-8")) > MAX_ID_BYTES):
                self.diagnostics.note("unsupported_projection")
                return
            receipt = self.recorder.start_operation(scope=self.scope, client_id="stdio-client",
                method=method, name=name, arguments=arguments, link_basis="none")
            if receipt.status == "accepted" and receipt.ticket is not None:
                self.calls[key] = (receipt.ticket, method)
            else:
                self.diagnostics.note("start_rejected")
        elif direction == "server" and "method" not in message and "id" in message:
            key = _identity(message["id"])
            item = self.calls.get(key)
            if item is _POISONED:
                return  # Permanently ambiguous, not reusable after one reply.
            if key not in self.calls:
                self.diagnostics.note("uncorrelated_response")
                return
            if ("result" in message) == ("error" in message):
                self.calls[key] = _POISONED
                self.diagnostics.note("unsupported_projection")
                return
            del self.calls[key]
            if item is None:
                return
            ticket, method = item
            complete, paths, error = True, None, None
            if "error" in message:
                body = {"error": message["error"]}
                status = "protocol_error"
                exposed = message["error"]
                code = exposed.get("code") if type(exposed) is dict else None
                if type(code) is not int or not -(2 ** 63) <= code < 2 ** 63:
                    code, complete = None, False
                error = {"origin": "server", "type": "JSONRPCError", "code": code}
            else:
                body = message["result"]
                status = "ok"
                if type(body) is not dict:
                    body, complete = MISSING, False
                elif body.get("resultType") == "input_required" or "inputRequests" in body:
                    # Intermediate modern MRTR follows the existing adapter's
                    # partial terminal meaning; the client owns its next RPC.
                    complete = False
                elif method == "tools/call":
                    status = "tool_error" if body.get("isError") is True else "ok"
                    if "isError" in body and type(body["isError"]) is not bool:
                        complete = False
                    content = body.get("content")
                    if type(content) is list:
                        paths = ["/content/" + str(i) + "/text" for i, block in enumerate(content)
                                 if type(block) is dict and block.get("type") == "text"
                                 and type(block.get("text")) is str]
                    else:
                        complete = False
                elif type(body.get("tools")) is not list:
                    complete = False
            receipt = self.recorder.finish_operation(ticket, status=status, result=body,
                resolution="unknown", input_rounds=None, error=error,
                text_paths=paths, complete=complete)
            if receipt.status != "accepted":
                self.diagnostics.note("terminal_rejected")

    def _run(self):
        try:
            try:
                self.recorder = Recorder(self.path, capture=self.capture, coverage=_COVERAGE)
                self.scope = self.recorder.capture_scope()
            except Exception:
                self.diagnostics.note("recording_unavailable")
                return
            while not self.stopping.is_set() or not self.pending.empty():
                if self.gap.is_set() and not self.disabled:
                    self._disable("observer_queue_limit")
                try:
                    direction, data = self.pending.get(timeout=0.05)
                except queue.Empty:
                    continue
                if not self.disabled:
                    try:
                        self._feed(direction, data)
                    except Exception:
                        self._disable("capture_failed")
            if any(self.frames.values()):
                self.diagnostics.note("incomplete_frame")
            health = self.recorder.health()
            if health.dropped_events or health.writer_errors:
                self.diagnostics.note("recorder_loss")
        except Exception:
            self.diagnostics.note("capture_failed")
        finally:
            if self.recorder is not None:
                try:
                    closed = self.recorder.close(timeout=CAPTURE_CLOSE_TIMEOUT)
                    if not closed.writer_stopped or not closed.portable:
                        self.diagnostics.note("recording_not_portable")
                    if closed.run_close_state != "clean":
                        self.diagnostics.note("recording_incomplete")
                except Exception:
                    self.diagnostics.note("capture_close_failed")

    def close(self):
        self.stopping.set()
        self.worker.join(timeout=CAPTURE_CLOSE_TIMEOUT + 0.5)
        if self.worker.is_alive():
            self.diagnostics.note("capture_cleanup_timeout")


def _bridge(child, observer, diagnostics, interrupted):
    """One selector owner for all transport buffers and closure decisions."""
    stdin, stdout = sys.stdin.fileno(), sys.stdout.fileno()
    child_in, child_out = child.stdin.fileno(), child.stdout.fileno()
    original = {fd: os.get_blocking(fd) for fd in (stdin, stdout)}
    to_child, to_client = bytearray(), bytearray()
    input_open = output_open = True
    sink_open = True
    deadline = None
    failed = False
    try:
        for fd in (stdin, stdout, child_in, child_out):
            os.set_blocking(fd, False)
        while True:
            now = time.monotonic()
            if interrupted[0] or child.poll() is not None:
                input_open = False
                if deadline is None:
                    deadline = now + SHUTDOWN_GRACE
            if not input_open and not to_child and not child.stdin.closed:
                child.stdin.close()
                if deadline is None:
                    deadline = now + SHUTDOWN_GRACE
            if child.poll() is not None and not output_open and not to_client:
                break
            if deadline is not None and now >= deadline:
                if child.poll() is None:
                    diagnostics.note("child_shutdown_timeout")
                if to_child or to_client:
                    diagnostics.note("transport_not_drained")
                break
            # SelectSelector supports ordinary redirected files as well as pipes
            # on POSIX. Reads never exceed the receiving buffer's free space.
            with selectors.SelectSelector() as selector:
                if input_open and not child.stdin.closed and len(to_child) < BUFFER_BYTES:
                    selector.register(stdin, selectors.EVENT_READ, "input")
                if output_open and len(to_client) < BUFFER_BYTES:
                    selector.register(child_out, selectors.EVENT_READ, "output")
                if to_child and not child.stdin.closed:
                    selector.register(child_in, selectors.EVENT_WRITE, "child")
                if to_client and sink_open:
                    selector.register(stdout, selectors.EVENT_WRITE, "client")
                for key, _ in selector.select(timeout=0.05):
                    try:
                        if key.data in ("input", "output"):
                            target = to_child if key.data == "input" else to_client
                            data = os.read(key.fd, min(CHUNK_BYTES, BUFFER_BYTES - len(target)))
                            if data:
                                target.extend(data)
                                if observer is not None:
                                    observer.offer("client" if key.data == "input" else "server", data)
                            elif key.data == "input":
                                input_open = False
                                if deadline is None:
                                    deadline = time.monotonic() + SHUTDOWN_GRACE
                            else:
                                output_open = input_open = False
                                if deadline is None:
                                    deadline = time.monotonic() + SHUTDOWN_GRACE
                        else:
                            target = to_child if key.data == "child" else to_client
                            written = os.write(key.fd, target)
                            del target[:written]
                    except BlockingIOError:
                        continue
                    except OSError:
                        failed = True
                        diagnostics.note("transport_failed")
                        input_open = False
                        if key.data == "child":
                            child.stdin.close()
                            to_child.clear()
                        elif key.data == "client":
                            sink_open = output_open = False
                            to_client.clear()
                        elif key.data == "output":
                            output_open = False
                        if deadline is None:
                            deadline = time.monotonic() + SHUTDOWN_GRACE
        return failed
    finally:
        for fd, blocking in original.items():
            try:
                os.set_blocking(fd, blocking)
            except OSError:
                diagnostics.note("transport_failed")


def run(command, *, record=None, capture="metadata"):
    """Forward one child until closure; never restart it or retry a request."""
    diagnostics = _Diagnostics()
    child = observer = None
    interrupted = [0]
    handlers = {}
    failed = False
    try:
        child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=None, bufsize=0, shell=False)
        if record is not None:
            observer = _Observer(record, capture, diagnostics)
        def interrupt(number, _frame):
            interrupted[0] = number
        for number in (signal.SIGINT, signal.SIGTERM):
            handlers[number] = signal.signal(number, interrupt)
        failed = _bridge(child, observer, diagnostics, interrupted)
    except (OSError, ValueError):
        diagnostics.note("transport_unavailable")
        failed = True
    finally:
        if child is not None:
            child.stdin.close()
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=TERMINATE_GRACE)
                except subprocess.TimeoutExpired:
                    child.kill()
            try:
                child.wait(timeout=TERMINATE_GRACE)
            except subprocess.TimeoutExpired:
                diagnostics.note("child_reap_timeout")
                failed = True
            child.stdout.close()
        if observer is not None:
            observer.close()
        for number, handler in handlers.items():
            signal.signal(number, handler)
        diagnostics.close()
    if interrupted[0]:
        return 128 + interrupted[0]
    if child is None:
        return 127
    if child.returncode:
        return child.returncode if child.returncode > 0 else 128 - child.returncode
    return 1 if failed else 0


class _Parser(argparse.ArgumentParser):
    def _print_message(self, message, file=None):
        super()._print_message(message, sys.stderr)

    def error(self, message):
        self.exit(2, "fr proxy: invalid arguments; use fr proxy --help.\n")


def main(arguments):
    parser = _Parser(prog="fr proxy", description="Forward one POSIX stdio child; optionally record selected MCP evidence.")
    parser.add_argument("--record", metavar="FILE", help="Create a fresh recording; omit for forwarding only.")
    parser.add_argument("--capture", choices=("metadata", "content"), help="Recording retention; default metadata.")
    parser.add_argument("child", nargs=argparse.REMAINDER, help="-- SERVER_COMMAND ARGUMENT...")
    args = parser.parse_args(arguments)
    if len(args.child) < 2 or args.child[0] != "--" or not args.child[1]:
        parser.error("child command required")
    if args.capture is not None and args.record is None or args.record == "":
        parser.error("capture requires a recording")
    if os.name != "posix":
        parser.exit(2, "fr proxy: this stdio bridge requires POSIX pipes.\n")
    return run(args.child[1:], record=args.record, capture=args.capture or "metadata")
