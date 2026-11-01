"""Owned loopback OpenAI JSON endpoint; no SDK/client constructor replacement."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading


def tool(call_id, name, arguments):
    return {"id": call_id, "type": "function",
            "function": {"name": "owned_" + name, "arguments": arguments}}


def script_for(case):
    call = tool("call_alpha", "echo", '{"label":"same","value":7}')
    if case == "parallel":
        return [[call, tool("call_beta", "echo", '{"label":"same","value":7}')]]
    if case == "repeated":
        return [[call], [tool("call_again", "echo", '{"label":"same","value":7}')]]
    if case == "tool_error":
        return [[tool("call_refusal", "refuse", '{"reason":"fixture refusal"}')]]
    if case == "malformed_json":
        return [[tool("call_bad", "echo", "{broken")]]
    return [[call]]


@contextmanager
def endpoint(case, ledger):
    script = script_for(case)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            position = len(ledger)
            if position < len(script):
                message = {"role": "assistant", "content": None, "tool_calls": script[position]}
                finish = "tool_calls"
            else:
                message = {"role": "assistant", "content": "native-" + case + "-complete"}
                finish = "stop"
            response = {"id": "chatcmpl-owned-" + str(position), "object": "chat.completion",
                        "created": 1700000000, "model": "gpt-4o",
                        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                        "usage": {"prompt_tokens": 37, "completion_tokens": 9, "total_tokens": 46}}
            ledger.append({"method": self.command, "path": self.path, "body": body, "response": response})
            payload = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port) + "/v1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive(), "The owned loopback endpoint must stop before fixture completion."
