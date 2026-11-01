"""Owned loopback Ollama endpoint; all host and SDK conversion code stays native."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread


def copied(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def response_chunks(request, scenario):
    results = [item for item in request["messages"] if item["role"] == "tool"]
    common = {"model": "review-model", "created_at": "2026-10-22T00:00:00.000000Z"}
    if results:
        answer = {"contents": [item.get("content", "") for item in results],
                  "names": [item.get("tool_name") for item in results]}
        text = json.dumps(answer, ensure_ascii=False, sort_keys=True)
        half = len(text) // 2
        yield {**common, "message": {"role": "assistant", "content": text[:half]}, "done": False}
        yield {**common, "message": {"role": "assistant", "content": text[half:]}, "done": False}
    else:
        selected = request["tools"] if scenario == "collision" else request["tools"][:1]
        if scenario == "empty_alias":
            selected = selected * 2
        for tool in selected:
            call = {"function": {"name": tool["function"]["name"],
                                 "arguments": {"x": -1 if scenario == "tool_error" else 7}}}
            yield {**common, "message": {"role": "assistant", "content": "", "tool_calls": [call]}, "done": False}
    yield {**common, "message": {"role": "assistant", "content": ""}, "done": True,
           "done_reason": "stop", "prompt_eval_count": 5, "eval_count": 3}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return None

    def send_body(self, body, media_type):
        self.send_response(200)
        self.send_header("Content-Type", media_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/api/show":
            response = {"capabilities": ["tools"]}
            self.server.requests.append({"path": self.path, "request": request, "response": response})
            self.send_body(json.dumps(response).encode(), "application/json")
            return
        if self.path != "/api/chat":
            self.send_error(404)
            return
        chunks = list(response_chunks(request, self.server.scenario))
        self.server.requests.append({"path": self.path, "request": copied(request), "response": copied(chunks)})
        body = b"".join(json.dumps(chunk, ensure_ascii=False).encode() + b"\n" for chunk in chunks)
        self.send_body(body, "application/x-ndjson")


class OllamaEndpoint:
    def __init__(self, scenario):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.server.requests = []
        self.server.scenario = scenario
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.url = "http://127.0.0.1:" + str(self.server.server_port)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        assert not self.thread.is_alive(), "The test-owned loopback listener must stop before its case relinquishes the port."

    @property
    def requests(self):
        return self.server.requests
