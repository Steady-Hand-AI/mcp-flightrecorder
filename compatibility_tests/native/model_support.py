"""Owned native transport ledgers and explicitly labelled model-boundary doubles."""
import asyncio
from collections.abc import AsyncIterator
from contextlib import closing
from functools import wraps
import json
import sqlite3

from any_llm import AnyLLM
from any_llm.types.completion import ChatCompletionChunk
import httpx
import pytest_check as check

PROMPT = "Keep café ✓ at the supplied boundary."
ANSWER = "ready Ω"
MODEL = "fixture-ollama"
TOOLS = [{"type": "function", "function": {"name": "echo", "description": "Echo x.",
          "parameters": {"type": "object", "properties": {"x": {"type": "integer"}},
                         "required": ["x"]}}}]
COVERAGE = {"producers": {"model": "compatibility.anyllm-submitted-normalized",
                          "host": "compatibility.explicit-fixture", "mcp": None}}


def projection(value):
    """Independent native model projection, not a recorder helper."""
    return value.model_dump(mode="json", by_alias=True, exclude_unset=True)


def persisted(recorder, path):
    closed = recorder.close(timeout=5)
    assert closed.writer_stopped and closed.portable, (
        "Recorded assertions require the test-owned writer to stop and leave a portable SQLite main file."
    )
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        events = [dict(row) for row in db.execute("SELECT * FROM events ORDER BY sequence")]
        artifacts = {row["artifact_id"]: dict(row) for row in db.execute("SELECT * FROM artifacts")}
        links = [dict(row) for row in db.execute("SELECT * FROM links")]
        runs = [dict(row) for row in db.execute("SELECT * FROM runs")]
        check.equal([tuple(row) for row in db.execute("PRAGMA integrity_check")], [("ok",)],
                    msg="Each model fixture retains a structurally readable portable recording.")
        check.equal(list(db.execute("PRAGMA foreign_key_check")), [],
                    msg="Model event/artifact references remain valid after native cleanup.")
    for event in events:
        event["details"] = json.loads(event["details_json"])
        artifact = artifacts.get(event["payload_ref"])
        event["body"] = (None if artifact is None or artifact["body_text"] is None
                         else json.loads(artifact["body_text"]))
    return events, artifacts, links, runs


def request_body(event, artifacts):
    """Reassemble documented request_refs/2 using its actual public pointers."""
    manifest = event["body"]
    assert manifest is not None and manifest["format"] == "request_refs/2", (
        "A complete submitted request must expose the documented request manifest before reassembly."
    )
    body = json.loads(artifacts[manifest["template_ref"]]["body_text"])
    check.equal(manifest["message_path"], "/messages",
                msg="The shared manifest must locate exactly the supplied message array.")
    body["messages"] = [json.loads(artifacts[key]["body_text"]) for key in manifest["message_refs"]]
    for field in manifest["field_refs"]:
        check.equal(field["pointer"], "/tools", msg="Only the supplied tool catalog is extracted as a body field.")
        body["tools"] = json.loads(artifacts[field["artifact_id"]]["body_text"])
    return body


def ollama_frame(*, content=ANSWER, tools=None, done=True, finish="stop", thinking="reasoning λ"):
    message = {"role": "assistant", "content": content}
    if thinking is not None:
        message["thinking"] = thinking
    if tools is not None:
        message["tool_calls"] = [{"function": {"name": name, "arguments": arguments}}
                                 for name, arguments in tools]
    return {"model": MODEL, "created_at": "2026-10-21T00:00:00.000000Z", "message": message,
            "done": done, "done_reason": finish, "prompt_eval_count": 5, "eval_count": 3,
            "total_duration": 19, "load_duration": 2, "prompt_eval_duration": 7, "eval_duration": 10}


class WireBytes(httpx.AsyncByteStream):
    """Owned NDJSON with exact read, block, error and cleanup witnesses."""
    def __init__(self, frames, *, block_after=None, error_after=None):
        self.frames = frames
        self.block_after, self.error_after = block_after, error_after
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.reads = []
        self.closes = 0
        self.error = httpx.ReadError("owned NDJSON failure")

    async def __aiter__(self):
        for index in range(len(self.frames) + 1):
            if index == self.block_after:
                self.entered.set()
                await self.release.wait()
            if index == self.error_after:
                raise self.error
            if index == len(self.frames):
                return
            self.reads.append(index)
            yield (json.dumps(self.frames[index], ensure_ascii=False) + "\n").encode("utf-8")

    async def aclose(self):
        self.closes += 1
        self.release.set()


def native_provider(requests, *, body=None, wire=None, status=200):
    def transport(request):
        assert request.url.host == "fixture.invalid", "The owned transport must never contact an external provider."
        requests.append(json.loads(request.content))
        if wire is not None:
            return httpx.Response(status, stream=wire, headers={"content-type": "application/x-ndjson"})
        return httpx.Response(status, json=body if body is not None else ollama_frame())

    return AnyLLM.create("ollama", api_base="http://fixture.invalid", unified_exceptions=True,
                         transport=httpx.MockTransport(transport))


class IteratorLedger:
    """A narrow pass-through witness; every yielded object/error is native."""
    def __init__(self, native):
        self.native = native
        self.chunks, self.errors = [], []
        self.eof = False
        self.closes = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            chunk = await self.native.__anext__()
        except StopAsyncIteration:
            self.eof = True
            raise
        except BaseException as error:
            self.errors.append(error)
            raise
        self.chunks.append(chunk)
        return chunk

    async def aclose(self):
        self.closes += 1
        return await self.native.aclose()


def native_spy(native):
    seen = {"calls": [], "results": [], "errors": [], "iterators": []}

    @wraps(native)
    async def call(*args, **kwargs):
        seen["calls"].append((args, kwargs))
        try:
            result = await native(*args, **kwargs)
        except BaseException as error:
            seen["errors"].append(error)
            raise
        seen["results"].append(result)
        if isinstance(result, AsyncIterator):
            ledger = IteratorLedger(result)
            seen["iterators"].append(ledger)
            return ledger
        return result

    return call, seen


def stream_chunk(*, choices=None, content=None, tools=None, finish=None, choice=0):
    """Labelled SDK-model boundary double; native conversions use ollama_frame."""
    if choices is None:
        delta = {}
        if content is not None:
            delta["content"] = content
        if tools is not None:
            delta["tool_calls"] = tools
        choices = [{"index": choice, "delta": delta, "finish_reason": finish}]
    return ChatCompletionChunk.model_validate({
        "id": "boundary-double", "object": "chat.completion.chunk", "created": 1,
        "model": MODEL, "choices": choices,
    })


def tool_delta(index, *, reference="reused-double-reference", name="echo", arguments="{}"):
    return {"index": index, "id": reference, "type": "function",
            "function": {"name": name, "arguments": arguments}}


class BoundaryIterator:
    """Labelled limit/malformed double, without a provider or wire claim."""
    def __init__(self, chunks):
        self.chunks = chunks
        self.position, self.closes = 0, 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.position == len(self.chunks):
            raise StopAsyncIteration
        value = self.chunks[self.position]
        self.position += 1
        return value

    async def aclose(self):
        self.closes += 1


def boundary_method(result):
    """Deliberately small fake callable for non-native boundary controls."""
    async def native(model, messages, *, stream=None, **kwargs):
        return result

    return native
