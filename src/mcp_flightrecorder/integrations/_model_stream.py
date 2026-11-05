"""Bounded observation of chunks actually requested by the host."""
import asyncio
import json

from ._model_capture import MAX_DECISIONS, exposed_index, project_model, usable_string


MAX_BYTES = 1_048_576
MAX_CHOICES = 64


def encode_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


class ToolFragments:
    """One positional tool occurrence; retained fragments are encoded buffers."""
    def __init__(self, order, *, partial=False):
        self.order = order
        self.parts = {"id": [], "name": [], "arguments": []}
        # None is provisional; False has exposed only an unsupported type.
        self.is_function = None
        self.partial = partial
        self.emitted = False

    def value(self, name):
        parts = self.parts[name]
        return None if not parts else "".join(json.loads(part) for part in parts)

    def release(self):
        size = sum(len(part) for values in self.parts.values() for part in values)
        self.parts.clear()
        self.emitted = True
        return size


class StreamCapture:
    """Own retained-byte accounting and one terminal observation per stream."""
    def __init__(self, call, *, representation="openai_chat_stream/1",
                 tool_terminal_reasons=frozenset(("tool_calls",))):
        self.call = call
        self.representation = representation
        self.tool_terminal_reasons = tool_terminal_reasons
        self.chunks = []
        self.tools = {}
        self.choices = {}
        self.chunk_bytes = 0
        self.fragment_bytes = 0
        self.body_stopped = False
        self.stopped = False
        self.selection_gap = False

    def _gap(self, reason):
        # An unavailable consumed projection can hide a prefix for a key that
        # has not been seen yet. This is separate from missing request context.
        self.selection_gap = True
        self.call.loss(reason)
        for tool in self.tools.values():
            if not tool.emitted:
                tool.partial = True

    def _overflow(self, reason):
        self.stopped = True
        self._gap(reason)

    def _retain_chunk(self, encoded):
        if self.chunk_bytes + len(encoded) > MAX_BYTES:
            self.body_stopped = True
            self.call.loss("stream_capture_limit")
            return False
        self.chunk_bytes += len(encoded)
        return True

    def _retain_fragment(self, encoded):
        if self.fragment_bytes + len(encoded) > MAX_BYTES:
            self._overflow("stream_capture_limit")
            return False
        self.fragment_bytes += len(encoded)
        return True

    def _choice(self, index):
        if index not in self.choices:
            if len(self.choices) >= MAX_CHOICES:
                self._overflow("stream_capture_limit")
                return False
            self.choices[index] = None
        return True

    def _tool(self, key):
        if key not in self.tools:
            if len(self.tools) >= MAX_DECISIONS:
                self._overflow("decision_limit")
                return None
            self.tools[key] = ToolFragments(len(self.tools), partial=self.selection_gap)
        return self.tools[key]

    def _fragment(self, tool, name, value):
        if value is None:
            return
        if type(value) is not str:
            tool.partial = True
            self.call.loss("partial_tool_call")
            return
        encoded = encode_json(value)
        if self._retain_fragment(encoded):
            tool.parts[name].append(encoded)

    def _tool_delta(self, choice, delta):
        if type(delta) is not dict or not exposed_index(delta.get("index")):
            self._gap("partial_tool_call")
            return
        tool = self._tool((choice, delta["index"]))
        if tool is None:
            return
        if tool.emitted:
            self.call.loss("partial_tool_call")
            return
        kind, function = delta.get("type"), delta.get("function")
        if kind not in (None, "function"):
            if tool.is_function is None:
                tool.is_function = False
            self._gap("partial_tool_call")
            return
        if kind == "function" or type(function) is dict:
            tool.is_function = True
        if delta.get("id") is not None:
            self._fragment(tool, "id", delta["id"])
        if function is not None and type(function) is not dict:
            tool.partial = True
            self.call.loss("partial_tool_call")
            return
        for name in ("name", "arguments"):
            if self.stopped:
                return
            if type(function) is dict and name in function:
                self._fragment(tool, name, function[name])

    def _emit_tool(self, key, tool, complete):
        if tool.emitted:
            return
        if tool.is_function is False:
            self.fragment_bytes -= tool.release()
            return
        reference, name, arguments = (tool.value(field) for field in ("id", "name", "arguments"))
        bounded_name, bounded_reference = usable_string(name), usable_string(reference)
        complete = (complete and tool.is_function is True and not tool.partial and bounded_name is not None
                    and arguments is not None and (reference is None or bounded_reference is not None))
        payload = {} if arguments is None else {"arguments_raw": arguments}
        self.call.decision(*key, payload, bounded_name, bounded_reference, complete, tool.order)
        self.fragment_bytes -= tool.release()

    def _finish_choice(self, index, finish):
        if finish is None:
            return
        if type(finish) is not str:
            self._gap("partial_tool_call")
            return
        self.choices[index] = finish
        if finish == "function_call":
            self._gap("partial_tool_call")
        if finish in self.tool_terminal_reasons:
            for key, tool in self.tools.items():
                if key[0] == index:
                    self._emit_tool(key, tool, True)

    def _choice_delta(self, choice):
        if type(choice) is not dict or not exposed_index(choice.get("index")):
            self._gap("partial_tool_call")
            return
        index = choice["index"]
        if not self._choice(index):
            return
        delta = choice.get("delta")
        if type(delta) is not dict:
            self._gap("partial_tool_call")
            return
        if delta.get("function_call") is not None:
            self._gap("partial_tool_call")
        tools = delta.get("tool_calls")
        if tools is not None:
            if type(tools) is not list:
                self._gap("partial_tool_call")
            else:
                for tool in tools:
                    self._tool_delta(index, tool)
                    if self.stopped:
                        return
        self._finish_choice(index, choice.get("finish_reason"))

    def consume(self, native):
        if self.call.finished:
            return
        self.call.observe_usage(native, chunk=True)
        if self.stopped:
            return
        try:
            projection = project_model(native, chunk=True)
            if not self.body_stopped:
                encoded = encode_json(projection)
                if self._retain_chunk(encoded):
                    self.chunks.append(encoded)
            choices = projection.get("choices")
            if type(choices) is not list:
                self._gap("partial_tool_call")
                return
            for choice in choices:
                self._choice_delta(choice)
                if self.stopped:
                    return
        except Exception:
            self._gap("projection_failed")

    def finish(self, state):
        if self.call.finished:
            return
        try:
            for key, tool in self.tools.items():
                self._emit_tool(key, tool, False)
            if state == "complete" and (not self.choices or any(value is None for value in self.choices.values())):
                state = "partial"
            payload = {"format": self.representation, "chunks": [json.loads(chunk) for chunk in self.chunks]}
            self.call.terminal(payload, state)
        except Exception:
            self.call.loss("projection_failed")
            self.call.terminal(state=state)
        finally:
            self.chunks.clear()
            self.tools.clear()
            self.choices.clear()
            self.chunk_bytes = 0
            self.fragment_bytes = 0


async def read_chunk(native, capture):
    """Observe one caller-driven read and its native terminal outcome."""
    try:
        chunk = await native.__anext__()
    except StopAsyncIteration:
        capture.finish("complete")
        raise
    except asyncio.CancelledError:
        capture.finish("cancelled")
        raise
    except BaseException:
        capture.finish("error")
        raise
    capture.consume(chunk)
    return chunk


async def cleanup_stream(capture, method, *args, state="partial"):
    """Delegate one host-owned cleanup before submitting its terminal state."""
    try:
        result = await method(*args)
    except asyncio.CancelledError:
        capture.finish("cancelled")
        raise
    except BaseException:
        capture.finish("error")
        raise
    capture.finish(state)
    return result


class ObservedStream:
    """Delegate native reads and cleanup; the recorder never drives the stream."""
    def __init__(self, native, call):
        self._native = native
        self._capture = StreamCapture(call)

    @property
    def response(self):
        return self._native.response

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await read_chunk(self._native, self._capture)

    async def close(self):
        return await cleanup_stream(self._capture, self._native.close)

    async def aclose(self):
        return await cleanup_stream(self._capture, self._native.aclose)

    async def __aenter__(self):
        try:
            await self._native.__aenter__()
        except asyncio.CancelledError:
            self._capture.finish("cancelled")
            raise
        except BaseException:
            self._capture.finish("error")
            raise
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        state = "cancelled" if isinstance(exc, asyncio.CancelledError) else "partial"
        return await cleanup_stream(self._capture, self._native.__aexit__, exc_type, exc, traceback, state=state)
