"""Submitted arguments and native Chat Completions observations."""
from dataclasses import dataclass

from openai import NotGiven, Omit
from openai.types.chat import ChatCompletion, ChatCompletionChunk

from .. import MISSING
from ._common import EvidenceBase, capture_call, copy_json, policy_retention


MAX_DECISIONS = 64
TRANSPORT_OPTIONS = frozenset(("extra_headers", "extra_query", "timeout"))


class ModelEvidence(EvidenceBase):
    """Call-local admitted receipts shared by the named model observers."""
    __slots__ = ("request", "response", "decisions")

    def __init__(self):
        super().__init__()
        self.request = None
        self.response = None
        self.decisions = ()


@dataclass(frozen=True)
class ModelDecision:
    """An exposed tool position and its explicit recorder occurrence receipt."""
    choice_index: int
    tool_index: int
    provider_ref: str | None
    receipt: object


def usable_string(value):
    """Only bounded UTF-8 strings can enter core control metadata."""
    if type(value) is not str:
        return None
    try:
        return value if len(value.encode("utf-8")) <= 256 else None
    except UnicodeError:
        return None


def exposed_index(value):
    return type(value) is int and value >= 0


def omitted(value):
    return type(value) in (Omit, NotGiven)


def submitted_body(kwargs):
    """Copy submitted ordinary body kwargs without a second SDK serializer."""
    extra = kwargs.get("extra_body")
    if extra is not None and not omitted(extra) and not (type(extra) is dict and not extra):
        raise ValueError("extra_body can override the observed body")
    body = {key: value for key, value in kwargs.items()
            if key not in TRANSPORT_OPTIONS and key != "extra_body" and not omitted(value)}
    if "functions" in body or "function_call" in body:
        raise ValueError("legacy function calls are outside this observation")
    body = copy_json(body)
    if type(body.get("messages")) is not list:
        raise ValueError("messages must expose an ordered ordinary JSON array")
    return body


def default_annotations(messages):
    """A provider ID never resolves a submitted tool result to an occurrence."""
    result = []
    for message in messages:
        item = {"bindings": [], "unresolved": [], "complete": True}
        if type(message) is dict and message.get("role") == "tool":
            unresolved = {"pointer": "/content", "reason": "binding_not_supplied"}
            reference = usable_string(message.get("tool_call_id"))
            if reference is not None:
                unresolved["provider_ref"] = reference
            item.update(bindings=None, unresolved=[unresolved])
        result.append(item)
    return result


def request_parts(kwargs, annotations, insertions, body_builder):
    body = body_builder(kwargs)
    annotations = (default_annotations(body["messages"]) if annotations is None
                   else copy_json(annotations))
    if type(insertions) not in (tuple, list):
        raise ValueError("insertions must be an ordered ordinary sequence")
    facts = {"message_path": "/messages", "messages": annotations,
             "insertions": copy_json(list(insertions))}
    if "tools" in body:
        facts.update(field_paths=["/tools"], catalog_path="/tools")
    return body, facts


def project_model(value, *, chunk=False):
    """Invoke only the selected trusted SDK model's documented projection."""
    expected = ChatCompletionChunk if chunk else ChatCompletion
    if not isinstance(value, expected):
        raise ValueError("the SDK result is not the selected native model")
    result = copy_json(value.to_dict(mode="json", use_api_names=True, exclude_unset=True))
    if type(result) is not dict:
        raise ValueError("the native projection must expose an object")
    return result


def function_fields(tool):
    """Expose function fields without parsing or normalizing raw arguments."""
    function = tool.get("function")
    if type(function) is not dict:
        function = {}
    payload = {}
    if type(function.get("arguments")) is str:
        payload["arguments_raw"] = function["arguments"]
    name = usable_string(function.get("name"))
    reference = usable_string(tool.get("id"))
    complete = name is not None and type(function.get("arguments")) is str
    if tool.get("id") is not None and reference is None:
        complete = False
    return payload, name, reference, complete


class ModelCall:
    """One invocation's fixed scope, admitted receipts, and terminal transition."""
    def __init__(self, recorder, scope, instance_id, evidence, *, retention_policy=None):
        self.recorder, self.scope = recorder, scope
        self.instance_id, self.evidence = instance_id, evidence
        self.retention_policy = retention_policy
        self.partial = bool(evidence.losses)
        self.finished = False
        self._entries = []

    def loss(self, reason):
        self.partial = True
        self.evidence.add_loss(reason)

    def observe(self, kind, payload, *, choice_index=None, tool_index=None, **facts):
        if self.retention_policy is not None:
            context = {"instance_id": self.instance_id, "choice_index": choice_index,
                       "tool_index": tool_index, "name": facts.get("name"),
                       "provider_ref": facts.get("provider_ref")}
            request_paths = ((facts["message_path"], facts.get("field_paths", []))
                             if kind == "model.request" and payload is not MISSING else None)
            facts["retention"] = policy_retention(
                self.evidence, self.retention_policy, kind, payload, context,
                request_paths=request_paths)
        facts["instance_id"] = self.instance_id
        receipt = capture_call(self.evidence, self.recorder.observe, kind,
                               scope=self.scope, payload=payload, facts=facts)
        if receipt is None or receipt.status != "accepted":
            self.partial = True
        return receipt

    @property
    def model_call_id(self):
        receipt = self.evidence.request
        return None if receipt is None else receipt.model_call_id

    def request(self, kwargs, annotations, insertions, *, body_builder=submitted_body):
        try:
            payload, facts = request_parts(kwargs, annotations, insertions, body_builder)
        except Exception:
            self.loss("request_unavailable")
            payload, facts = MISSING, {"message_path": None, "complete": False}
        self.evidence.request = self.observe("model.request", payload, **facts)

    def decision(self, choice, tool, payload, name, reference, complete, order):
        if not complete:
            self.loss("partial_tool_call")
        receipt = self.observe("model.tool_call", payload, choice_index=choice, tool_index=tool,
                               name=name,
                               provider_ref=reference, model_call_id=self.model_call_id,
                               complete=complete)
        self._entries.append((order, ModelDecision(choice, tool, reference, receipt)))
        self._entries.sort(key=lambda item: item[0])
        self.evidence.decisions = tuple(entry for _, entry in self._entries)

    def terminal(self, payload=MISSING, state="complete"):
        if self.finished:
            return
        self.finished = True
        if state == "complete" and self.partial:
            state = "partial"
        self.evidence.response = self.observe("model.response", payload,
                                             model_call_id=self.model_call_id,
                                             state=state, complete=state == "complete")

    def _normal_tool(self, tool, choice, position, finish, seen):
        if type(tool) is not dict or tool.get("type") != "function":
            self.loss("partial_tool_call")
            return
        key = choice, position
        if key in seen:
            self.loss("partial_tool_call")
            return
        if len(seen) >= MAX_DECISIONS:
            self.loss("decision_limit")
            return
        seen.add(key)
        payload, name, reference, fields_complete = function_fields(tool)
        self.decision(choice, position, payload, name, reference,
                      fields_complete and finish == "tool_calls", len(seen) - 1)

    def _normal_choice(self, choice, seen):
        if type(choice) is not dict or not exposed_index(choice.get("index")):
            self.loss("partial_tool_call")
            return
        message = choice.get("message")
        if type(message) is not dict:
            self.loss("partial_tool_call")
            return
        if message.get("function_call") is not None or choice.get("finish_reason") == "function_call":
            self.loss("partial_tool_call")
        tools = message.get("tool_calls")
        if tools is None:
            return
        if type(tools) is not list:
            self.loss("partial_tool_call")
            return
        for position, tool in enumerate(tools):
            self._normal_tool(tool, choice["index"], position, choice.get("finish_reason"), seen)

    def normal(self, native):
        try:
            payload = project_model(native)
        except Exception:
            self.loss("projection_failed")
            self.terminal()
            return
        try:
            choices = payload.get("choices")
            if type(choices) is not list or not choices:
                self.loss("partial_tool_call")
            else:
                seen = set()
                for choice in choices:
                    self._normal_choice(choice, seen)
        except Exception:
            self.loss("projection_failed")
        self.terminal(payload)
