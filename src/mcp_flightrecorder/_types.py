"""Immutable public receipts and internal ownership records."""
from dataclasses import dataclass, field
import json
from typing import Any


class RecorderError(RuntimeError):
    """The new recording could not be initialized."""


@dataclass(frozen=True)
class CaptureResult:
    status: str
    reason: str | None = None
    event_id: str | None = None
    turn_id: str | None = None
    model_call_id: str | None = None
    decision_id: str | None = None
    operation_id: str | None = None
    ticket: Any = None
    losses: tuple[str, ...] = ()


@dataclass(frozen=True)
class CaptureScope:
    run_id: str
    turn_id: str | None
    dispatch_event_id: str | None
    _disabled: bool = field(repr=False)
    _seal: bytes = field(repr=False)


@dataclass(frozen=True)
class Capacity:
    used_bytes: int
    max_bytes: int
    used_slots: int
    max_slots: int


@dataclass(frozen=True)
class Health:
    state: str
    dropped_events: int
    writer_errors: int
    reason_counts: Any
    open_operations: int
    ordinary: Capacity
    completion: Capacity


@dataclass(frozen=True)
class CloseResult:
    writer_stopped: bool
    run_close_state: str | None
    portable: bool
    reason: str | None


@dataclass(frozen=True)
class Identity:
    category: str
    key: str
    encoded: bytes


@dataclass(frozen=True)
class Bundle:
    """Frozen encoded ownership; decoded working objects are writer-local."""
    encoded: bytes
    bodies: tuple[tuple[int, bytes], ...]
    kind: str
    turn_id: str | None
    identities: tuple[Identity, ...]
    receipt: CaptureResult
    is_event: bool
    losses: tuple[str, ...]
    partial: bool
    ticket: Any
    terminal: bool
    optional: bytes
    admitted_at: float = 0.0

    @property
    def metadata_bytes(self):
        return len(self.encoded) + sum(len(item.encoded) for item in self.identities)

    @property
    def body_bytes(self):
        return sum(len(body) for _, body in self.bodies)

    @property
    def size(self):
        return self.metadata_bytes + self.body_bytes + len(self.optional)

    @property
    def ordinary_charge(self):
        return self.body_bytes + len(self.optional) if self.terminal else self.size

    def decode(self):
        value = json.loads(self.encoded)
        if self.optional:
            value["artifacts"] = json.loads(self.optional)
        for index, body in self.bodies:
            value["artifacts"][index]["body_text"] = body.decode("utf-8")
        return value


class Ticket:
    """Only the recorder mutates reservation state, under its admission lock."""
    __slots__ = ("owner", "operation_id", "encoded", "phase", "reserved")

    def __init__(self, owner, operation_id, encoded):
        self.owner = owner
        self.operation_id = operation_id
        self.encoded = encoded
        self.phase = "new"
        self.reserved = False


class CaptureRejected(ValueError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


class LogicalRejection(Exception):
    """An explicitly recognized transition rejection, never a SQLite error."""
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)
