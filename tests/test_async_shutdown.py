"""R2: async shutdown owns initiation and its budget before executor admission."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import sqlite3
import threading

import pytest
import pytest_check as check

from mcp_flightrecorder import Recorder


COVERAGE = {"producers": {"model": "model", "host": "host", "mcp": "mcp"}}


def accepted(receipt):
    assert receipt.status == "accepted", f"The shutdown control requires admitted evidence before closing: {receipt.reason}."
    return receipt


@pytest.fixture
def held_recording(tmp_path, monkeypatch):
    rec = Recorder(tmp_path / "capture.sqlite", capture="content", coverage=COVERAGE,
                   limits={"batch_items": 1})
    entered, release = threading.Event(), threading.Event()
    original = rec._persist_bundle

    def hold_start(connection, bundle):
        if bundle.kind == "mcp.request":
            entered.set()
            assert release.wait(10), "The test must release its independent writer gate during bounded cleanup."
        return original(connection, bundle)

    monkeypatch.setattr(rec, "_persist_bundle", hold_start)
    try:
        start = accepted(rec.start_operation(scope=rec.capture_scope(), client_id="client",
                                             method="tools/call", arguments={"accepted_before_close": True}))
        assert entered.wait(2), "The real writer must hold the admitted start before async shutdown is exercised."
        terminal = accepted(rec.finish_operation(start.ticket, status="ok", result={"drained": True}, resolution="remote"))
        yield rec, release, (start.event_id, terminal.event_id)
    finally:
        release.set()
        result = rec.close(timeout=2)
        assert result.writer_stopped, "Fixture cleanup must join only its own released recorder writer."


@pytest.fixture
def occupied_executor():
    entered, release = threading.Event(), threading.Event()
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-occupied-default")

    def occupy():
        entered.set()
        assert release.wait(10), "The test must release its one real executor worker during bounded cleanup."

    held = pool.submit(occupy)
    try:
        assert entered.wait(2), "The sole default-executor worker must be occupied before aclose is scheduled."
        yield pool, release, held
    finally:
        release.set()
        held.result(timeout=2)
        pool.shutdown(wait=True)


async def writer_exited_without_new_shutdown(rec):
    """Observe a real join outside the occupied default executor; request no close."""
    loop = asyncio.get_running_loop()
    reported = asyncio.Event()

    def observe_exit():
        rec._writer_thread.join(timeout=1)
        loop.call_soon_threadsafe(reported.set)

    observer = threading.Thread(target=observe_exit, name="test-observe-writer-exit")
    observer.start()
    try:
        await asyncio.wait_for(reported.wait(), timeout=1.5)
    finally:
        observer.join(timeout=2)
    assert not observer.is_alive(), "The test-owned join observer must end before its event loop closes."
    return not rec._writer_thread.is_alive()


def check_reclaimed(rec):
    health = rec.health()
    check.equal((health.ordinary.used_slots, health.ordinary.used_bytes,
                 health.completion.used_slots, health.completion.used_bytes, health.open_operations),
                (0, 0, 0, 0, 0),
                msg="Draining accepted work releases ordinary buffers and the original completion reservation exactly once, never below zero.")
    check.equal(health.writer_errors, 0,
                msg="Executor contention, caller cancellation and timeout must not manufacture writer storage errors.")


def check_persisted(path, event_ids):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        check.equal(reader.execute("SELECT event_id FROM events ORDER BY sequence").fetchall(),
                    [(identity,) for identity in event_ids],
                    msg="Shutdown must drain the original admitted start and terminal, with no late capture after admission stops.")
        check.equal(reader.execute("SELECT status,end_event_id FROM operations").fetchall(),
                    [("ok", event_ids[1])],
                    msg="Cancellation or timeout of a close waiter cannot discard an already accepted atomic operation completion.")
        check.is_in(reader.execute("SELECT close_state FROM runs").fetchone()[0], ("clean", "incomplete"),
                    msg="The existing shutdown request must persist logical closure rather than leave an open run.")


def finish_and_verify(rec, path, event_ids):
    closed = rec.close(timeout=2)
    assert closed.writer_stopped, "After releasing both gates, a bounded cleanup close must join the original writer."
    portable = closed if closed.portable else rec.finalize(timeout=2)
    check.is_true(portable.writer_stopped and portable.portable,
                  msg="An expired original close budget must still permit a fresh-owner retry to verify a portable recording.")
    check_persisted(path, event_ids)
    check_reclaimed(rec)


def test_queued_aclose_cancellation_preserves_shutdown_before_executor_starts(
        tmp_path, held_recording, occupied_executor, run_sdk_async):
    rec, writer_release, event_ids = held_recording
    pool, executor_release, held = occupied_executor

    async def exercise():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(pool)
        begun = asyncio.Event()

        async def begin_close():
            begun.set()
            # There is no intervening await: the observer resumes only after the
            # actual aclose coroutine executes through its first suspension.
            return await rec.aclose(timeout=1.5)

        waiter = asyncio.create_task(begin_close())
        try:
            await begun.wait()
            assert not waiter.done(), "The cancelled-waiter control must reach aclose while both real resource gates remain held."
            check.is_false(executor_release.is_set(), msg="No executor capacity may become available before queued cancellation is exercised.")
            check.is_false(writer_release.is_set(), msg="The writer must still own accepted work when the async waiter is cancelled.")
            cancellation = "queued-close-host-cancellation"
            assert waiter.cancel(cancellation), "The pending async waiter must accept the host's cancellation request."
            # The host's cancellation must propagate before testing independent recorder shutdown.
            with pytest.raises(asyncio.CancelledError) as caught:
                await waiter
            check.equal(caught.value.args, (cancellation,),
                        msg="Async close must propagate the original host cancellation payload rather than replace it with a recorder result.")
            check.equal(rec.health().state, "stopped",
                        msg="Beginning aclose must stop admission even if its executor callable never starts.")
            late_event = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
            late_start = rec.start_operation(scope=rec.capture_scope(), client_id="late-client",
                                             method="tools/call", arguments={})
            check.equal((late_event.status, late_event.reason), ("closed", "closed"),
                        msg="Cancelling a queued close waiter cannot reopen ordinary event admission.")
            check.equal((late_start.status, late_start.reason), ("closed", "closed"),
                        msg="Cancelling a queued close waiter cannot admit a new operation reservation.")
            check.is_none(late_start.ticket, msg="A late operation attempt must not acquire a terminal ticket after shutdown begins.")

            writer_release.set()
            # This observer only joins the actual writer. No new close/finalize
            # call and no available default-executor job can initiate shutdown.
            assert await writer_exited_without_new_shutdown(rec), (
                "The original aclose request must autonomously drain and stop its writer while the executor remains occupied."
            )
            check.is_false(executor_release.is_set(),
                           msg="Writer exit must be proven before executor release could run another queued callable.")
            check.is_false(held.done(), msg="The default executor must still be occupied when autonomous writer shutdown is established.")
            check_persisted(tmp_path / "capture.sqlite", event_ids)
            check_reclaimed(rec)
        finally:
            writer_release.set()
            executor_release.set()
            if not waiter.done():
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)

    run_sdk_async(exercise(), timeout=2)
    finish_and_verify(rec, tmp_path / "capture.sqlite", event_ids)


def test_queued_aclose_timeout_expires_while_executor_and_writer_remain_held(
        tmp_path, held_recording, occupied_executor, run_sdk_async):
    rec, writer_release, event_ids = held_recording
    pool, executor_release, held = occupied_executor

    async def exercise():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(pool)
        heartbeat = []
        waiter = None

        async def begin_close():
            loop.call_soon(lambda: heartbeat.append(not waiter.done()))
            return await rec.aclose(timeout=0.05)

        waiter = asyncio.create_task(begin_close())
        try:
            # The shared runner supplies the independent two-second watchdog.
            # Both gates remain closed until a real public result is inspected.
            result = await waiter
            check.equal((result.reason, result.writer_stopped, result.portable), ("timeout", False, False),
                        msg="The original 0.05-second caller budget includes executor queue delay and cannot report a held writer joined or portable.")
            check.equal(heartbeat, [True],
                        msg="An event-loop heartbeat must run while the real close waiter is pending, without waiting for either resource gate.")
            check.is_false(writer_release.is_set(), msg="The timeout receipt must arrive before the independently held writer is released.")
            check.is_false(executor_release.is_set(), msg="The timeout receipt must arrive before executor capacity becomes available.")
            check.is_false(held.done(), msg="A queued timeout must complete while the sole real default-executor worker is still occupied.")
            check.equal(rec.health().state, "stopped", msg="A caller timeout cannot undo the shutdown request or reopen admission.")
            health = rec.health()
            check.equal((health.ordinary.used_slots, health.completion.used_slots,
                         health.completion.used_bytes, health.open_operations), (1, 1, 4096, 1),
                        msg="Timeout cannot release the ordinary start or its accepted terminal reservation while the writer still owns them.")
            check.is_true(health.ordinary.used_bytes > 0,
                          msg="The writer-held immutable buffers must remain charged through the timed-out wait.")
            late = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
            check.equal((late.status, late.reason), ("closed", "closed"),
                        msg="The timed-out async caller must already have stopped future evidence admission.")
        finally:
            writer_release.set()
            executor_release.set()
            if not waiter.done():
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)

    run_sdk_async(exercise(), timeout=2)
    finish_and_verify(rec, tmp_path / "capture.sqlite", event_ids)


def test_available_executor_aclose_starts_when_awaited_and_closes_normally(tmp_path, run_sdk_async):
    rec = Recorder(tmp_path / "capture.sqlite", capture="metadata", coverage=COVERAGE)
    pending_close = rec.aclose(timeout=1)
    try:
        check.equal(rec.health().state, "accepting",
                    msg="Creating an unscheduled aclose coroutine must not begin shutdown before it executes.")
        event = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))

        async def exercise():
            result = await pending_close
            check.equal((result.writer_stopped, result.portable, result.reason), (True, True, None),
                        msg="With executor capacity available, aclose must join its real writer and verify standalone portability.")
            repeated = await rec.aclose(timeout=1)
            check.equal(repeated, result, msg="Repeated async close must preserve the already verified shutdown result.")

        run_sdk_async(exercise(), timeout=2)
        late = rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"})
        check.equal((late.status, late.reason), ("closed", "closed"),
                    msg="The ordinary successful async control must stop later capture admission.")
        with closing(sqlite3.connect((tmp_path / "capture.sqlite").as_uri() + "?mode=ro", uri=True)) as reader:
            check.equal(reader.execute("SELECT event_id FROM events").fetchall(), [(event.event_id,)],
                        msg="Available-executor async close must retain the actual event admitted before awaiting the coroutine.")
        check_reclaimed(rec)
    finally:
        pending_close.close()
        rec.close(timeout=2)


@pytest.mark.parametrize("timeout", [0, -1, True, float("nan"), float("inf"), 10**400],
                         ids=["zero", "negative", "boolean", "nan", "infinity", "unrepresentable"])
def test_invalid_async_timeout_preserves_accepting_state(tmp_path, timeout, run_sdk_async):
    rec = Recorder(tmp_path / "capture.sqlite", capture="metadata", coverage=COVERAGE)
    try:
        async def exercise():
            # Invalid async wait configuration must fail before requesting shutdown.
            with pytest.raises(ValueError):
                await rec.aclose(timeout=timeout)

        run_sdk_async(exercise(), timeout=2)
        check.equal(rec.health().state, "accepting",
                    msg="Validation of an invalid async timeout must precede any shutdown transition.")
        event = accepted(rec.observe("cache.served", scope=rec.capture_scope(), facts={"owner": "host"}))
        closed = rec.close(timeout=2)
        assert closed.writer_stopped, "The valid follow-up must finish before persisted evidence is inspected."
        with closing(sqlite3.connect((tmp_path / "capture.sqlite").as_uri() + "?mode=ro", uri=True)) as reader:
            check.equal(reader.execute("SELECT event_id FROM events").fetchall(), [(event.event_id,)],
                        msg="An invalid async timeout must leave subsequent valid capture usable and durable.")
        check_reclaimed(rec)
    finally:
        rec.close(timeout=2)
