"""Local portable backup; only a newly owned destination is ever finalized."""
from contextlib import closing
import logging
import os
from pathlib import Path
import tempfile
import sqlite3
import time

from ._inspect_common import (Context, InspectionError, error_envelope, finish_envelope,
                              open_read, storage_error, validate_path, validate_timeout,
                              verify_schema)
from ._logging import emit


_logger = logging.getLogger(__name__)


def _destination_paths(path):
    return [Path(str(path) + suffix) for suffix in ("-wal", "-shm", "-journal", "")]


def _check_destination_available(paths, *, late_suffixes=()):
    """Refuse occupied names, including dangling symlinks, without touching them."""
    if any(os.path.lexists(item) for item in paths):
        if any(os.path.lexists(item) for item in late_suffixes):
            raise InspectionError("recording_unreadable", "destination_finalize",
                                  "Another owner created a destination SQLite sidecar.")
        raise InspectionError("invalid_request", "destination_exists",
                              "The destination or one of its SQLite sidecars already exists.")


def _finalize_destination(connection, ctx):
    ctx.check_deadline()
    mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    if mode == "wal":
        result = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if result[0] != 0:
            raise InspectionError("recording_unreadable", "destination_finalize", "The new snapshot could not be finalized portably.")
    if str(connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower() != "delete":
        raise InspectionError("recording_unreadable", "destination_finalize", "The new snapshot could not be finalized portably.")
    ctx.check_deadline()


def _backup(ctx, destination):
    remaining = max(0, ctx.deadline - time.monotonic())
    with closing(sqlite3.connect(destination.as_uri() + "?mode=rw", uri=True,
                                 timeout=min(1.0, remaining))) as target:
        target.execute("PRAGMA synchronous=FULL")
        target.set_progress_handler(lambda: int(time.monotonic() >= ctx.deadline), 1000)

        def progress(status, remaining_pages, total_pages):
            ctx.check_deadline()

        ctx.connection.backup(target, pages=256, progress=progress, sleep=0.01)
        _finalize_destination(target, ctx)


def _verified_states(destination, ctx):
    remaining = max(0, ctx.deadline - time.monotonic())
    with closing(sqlite3.connect(destination.as_uri() + "?mode=ro", uri=True,
                                 timeout=min(1.0, remaining))) as target:
        target.row_factory = sqlite3.Row
        target.set_progress_handler(lambda: int(time.monotonic() >= ctx.deadline), 1000)
        target.execute("PRAGMA query_only=ON")
        target.execute("BEGIN")
        if str(target.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "delete":
            raise InspectionError("recording_unreadable", "destination_verification", "The snapshot did not reopen as a portable DELETE database.")
        verify_schema(target)
        states = []
        for row in target.execute("SELECT run_id,close_state FROM runs ORDER BY started_at,run_id"):
            ctx.check_deadline()
            states.append(dict(row))
        return states


def _close_source(ctx):
    if ctx is not None and ctx.connection is not None:
        connection = ctx.connection
        ctx.connection = None
        connection.set_progress_handler(None, 0)
        try:
            connection.rollback()
        finally:
            connection.close()


def _publish_staged(staged, destination, paths, late_suffixes, ctx):
    """Atomically create the public name only after a complete private copy.

    link() is exclusive: it fails if a competing main name appeared, even if
    that name is a dangling symlink. No failure path unlinks a public pathname.
    """
    ctx.check_deadline()
    _check_destination_available(paths, late_suffixes=late_suffixes)
    ctx.check_deadline()
    try:
        os.link(staged, destination)
    except FileExistsError:
        raise InspectionError("invalid_request", "destination_exists",
                              "The destination already exists.") from None
    except OSError as error:
        # An interrupted link may have succeeded in the kernel. Never unlink
        # its public pathname: a different process may already have replaced it.
        raise InspectionError("recording_unreadable", "storage_failure",
                              "Snapshot publication could not be confirmed; any public file was left untouched.") from error

    # The native link succeeded, but a competing SQLite sidecar may have been
    # created during that call. Preserve every public name and report uncertainty.
    if any(os.path.lexists(sidecar) for sidecar in late_suffixes):
        raise InspectionError(
            "recording_unreadable", "destination_finalize",
            "A destination SQLite sidecar appeared during publication; "
            "the public main may already exist. Inspect the destination before using it.",
        )


def snapshot(path, output, *, timeout=10.0):
    """Make a verified portable copy and publish its main file without overwrite.

    All SQLite writing and verification happen under a private 0700 staging
    directory. An error or interruption never removes a public destination:
    no public name exists until the single exclusive publication operation.
    """
    ctx = None
    stage = None
    cleanup_failed = False
    result = None
    try:
        timeout = validate_timeout(timeout)
        source, destination = validate_path(path), validate_path(output)
        if source == destination:
            raise InspectionError("invalid_request", "self_copy", "The snapshot destination must differ from its source.")
        ctx = Context("snapshot", {}, timeout=timeout)
        open_read(ctx, source)
        # Force an actual source read before inspecting prospective output names.
        ctx.connection.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
        paths = _destination_paths(destination)
        requested_paths = _destination_paths(Path(os.fspath(output)).absolute())
        _check_destination_available(paths + requested_paths)

        # SQLite owns all its sidecars under this same-filesystem private stage.
        stage = tempfile.TemporaryDirectory(prefix=".mfr-snapshot-", dir=destination.parent)
        staged = Path(stage.name) / "copy.sqlite"
        descriptor = os.open(staged, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        os.close(descriptor)
        _backup(ctx, staged)
        states = _verified_states(staged, ctx)
        data = {"output_path": str(destination), "journal_mode": "delete", "verified": True,
                "source_run_states": states}

        def shrink():
            if not states:
                return False
            states.pop()
            return True

        # Validation, result preparation, source close and deadline check all
        # precede the first public pathname creation. Never delete after link.
        result = finish_envelope(ctx, data, shrink)
        _close_source(ctx)
        ctx.check_deadline()
        _publish_staged(staged, destination, paths + requested_paths,
                        paths[:-1] + requested_paths[:-1], ctx)
    except InspectionError as error:
        result = error_envelope("snapshot", error.code, error.reason, error.message)
    except sqlite3.Error as error:
        translated = storage_error(ctx, error)
        result = error_envelope("snapshot", translated.code, translated.reason, translated.message)
    except OSError:
        result = error_envelope("snapshot", "recording_unreadable", "storage_failure", "The snapshot files could not be read or created.")
    finally:
        # Closing the source and removing the private stage cannot delete
        # another owner's public main or suffix, even on interruption.
        try:
            _close_source(ctx)
        except (OSError, sqlite3.Error):
            cleanup_failed = True
        if stage is not None:
            try:
                stage.cleanup()
            except OSError:
                cleanup_failed = True
        if cleanup_failed:
            result = error_envelope("snapshot", "recording_unreadable", "output_cleanup_failed",
                                    "The snapshot could not clean up its private staging files.")
            emit(_logger, logging.ERROR, "snapshot_failed code=recording_unreadable reason=output_cleanup_failed")
    if not cleanup_failed:
        if result["ok"]:
            emit(_logger, logging.INFO, "snapshot_completed")
        else:
            error = result["error"]
            level = logging.WARNING if error["code"] in {
                "incompatible_schema", "recording_unreadable", "recording_open_wal", "query_timeout"
            } else logging.DEBUG
            emit(_logger, level, "snapshot_failed code=%s reason=%s", error["code"], error["reason"])
    return result
