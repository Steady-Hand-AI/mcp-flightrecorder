"""Choose a destination; the recorder's writer alone reserves the database."""
from datetime import datetime, timezone
import os
from pathlib import Path
import secrets


def recording_path(destination):
    """Preserve an explicit file or choose an absolute file in a directory.

    A trailing separator requests directory mode even when the leaf is missing.
    Only that leaf may be created. Existing directories keep their permissions.
    No database is pre-created: the writer retains its exclusive reservation and
    file/sidecar collision checks.
    """
    try:
        raw = os.fspath(destination)
        path = Path(raw).absolute()
    except (TypeError, ValueError) as error:
        raise ValueError('path must name a new local recording') from error
    if raw and (path.is_dir() or raw.endswith(os.sep)):
        path.mkdir(mode=0o700, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        return path / f'recording-{stamp}-{os.getpid()}-{secrets.token_hex(8)}.sqlite'
    return destination
