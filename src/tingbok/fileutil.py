"""Small filesystem helpers shared by the services."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def write_atomically(path: Path, text: str) -> None:
    """Replace *path* with *text* via a fsynced temp file and a rename.

    Only the directory has to be writable, not the file: a git command run as
    root once left ``ean-db.json`` owned by root, and an in-place write then
    failed every PUT.  Neither a crash nor a power loss mid-write leaves a
    truncated file behind.
    """
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    try:
        dir_fd = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)
