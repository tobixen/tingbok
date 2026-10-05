"""Small filesystem helpers shared by the services."""

from __future__ import annotations

import io
import os
import tempfile
from pathlib import Path
from typing import Any


def write_atomically(path: Path, text: str, *, durable: bool = True) -> None:
    """Replace *path* with *text* via a fsynced temp file and a rename.

    Only the directory has to be writable, not the file: a git command run as
    root once left ``ean-db.json`` owned by root, and an in-place write then
    failed every PUT.  Neither a crash nor a power loss mid-write leaves a
    truncated file behind.  *durable* False skips the fsyncs, for a cache
    that can be fetched again: a crash still never leaves a truncated file,
    but a power loss may lose the write, or on some filesystems leave the
    file empty.
    """
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            if durable:
                f.flush()
                os.fsync(f.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    if not durable:
        return
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


def dump_yaml_atomically(yaml: Any, doc: Any, path: Path) -> None:
    """Serialise *doc* with the ruamel *yaml* instance and replace *path* with it.

    See :func:`write_atomically`: vocabulary.yaml lives in the same checkout as
    ``ean-db.json`` and is as exposed to a root-owned file.
    """
    buf = io.StringIO()
    yaml.dump(doc, buf)
    write_atomically(path, buf.getvalue())
