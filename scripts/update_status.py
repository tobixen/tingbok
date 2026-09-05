#!/usr/bin/env python3
"""Record the state of the deployment's self-update for /health to report.

Usage:
    update_status.py FILE --repo-rev REV --venv-rev REV --success
    update_status.py FILE --repo-rev REV --venv-rev REV \
        --failure STAGE [--error TEXT]

``tingbok-update`` on the VM pulls, reinstalls and restarts the service on a
timer.  When one of those steps fails there is nothing to notice it: the
running service keeps answering from the code it imported at startup, and the
breakage only surfaces at the next restart, possibly weeks later.  This script
maintains the small JSON file that ``/health`` reads, so a stuck deployment is
visible over HTTP instead of only in a journal nobody opens.

Everything is merged into whatever the file already holds, because the counter
and the last-success timestamp have to survive across runs.  A missing or
corrupt file is treated as empty rather than as an error — losing the history
is not a reason to also lose the report of the failure being recorded now.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

#: Cap on the stored error text.  Keep the tail: pip and git both put the line
#: that names the actual problem last, after pages of context.
MAX_ERROR_CHARS = 1000


def _now() -> str:
    # .astimezone() rather than datetime.UTC: the offset makes the stamp
    # unambiguous either way, and UTC needs 3.11 while the project still
    # claims 3.10.
    return datetime.now().astimezone().isoformat(timespec="seconds")


def load(path: Path) -> dict[str, Any]:
    """Read the existing status, or an empty dict if there is nothing usable."""
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def update(
    previous: dict[str, Any],
    *,
    repo_rev: str,
    venv_rev: str,
    stage: str | None,
    error: str | None,
    stale_after: int | None = None,
) -> dict[str, Any]:
    """Fold one run's outcome into the previous status.

    ``stage`` is None for a successful run, which clears the failure fields;
    otherwise it names the step that failed and the counter is advanced.
    """
    status = dict(previous)
    status["repo_rev"] = repo_rev
    status["venv_rev"] = venv_rev
    status["last_attempt"] = _now()
    if stale_after is not None:
        # Written on every run so that changing the timer interval takes effect
        # without anyone having to remember this file exists.
        status["stale_after_seconds"] = stale_after
    if stage is None:
        status["install_failures"] = 0
        status["stage"] = None
        status["last_error"] = None
        status["last_success"] = status["last_attempt"]
    else:
        previous_failures = previous.get("install_failures")
        status["install_failures"] = (previous_failures if isinstance(previous_failures, int) else 0) + 1
        status["stage"] = stage
        status["last_error"] = (error or "").strip()[-MAX_ERROR_CHARS:] or None
    return status


def write(path: Path, status: dict[str, Any]) -> None:
    """Write the status atomically, so /health never reads a half-written file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file", type=Path, help="status file to maintain")
    parser.add_argument("--repo-rev", required=True, help="revision of the checkout")
    parser.add_argument(
        "--stale-after",
        type=int,
        default=None,
        help="seconds after which an update run that never happened is itself a fault",
    )
    parser.add_argument("--venv-rev", required=True, help="revision the venv was last installed at")
    outcome = parser.add_mutually_exclusive_group(required=True)
    outcome.add_argument("--success", action="store_true", help="the run completed")
    outcome.add_argument("--failure", metavar="STAGE", help="the named step failed (merge, push, pip)")
    parser.add_argument("--error", default="", help="failure output; the tail is stored")
    args = parser.parse_args(argv)

    status = update(
        load(args.file),
        repo_rev=args.repo_rev,
        venv_rev=args.venv_rev,
        stage=None if args.success else args.failure,
        error=args.error,
        stale_after=args.stale_after,
    )
    write(args.file, status)
    return 0


if __name__ == "__main__":
    sys.exit(main())
