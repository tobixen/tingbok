"""/health surfaces the deployment's update state.

The VM reinstalls itself from git on a timer.  When that reinstall fails the
running service does not notice: it keeps serving from the code it imported at
startup, so nothing is visibly wrong until the next restart.  These tests pin
the only channel that reports it without an ssh session.
"""

import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

import tingbok.app as app_module
from tingbok.app import app

_SCRIPT = Path(__file__).parents[1] / "scripts" / "update_status.py"
_spec = importlib.util.spec_from_file_location("update_status", _SCRIPT)
assert _spec is not None
assert _spec.loader is not None
update_status = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(update_status)


@pytest.fixture
def update_status_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the app at a throwaway update-status file."""
    path = tmp_path / "update-status.json"
    monkeypatch.setattr(app_module, "_UPDATE_STATUS_FILE", path)
    return path


async def _health(client: tuple[str, int] = ("127.0.0.1", 12345)) -> dict:
    async with AsyncClient(transport=ASGITransport(app=app, client=client), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    return response.json()


@pytest.mark.anyio
async def test_health_degraded_when_venv_behind_repo(update_status_file: Path):
    """A venv older than the checkout means a reinstall failed and was retried."""
    update_status_file.write_text(
        json.dumps(
            {
                "repo_rev": "5755391",
                "venv_rev": "2686388",
                "install_failures": 4,
                "last_error": "No matching distribution found for fastmcp>=4.0.1",
                "last_attempt": "2026-09-05T14:30:04+00:00",
                "last_success": "2026-09-05T12:00:03+00:00",
            }
        )
    )
    data = await _health()
    assert data["status"] == "degraded"
    update = data["update"]
    assert update["venv_rev"] == "2686388"
    assert update["repo_rev"] == "5755391"
    assert update["install_failures"] == 4
    assert "fastmcp" in update["last_error"]


@pytest.mark.anyio
async def test_health_degraded_when_failures_without_drift(update_status_file: Path):
    """A failed merge or push leaves the venv current but still needs flagging."""
    update_status_file.write_text(
        json.dumps(
            {
                "repo_rev": "5755391",
                "venv_rev": "5755391",
                "install_failures": 2,
                "last_error": "merge of origin/main conflicts",
                "stage": "merge",
            }
        )
    )
    data = await _health()
    assert data["status"] == "degraded"
    assert data["update"]["stage"] == "merge"


@pytest.mark.anyio
async def test_health_ok_when_update_current(update_status_file: Path):
    """A healthy deployment still reports its state, just not as degraded."""
    update_status_file.write_text(
        json.dumps(
            {
                "repo_rev": "5755391",
                "venv_rev": "5755391",
                "install_failures": 0,
                "last_success": "2026-09-05T14:30:04+00:00",
            }
        )
    )
    data = await _health()
    assert data["status"] == "ok"
    assert data["update"]["last_success"] == "2026-09-05T14:30:04+00:00"


@pytest.mark.anyio
async def test_health_omits_update_when_not_deployed(update_status_file: Path):
    """No status file at all is the normal case off the VM, not a fault."""
    assert not update_status_file.exists()
    data = await _health()
    assert data["status"] == "ok"
    assert data.get("update") is None


@pytest.mark.anyio
async def test_health_survives_malformed_update_status(update_status_file: Path):
    """A half-written file must not take the health endpoint down with it.

    Answering is the requirement; answering *ok* is not.  A file configured and
    unreadable means the reporting channel is broken — a truncated write, the
    wrong owner, a deleted file — and reporting health at that moment is the
    monitor going green exactly when it has lost sight of the thing it watches.
    """
    update_status_file.write_text("{ this is not json")
    data = await _health()
    assert data["status"] == "degraded"
    assert data.get("update") is None


@pytest.mark.anyio
async def test_an_unreadable_status_file_is_distinguishable_from_no_deployment(
    update_status_file: Path,
):
    """Configured-but-broken and never-configured must not look the same.

    Both yield no ``update`` block, so the status field is the only thing left
    to tell them apart.
    """
    update_status_file.write_text("{ truncated")
    broken = await _health()

    app_module._UPDATE_STATUS_FILE = None
    try:
        undeployed = await _health()
    finally:
        app_module._UPDATE_STATUS_FILE = update_status_file

    assert broken.get("update") is None
    assert undeployed.get("update") is None
    assert broken["status"] != undeployed["status"]


# --- scripts/update_status.py, the other half of the channel ----------------


def _run(path: Path, *args: str) -> dict:
    assert update_status.main([str(path), "--repo-rev", "aaa", "--venv-rev", "bbb", *args]) == 0
    return json.loads(path.read_text())


def test_failures_accumulate_across_runs(tmp_path: Path):
    """The counter is what distinguishes a blip from a deployment stuck for hours."""
    path = tmp_path / "update-status.json"
    assert _run(path, "--failure", "pip", "--error", "boom")["install_failures"] == 1
    second = _run(path, "--failure", "pip", "--error", "boom again")
    assert second["install_failures"] == 2
    assert second["stage"] == "pip"
    assert second["last_error"] == "boom again"


def test_success_clears_the_failure_fields(tmp_path: Path):
    """A recovered deployment must stop reporting the failure it recovered from."""
    path = tmp_path / "update-status.json"
    _run(path, "--failure", "merge", "--error", "conflict")
    recovered = _run(path, "--success")
    assert recovered["install_failures"] == 0
    assert recovered["stage"] is None
    assert recovered["last_error"] is None
    assert recovered["last_success"] == recovered["last_attempt"]


def test_success_timestamp_survives_a_later_failure(tmp_path: Path):
    """How long it has been broken is the first thing you want to know."""
    path = tmp_path / "update-status.json"
    succeeded_at = _run(path, "--success")["last_success"]
    assert _run(path, "--failure", "pip", "--error", "boom")["last_success"] == succeeded_at


def test_corrupt_previous_status_is_not_fatal(tmp_path: Path):
    """Losing the history is no reason to also lose the failure being recorded."""
    path = tmp_path / "update-status.json"
    path.write_text("{ truncated")
    assert _run(path, "--failure", "pip", "--error", "boom")["install_failures"] == 1


def test_long_error_keeps_the_tail(tmp_path: Path):
    """pip names the actual problem on its last line, after pages of context."""
    path = tmp_path / "update-status.json"
    error = "noise\n" * 5000 + "No matching distribution found for fastmcp>=4.0.1"
    stored = _run(path, "--failure", "pip", "--error", error)["last_error"]
    assert len(stored) <= update_status.MAX_ERROR_CHARS
    assert stored.endswith("No matching distribution found for fastmcp>=4.0.1")


@pytest.mark.anyio
async def test_health_reads_what_the_script_writes(update_status_file: Path):
    """Round trip: the writer's output must be exactly what /health can parse."""
    update_status.main(
        [
            str(update_status_file),
            "--repo-rev",
            "5755391",
            "--venv-rev",
            "2686388",
            "--failure",
            "pip",
            "--error",
            "No matching distribution found for fastmcp>=4.0.1",
        ]
    )
    data = await _health()
    assert data["status"] == "degraded"
    assert data["update"]["stage"] == "pip"
    assert data["update"]["install_failures"] == 1


# --- what a remote client is allowed to see --------------------------------


@pytest.mark.anyio
async def test_remote_client_sees_the_fault_but_not_the_error_text(update_status_file: Path):
    """pip and git errors quote filesystem paths; the rest is safe to publish.

    Dropping the whole block would defeat the point of putting it on /health,
    which is that a monitor outside the VM can see a stuck deployment.
    """
    update_status_file.write_text(
        json.dumps(
            {
                "repo_rev": "5755391",
                "venv_rev": "2686388",
                "install_failures": 4,
                "stage": "pip",
                "last_error": "could not install /opt/tingbok/repo",
            }
        )
    )
    data = await _health(client=("2001:db8::1", 54321))
    assert data["status"] == "degraded"
    assert data["update"]["install_failures"] == 4
    assert data["update"]["venv_rev"] == "2686388"
    assert data["update"]["stage"] == "pip"
    assert data["update"]["last_error"] is None


@pytest.mark.anyio
async def test_localhost_still_gets_the_error_text(update_status_file: Path):
    """On the VM itself the error is the whole point of looking."""
    update_status_file.write_text(
        json.dumps({"repo_rev": "a", "venv_rev": "b", "install_failures": 1, "last_error": "boom"})
    )
    assert (await _health())["update"]["last_error"] == "boom"


# --- silence is a fault too ------------------------------------------------


def _stamp(age_seconds: float) -> str:
    return (datetime.now().astimezone() - timedelta(seconds=age_seconds)).isoformat(timespec="seconds")


@pytest.mark.anyio
async def test_an_updater_that_stopped_running_is_degraded(update_status_file: Path):
    """The failures only cover runs that got far enough to report one.

    A run killed before that — no network for the fetch, a stopped timer, a unit
    that will not start — leaves this file exactly as a healthy one looks, so a
    last_attempt that has stopped advancing is the only evidence there is.
    """
    update_status_file.write_text(
        json.dumps(
            {
                "repo_rev": "a",
                "venv_rev": "a",
                "install_failures": 0,
                "last_attempt": _stamp(4000),
                "stale_after_seconds": 2700,
            }
        )
    )
    assert (await _health())["status"] == "degraded"


@pytest.mark.anyio
async def test_a_recent_run_is_not_degraded(update_status_file: Path):
    """The common case: the timer is firing and everything worked."""
    update_status_file.write_text(
        json.dumps(
            {
                "repo_rev": "a",
                "venv_rev": "a",
                "install_failures": 0,
                "last_attempt": _stamp(60),
                "stale_after_seconds": 2700,
            }
        )
    )
    assert (await _health())["status"] == "ok"


@pytest.mark.anyio
async def test_no_threshold_means_no_staleness_check(update_status_file: Path):
    """A status file written before the threshold existed must not read as broken."""
    update_status_file.write_text(
        json.dumps({"repo_rev": "a", "venv_rev": "a", "install_failures": 0, "last_attempt": _stamp(999999)})
    )
    assert (await _health())["status"] == "ok"


@pytest.mark.anyio
async def test_unparseable_timestamp_is_not_fatal(update_status_file: Path):
    """/health answering at all matters more than this particular signal."""
    update_status_file.write_text(
        json.dumps(
            {
                "repo_rev": "a",
                "venv_rev": "a",
                "install_failures": 0,
                "last_attempt": "not a date",
                "stale_after_seconds": 60,
            }
        )
    )
    assert (await _health())["status"] == "ok"


def test_the_script_records_the_threshold(tmp_path: Path):
    """Written every run, so changing the timer interval needs no migration."""
    path = tmp_path / "update-status.json"
    assert (
        update_status.main([str(path), "--repo-rev", "a", "--venv-rev", "a", "--stale-after", "2700", "--success"]) == 0
    )
    assert json.loads(path.read_text())["stale_after_seconds"] == 2700
