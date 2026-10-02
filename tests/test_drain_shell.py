# =============================================================================
# D1 / R8 -- drain's shell side, asserted by what it prints and returns.
# =============================================================================
#
# The bodies of cmd_drain, drain_driver and rotate_predrain_dumps are
# taken from deploy/comms-deploy.sh as they are and run under `bash -c`.
# docker compose is replaced by a shell FUNCTION declared in the same
# command line (`compose`), and $COMPOSE_CMD names it: no file is
# written for it, here or in a temporary directory (D1-2 gate). The
# driver's python never runs -- `compose run` swallows the heredoc and
# answers with the code the scenario sets; the driver itself is run for
# real in tests/test_drain_source.py.
#
# MUTATIONS:
#   M18 "at or past 0013" back to code 2 -> tests/test_drain_source.py
#       test_drain_past_0013_has_nothing_to_drain
#   M19 rotation before the dump       -> test_a_failed_dump_removes_nothing
# =============================================================================

import re
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "comms-deploy.sh"
SELF = "./comms-deploy.sh"


def _function(name: str) -> str:
    """One top-level function of the script, verbatim."""
    text = SCRIPT.read_text(encoding="utf-8")
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert match, f"{name} not found at column zero"
    return match.group(0)


_COMPOSE = r'''
compose() {
    case "$1" in
        ps)   [ "$PG_UP" = 1 ] && echo comms-postgres ;;
        run)  cat >/dev/null; echo "driver: ${9} ${10}"; return "${DRIVER_RC:-0}" ;;
        stop) return "${STOP_RC:-0}" ;;
        exec) [ "${DUMP_RC:-0}" = 0 ] && echo "-- dump" ; return "${DUMP_RC:-0}" ;;
    esac
}
'''


def _drain(
    tmp_path: Path, *args: str, answer: str = "yes\n", **scenario: str,
) -> subprocess.CompletedProcess[str]:
    env_lines = "".join(f"{k}={v}\n" for k, v in scenario.items())
    program = "\n".join([
        "set -u",
        'RED= GREEN= YELLOW= CYAN= NC=',
        f'BACKUP_DIR="{tmp_path}"',
        "POSTGRES_USER=comms POSTGRES_DB=comms",
        "COMPOSE_CMD=compose",
        "cd_compose() { :; }",
        "load_env() { :; }",
        _COMPOSE,
        env_lines,
        _function("rotate_predrain_dumps"),
        _function("drain_driver"),
        _function("cmd_drain"),
        'cmd_drain "$@"',
    ])
    return subprocess.run(
        ["bash", "-c", program, SELF, *args],
        input=answer, capture_output=True, text=True, timeout=30,
    )


def _dumps(tmp_path: Path) -> list[str]:
    return sorted(p.name for p in tmp_path.glob("comms-predrain-*.sql"))


class TestEveryCodeTwoNamesTheNextCommand:
    """Gate correction 1 / done-when (1): every `exit 2` of cmd_drain,
    by its collected output and code."""

    def test_a_wrong_argument(self, tmp_path: Path) -> None:
        done = _drain(tmp_path, "--force", PG_UP="1")
        assert done.returncode == 2
        assert f"Usage: {SELF} drain [--apply]" in done.stdout

    def test_the_database_is_not_running(self, tmp_path: Path) -> None:
        done = _drain(tmp_path, PG_UP="0")
        assert done.returncode == 2
        assert "Next: compose up -d comms-postgres" in done.stdout

    def test_comms_app_cannot_be_stopped(self, tmp_path: Path) -> None:
        done = _drain(tmp_path, "--apply", PG_UP="1", DRIVER_RC="1", STOP_RC="1")
        assert done.returncode == 2
        assert f"Next: {SELF} status" in done.stdout
        assert _dumps(tmp_path) == []

    def test_the_dump_fails(self, tmp_path: Path) -> None:
        done = _drain(tmp_path, "--apply", PG_UP="1", DRIVER_RC="1", DUMP_RC="1")
        assert done.returncode == 2
        assert f"Next: {SELF} start (back in service)" in done.stdout
        assert f"{SELF} drain --apply" in done.stdout

    def test_every_exit_2_in_the_body_is_one_of_these(self) -> None:
        """The pair to the four tests above: they cover every `exit 2`
        cmd_drain has -- one more would be a branch nobody asserted."""
        assert _function("cmd_drain").count("exit 2") == 4


class TestTheOtherCodes:
    def test_rows_to_delete_name_the_apply(self, tmp_path: Path) -> None:
        done = _drain(tmp_path, PG_UP="1", DRIVER_RC="1")
        assert done.returncode == 1
        assert f"Next: {SELF} drain --apply" in done.stdout

    def test_clean_asks_for_nothing(self, tmp_path: Path) -> None:
        done = _drain(tmp_path, PG_UP="1", DRIVER_RC="0")
        assert done.returncode == 0
        assert "Next:" not in done.stdout

    def test_the_driver_gets_the_script_name(self, tmp_path: Path) -> None:
        """drain_driver hands $0 to the driver, which names the next
        command with it (its own refusals: test_drain_source.py)."""
        done = _drain(tmp_path, PG_UP="1", DRIVER_RC="0")
        assert f"driver: check {SELF}" in done.stdout


class TestRotation:
    def _old(self, tmp_path: Path, count: int) -> list[str]:
        names = [f"comms-predrain-20260101-00000{i}.sql" for i in range(count)]
        for name in names:
            (tmp_path / name).write_text("-- old\n")
        return names

    def test_a_fourth_dump_removes_the_oldest(self, tmp_path: Path) -> None:
        """done-when (3): three kept, the oldest gone."""
        old = self._old(tmp_path, 3)
        done = _drain(tmp_path, "--apply", PG_UP="1", DRIVER_RC="1")
        # The apply run answers 1 too (the scenario's driver code).
        assert done.returncode == 1
        kept = _dumps(tmp_path)
        assert len(kept) == 3
        assert old[0] not in kept
        assert old[1] in kept and old[2] in kept
        assert f"removed an older pre-drain dump: {tmp_path}/{old[0]}" in done.stdout

    def test_a_failed_dump_removes_nothing(self, tmp_path: Path) -> None:
        """done-when (3). M19."""
        old = self._old(tmp_path, 4)
        done = _drain(tmp_path, "--apply", PG_UP="1", DRIVER_RC="1", DUMP_RC="1")
        assert done.returncode == 2
        assert _dumps(tmp_path) == sorted(old)

    @pytest.mark.parametrize("count", [0, 2])
    def test_fewer_than_three_are_all_kept(self, tmp_path: Path, count: int) -> None:
        old = self._old(tmp_path, count)
        program = (
            _function("rotate_predrain_dumps")
            + f'rotate_predrain_dumps "{tmp_path}" 3'
        )
        done = subprocess.run(["bash", "-c", program], capture_output=True, text=True)
        assert done.returncode == 0
        assert _dumps(tmp_path) == sorted(old)
