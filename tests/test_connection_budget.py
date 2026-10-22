# =============================================================================
# H7 R-G -- every comms process's pool, together, fits max_connections
# =============================================================================
#
# THE SUM, read off deploy/docker-compose.yml as it ships:
#   api pool x comms-app's uvicorn --workers
#   + worker pool + its wake listener (one connection, own engine)
#   + consumer pool (the push relay shares it)
#   + RESERVED_CONNECTIONS (migration, psql / pg_dump, the deploy probe)
#   <= max_connections - superuser_reserved_connections
# max_connections is the number written in the compose file -- never a
# default assumed here. superuser_reserved_connections is Postgres'
# default (3): comms connects as POSTGRES_USER, a superuser, who may use
# those three -- counted against the sum anyway, so the budget does not
# lean on who the role is.
#
# MUTATIONS these tests were written against (each turns one red):
#   M10 comms-app at --workers 3            -> TestTheSum
#   M11 an engine built with no role        -> TestTheRole
#   M12 max_connections not written         -> TestTheSum
# =============================================================================

import inspect
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

import app.consumer as consumer_entry
import app.core.database as database
import app.main as api_entry
import app.worker as worker_entry
from app.core.database import (
    POOLS,
    RESERVED_CONNECTIONS,
    ProcessRole,
    dispose_engine,
    get_engine,
    set_process_role,
)

_COMPOSE = Path(__file__).resolve().parents[1] / "deploy" / "docker-compose.yml"
_SUPERUSER_RESERVED = 3  # Postgres' default superuser_reserved_connections
_LISTENER = 1  # the worker's wake listener, outside its pool


def _services() -> dict[str, Any]:
    services: dict[str, Any] = yaml.safe_load(
        _COMPOSE.read_text(encoding="utf-8"),
    )["services"]
    return services


def _command(service: dict[str, Any]) -> str:
    command = service.get("command", "")
    return " ".join(command) if isinstance(command, list) else str(command)


def _role_of(command: str) -> ProcessRole:
    if "uvicorn app.main:app" in command:
        return ProcessRole.API
    if "app.worker" in command:
        return ProcessRole.WORKER
    if "app.consumer" in command:
        return ProcessRole.CONSUMER
    raise AssertionError(f"a comms process with no role: {command!r}")


def _max_connections() -> int:
    command = _command(_services()["comms-postgres"])
    found = re.search(r"max_connections=(\d+)", command)
    assert found is not None, "comms-postgres must write max_connections"
    return int(found.group(1))


def _budget() -> dict[str, int]:
    """Every connection comms can hold at once, by where it comes from."""
    held: dict[str, int] = {}
    for name, service in _services().items():
        if "build" not in service:  # postgres, redis: not comms processes
            continue
        command = _command(service)
        role = _role_of(command)
        processes = 1
        if role is ProcessRole.API:
            workers = re.search(r"--workers (\d+)", command)
            assert workers is not None, "uvicorn's --workers must be written"
            processes = int(workers.group(1))
        held[name] = processes * POOLS[role].capacity
        if role is ProcessRole.WORKER:
            held[f"{name} listener"] = _LISTENER
    held["reserved"] = RESERVED_CONNECTIONS
    return held


class TestTheSum:
    """done-when R-G (2). M10, M12."""

    def test_it_fits(self) -> None:
        held = _budget()
        assert held == {
            "comms-app": 2 * 20,
            "comms-worker": 35,
            "comms-worker listener": 1,
            "comms-consumer": 10,
            "reserved": 3,
        }
        assert sum(held.values()) == 89
        assert _max_connections() == 100
        assert sum(held.values()) <= _max_connections() - _SUPERUSER_RESERVED

    def test_one_more_api_worker_would_not(self) -> None:
        """The edge, computed: a third uvicorn worker is 109 > 97 -- the
        compose file cannot grow it without this test going red."""
        third = sum(_budget().values()) + POOLS[ProcessRole.API].capacity
        assert third > _max_connections() - _SUPERUSER_RESERVED

    def test_every_process_of_the_image_has_a_role(self) -> None:
        """SHORTAGE: a new service built from the image and not mapped to
        a role fails here, not silently outside the sum."""
        built = [n for n, s in _services().items() if "build" in s]
        assert sorted(built) == ["comms-app", "comms-consumer", "comms-worker"]
        assert {_role_of(_command(_services()[n])) for n in built} == set(
            ProcessRole,
        )


class TestTheRole:
    """No engine without a role; the role sizes the pool. M11."""

    async def test_no_role_no_engine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        await dispose_engine()
        monkeypatch.setattr(database, "_role", None)
        with pytest.raises(RuntimeError, match="no process role"):
            get_engine()

    @pytest.mark.parametrize("role", list(ProcessRole))
    async def test_the_role_sizes_the_pool(self, role: ProcessRole) -> None:
        await dispose_engine()
        set_process_role(role)
        try:
            pool: Any = get_engine().pool
            assert (pool.size(), pool._max_overflow) == (
                POOLS[role].size, POOLS[role].overflow,
            )
        finally:
            await dispose_engine()
            set_process_role(ProcessRole.WORKER)

    @pytest.mark.parametrize(
        ("entry", "role"),
        [
            (worker_entry.main, ProcessRole.WORKER),
            (consumer_entry.main, ProcessRole.CONSUMER),
            (api_entry.lifespan, ProcessRole.API),
        ],
    )
    def test_each_entry_point_names_its_role(
        self, entry: Any, role: ProcessRole,
    ) -> None:
        """Each entry point sets its role before anything that could open
        the database -- read in its body."""
        body = inspect.getsource(entry)
        call = f"set_process_role(ProcessRole.{role.name})"
        assert call in body
        assert body.index(call) < min(
            (body.index(word) for word in ("install_profile", "asyncio.run")
             if word in body),
            default=len(body),
        )

    async def test_the_api_lifespan_sets_it(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Only the role is under test. The lifespan's other startup calls
        # change the process for every test after this one, and stay out:
        # setup_logging() configures structlog with cached loggers, after
        # which structlog's capture_logs() sees nothing in any later test
        # (it broke tests/test_transport_consumer.py on the stand, one
        # process for the whole suite); the profile is the stub's already,
        # and its install wants a directory this suite does not set.
        monkeypatch.setattr(api_entry, "setup_logging", lambda: None)
        monkeypatch.setattr(api_entry, "install_profile_from_settings", lambda: None)
        await dispose_engine()
        set_process_role(ProcessRole.WORKER)
        async with api_entry.lifespan(api_entry.app):
            assert database._role is ProcessRole.API
        set_process_role(ProcessRole.WORKER)
