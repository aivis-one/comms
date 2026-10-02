# =============================================================================
# D1 / R7 -- lifespan is held by a test; no private name crosses modules.
# =============================================================================
#
# MUTATIONS:
#   M20 install_profile_from_settings() removed from lifespan
#       -> test_startup_installs_the_profile
#   M21 a private name imported between modules again
#       -> test_no_private_name_is_imported_between_app_modules
# =============================================================================

import ast
from pathlib import Path

import pytest

from app import main
from app.core.config import settings
from app.core.exceptions import ProfileError
from app.profile.registry import registry
from tests.conftest import FIXTURE_PROFILE_DIR
from tests.helpers import configure_every_channel

_APP = Path(__file__).resolve().parents[1] / "app"


@pytest.fixture
def spies(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record setup_logging and the shutdown calls instead of running
    them: reconfiguring structlog would outlive the test (and blind
    capture_logs), and the engine is the suite's."""
    called: list[str] = []

    def setup_logging() -> None:
        called.append("setup_logging")

    async def close_formatters() -> None:
        called.append("close_formatters")

    async def dispose_engine() -> None:
        called.append("dispose_engine")

    monkeypatch.setattr(main, "setup_logging", setup_logging)
    monkeypatch.setattr(main, "close_formatters", close_formatters)
    monkeypatch.setattr(main, "dispose_engine", dispose_engine)
    return called


class TestLifespan:
    async def test_startup_installs_the_profile(
        self, monkeypatch: pytest.MonkeyPatch, spies: list[str],
    ) -> None:
        """done-when (1). M20. The pair: the registry was EMPTY before
        startup, so what is there afterwards is startup's work."""
        registry.reset()
        assert registry.registered_types() == frozenset()
        configure_every_channel(monkeypatch)
        monkeypatch.setattr(settings, "templates_dir", str(FIXTURE_PROFILE_DIR))
        async with main.lifespan(main.app):
            assert "unit_event_in_app" in registry.registered_types()
            assert spies == ["setup_logging"]
        assert spies == ["setup_logging", "close_formatters", "dispose_engine"]

    async def test_a_missing_profile_outside_development_refuses_to_start(
        self, monkeypatch: pytest.MonkeyPatch, spies: list[str],
    ) -> None:
        """The state the grid names: no profile on a deploy is a red
        start, before any traffic -- and nothing is shut down that was
        never started."""
        monkeypatch.setattr(settings, "templates_dir", "")
        monkeypatch.setattr(settings, "app_env", "production")
        with pytest.raises(ProfileError, match="TEMPLATES_DIR"):
            async with main.lifespan(main.app):
                pass  # pragma: no cover -- startup must not get here
        assert spies == ["setup_logging"]

    async def test_an_empty_token_is_said_out_loud(
        self, monkeypatch: pytest.MonkeyPatch, spies: list[str],
    ) -> None:
        from structlog.testing import capture_logs

        configure_every_channel(monkeypatch)
        monkeypatch.setattr(settings, "templates_dir", str(FIXTURE_PROFILE_DIR))
        monkeypatch.setattr(settings, "comms_service_token", "")
        with capture_logs() as logs:
            async with main.lifespan(main.app):
                pass
        assert [log for log in logs if log["event"] == "service_auth_disabled"]


def _private_imports(source: str, name: str) -> list[str]:
    """`from app.<module> import _name` -- a private name crossing a
    module boundary inside app/."""
    tree = ast.parse(source, name)
    return [
        f"{name}:{node.lineno}: {node.module}.{alias.name}"
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module is not None
        and node.module.startswith("app")
        for alias in node.names
        if alias.name.startswith("_") and not alias.name.startswith("__")
    ]


def test_no_private_name_is_imported_between_app_modules() -> None:
    """done-when (3). The pair: the scan walked the tree."""
    files = sorted(_APP.rglob("*.py"))
    assert len(files) > 40
    hits = [
        hit for path in files
        for hit in _private_imports(path.read_text(encoding="utf-8"), str(path))
    ]
    assert hits == []


@pytest.mark.parametrize(
    "planted",
    [
        "from app.messaging.threads import _x\n",
        "from app.messaging.threads import (\n    public,\n    _x,\n)\n",
        "from app.messaging.threads import _x as x\n",
    ],
)
def test_the_scan_sees_every_form(planted: str) -> None:
    """The same gate on the tool: one line, a parenthesised list, an
    alias -- the forms a grep would miss."""
    assert _private_imports(planted, "planted.py")
