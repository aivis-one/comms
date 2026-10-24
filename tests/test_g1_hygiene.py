# =============================================================================
# G1 -- hygiene of app/: items 2-9 (item 1: tests/test_g1_forgetting_in_
# flight.py)
# =============================================================================
#
#   2 the letter is read through letter_text(): an AST watchman over app/
#   3 notification_deliveries.channel_options is gone (migration 0032)
#   4 a base profile's in_app sheet refuses the start
#   5 get_db_reader's transaction is READ ONLY
#   6 the email and llm HTTP clients take nothing from the environment
#   7 template_render_error logs the class, never the text
#   8 a query key given twice is refused
#   9 sections and their rosters run the insert race's one rule
#
# MUTATIONS (one run, every one red here):
#   M4  a new reader of notification.title    -> test_every_reader_is_listed
#   M5  the watchman without getattr          -> test_the_scanner_sees_every_
#                                                form
#   M6  the formatter reads channel_options    -> test_nothing_in_app_names_
#       again (through getattr)                   the_column
#   M7  in_app back in TEMPLATE_CHANNELS, its  -> TestInAppSheetInTheBase
#       refusal removed
#   M8  SET TRANSACTION READ ONLY removed      -> test_a_write_is_refused
#   M9  trust_env removed from either client   -> TestNoProxyFromThe
#                                                Environment
#   M10 error=str(exc) back                    -> test_the_letter_stays_out_
#                                                of_the_log
#   M11 the repeated-query dependency removed  -> TestRepeatedQueryKey
#   M12 sections / membership back to a plain  -> TestTheInsertRace
#       insert (no race handling)
# =============================================================================

import ast
import asyncio
import re
import shutil
from pathlib import Path
from typing import Any

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from structlog.testing import capture_logs

from app.core.database import get_db_reader, get_db_session, get_session_factory
from app.core.exceptions import ProfileError
from app.engine import formatters
from app.engine.models import NotificationDelivery
from app.engine.template_engine import render
from app.messaging.membership import set_membership
from app.messaging.models import Section, SectionMember
from app.messaging.sections import get_or_create_section
from app.profile.loader import (
    TEMPLATE_CHANNELS,
    FileProfileSource,
    _unrendered_complaint,
    load_profile,
)
from app.profile.registry import registry
from tests.conftest import FIXTURE_PROFILE_DIR
from tests.helpers import create_recipient

_REPO = Path(__file__).resolve().parents[1]
_APP = _REPO / "app"
_BOUND = 10.0


# ---------------------------------------------------------------------------
# Item 2 -- the letter is read through letter_text()
# ---------------------------------------------------------------------------

_LETTER_FIELDS = frozenset({"title", "body", "target_type", "target_value"})
_SQL_FORM = re.compile(r"\bnotifications\.(title|body|target_type|target_value)\b")

_ADDRESS = "the job's address (target), the envelope -- comms reads it"
_NOT_A_JOB = "not a job: "

# Every place in app/ that reads one of the four names, and why it may.
# (file, function, name) -> reason. The scanner cannot know the type of
# what is read, so the key is the place; a place no longer in the code
# is an error too (test_every_reader_is_listed, both ways).
ALLOWED: dict[tuple[str, str, str], str] = {
    ("app/engine/models.py", "Notification.letter_text", "title"):
        "THE one way a person's channel reads the letter",
    ("app/engine/models.py", "Notification.letter_text", "body"):
        "THE one way a person's channel reads the letter",
    ("app/engine/service.py", "list_recipient_deliveries", "title"):
        "the bell feed returns the stored letter to its own recipient "
        "(review 3.5.0: leave as it is)",
    ("app/engine/service.py", "list_recipient_deliveries", "body"):
        "the bell feed returns the stored letter to its own recipient "
        "(review 3.5.0: leave as it is)",
    ("app/engine/models.py", "Notification.__repr__", "target_type"): _ADDRESS,
    ("app/engine/models.py", "Notification.__repr__", "target_value"): _ADDRESS,
    ("app/engine/service.py", "resolve_notification", "target_type"): _ADDRESS,
    ("app/engine/service.py", "resolve_notification", "target_value"): _ADDRESS,
    ("app/engine/reminders.py", "cancel_reminders", "target_type"): _ADDRESS,
    ("app/engine/reminders.py", "cancel_reminders", "target_value"): _ADDRESS,
    ("app/api/jobs.py", "_job", "target_type"):
        _ADDRESS + "; echoed to the product that sent it",
    ("app/api/jobs.py", "_job", "target_value"):
        _ADDRESS + "; echoed to the product that sent it",
    ("app/transport/handlers.py", "_handle_notification_request", "title"):
        _NOT_A_JOB + "the product's event, written into a new job",
    ("app/transport/handlers.py", "_handle_notification_request", "body"):
        _NOT_A_JOB + "the product's event, written into a new job",
    ("app/transport/handlers.py", "_handle_notification_request", "target_type"):
        _NOT_A_JOB + "the product's event, written into a new job",
    ("app/transport/handlers.py", "_handle_notification_request", "target_value"):
        _NOT_A_JOB + "the product's event, written into a new job",
    ("app/transport/handlers.py", "handle_event", "target_type"):
        _NOT_A_JOB + "the product's event, its address in the log",
    ("app/transport/handlers.py", "handle_event", "target_value"):
        _NOT_A_JOB + "the product's event, its address in the log",
    ("app/api/idempotency.py", "request_fingerprint", "body"):
        _NOT_A_JOB + "Request.body(), the request digested whole",
    ("app/api/jobs.py", "_answer", "body"):
        _NOT_A_JOB + "LLMAnswer.body, the model's answer for the product",
    ("app/engine/service.py", "_answer_row", "body"):
        _NOT_A_JOB + "LLMReply.body, the model's answer being stored",
    ("app/engine/service.py", "delete_expired_notifications_batch", "body"):
        _NOT_A_JOB + "LLMAnswer.body in a WHERE",
    ("app/engine/service.py", "purge_llm_answers", "body"):
        _NOT_A_JOB + "LLMAnswer.body in a WHERE",
    ("app/api/messaging.py", "_message_out", "body"):
        _NOT_A_JOB + "a thread's message",
    ("app/api/messaging.py", "post_thread_message", "body"):
        _NOT_A_JOB + "a thread's message being posted",
    ("app/api/messaging.py", "_thread_out", "title"):
        _NOT_A_JOB + "a thread's title",
    ("app/api/messaging.py", "create_thread", "title"):
        _NOT_A_JOB + "a thread's title being created",
    ("app/api/messaging.py", "VariantIn._one_form", "body"):
        _NOT_A_JOB + "a message variant's form, checked (P5-4)",
    ("app/api/messaging.py", "_variant_out", "body"):
        _NOT_A_JOB + "a message variant, returned to the product (P5-4)",
    ("app/api/messaging.py", "post_variant", "body"):
        _NOT_A_JOB + "a message variant being written (P5-4)",
    ("app/variants.py", "decide_state", "body"):
        _NOT_A_JOB + "whether a message variant has text (P5-4)",
}


def _docstrings(tree: ast.AST) -> set[int]:
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            first = node.body[0] if node.body else None
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                found.add(id(first.value))
    return found


def scan_readers(source: str, path: str) -> set[tuple[str, str, str]]:
    """Every read of a letter field in `source`, as (path, function,
    name). The forms: an attribute read (x.title, Notification.title),
    getattr / attrgetter with the name, and the column in SQL text.
    A store, a keyword argument and a docstring are not reads."""
    tree = ast.parse(source)
    docs = _docstrings(tree)
    found: set[tuple[str, str, str]] = set()

    def visit(node: ast.AST, scope: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = scope
            if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
            ):
                inner = child.name if scope == "<module>" else f"{scope}.{child.name}"
            if (
                isinstance(child, ast.Attribute)
                and isinstance(child.ctx, ast.Load)
                and child.attr in _LETTER_FIELDS
            ):
                found.add((path, inner, child.attr))
            if isinstance(child, ast.Call):
                func = child.func
                name = (
                    func.attr if isinstance(func, ast.Attribute)
                    else getattr(func, "id", None)
                )
                if name in ("getattr", "attrgetter"):
                    for arg in child.args:
                        if (
                            isinstance(arg, ast.Constant)
                            and arg.value in _LETTER_FIELDS
                        ):
                            found.add((path, inner, arg.value))
            if (
                isinstance(child, ast.Constant)
                and isinstance(child.value, str)
                and id(child) not in docs
            ):
                for match in _SQL_FORM.finditer(child.value):
                    found.add((path, inner, match.group(1)))
            visit(child, inner)

    visit(tree, "<module>")
    return found


class TestTheLetterWatchman:
    def test_every_reader_is_listed(self) -> None:
        """Both ways: an unlisted reader is red, and so is a listed place
        that no longer reads. The pair: the walk finds letter_text."""
        found: set[tuple[str, str, str]] = set()
        for path in sorted(_APP.rglob("*.py")):
            found |= scan_readers(
                path.read_text(encoding="utf-8"),
                str(path.relative_to(_REPO)),
            )
        assert ("app/engine/models.py", "Notification.letter_text", "title") in found
        unlisted = sorted(found - set(ALLOWED))
        stale = sorted(set(ALLOWED) - found)
        assert unlisted == [], f"a letter field read outside letter_text: {unlisted}"
        assert stale == [], f"listed, no longer read there: {stale}"
        assert all(reason.strip() for reason in ALLOWED.values())

    def test_the_scanner_sees_every_form(self) -> None:
        """The instrument's own twin: each form is seen, and what is not
        a read is not."""
        source = '''
"""Module notes on notifications.title -- a docstring, not a read."""
from operator import attrgetter

def reader(job):
    """notifications.body in a docstring is no read either."""
    a = job.title
    b = getattr(job, "body")
    c = attrgetter("target_value")(job)
    d = text("SELECT notifications.target_type FROM notifications")
    job.title = "stored"
    make(title="keyword")
    return a, b, c, d

class Box:
    def peek(self):
        return Notification.body
'''
        assert scan_readers(source, "m.py") == {
            ("m.py", "reader", "title"),
            ("m.py", "reader", "body"),
            ("m.py", "reader", "target_value"),
            ("m.py", "reader", "target_type"),
            ("m.py", "Box.peek", "body"),
        }


# ---------------------------------------------------------------------------
# Item 3 -- channel_options is gone
# ---------------------------------------------------------------------------


class TestTheDeadColumnIsGone:
    async def test_not_in_the_schema(self) -> None:
        """The pair: the table and its `channel` column are there."""
        async with get_session_factory()() as session:
            columns = set((await session.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'notification_deliveries'"
            ))).scalars())
            checks = set((await session.execute(text(
                "SELECT conname FROM pg_constraint WHERE conname LIKE "
                "'%channel\\_options%'"
            ))).scalars())
        assert "channel" in columns
        assert "channel_options" not in columns
        assert checks == set()
        assert not hasattr(NotificationDelivery, "channel_options")

    def test_nothing_in_app_names_the_column(self) -> None:
        """Every form: an attribute, a name, a string (getattr, SQL). The
        pair: the same walk finds `channel`, which every delivery has."""
        named: list[str] = []
        channel_seen = False
        for path in sorted(_APP.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                values = []
                if isinstance(node, ast.Attribute):
                    values.append(node.attr)
                elif isinstance(node, ast.Name):
                    values.append(node.id)
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    values.append(node.value)
                for value in values:
                    if "channel_options" in value:
                        named.append(f"{path.relative_to(_REPO)}:{node.lineno}")
                    if value == "channel":
                        channel_seen = True
        assert channel_seen
        assert named == []


# ---------------------------------------------------------------------------
# Item 4 -- an in_app sheet in the base refuses the start
# ---------------------------------------------------------------------------


def _base(tmp_path: Path, en: str) -> Path:
    root = tmp_path / "profile"
    (root / "templates").mkdir(parents=True)
    shutil.copy(FIXTURE_PROFILE_DIR / "types.yaml", root / "types.yaml")
    (root / "templates" / "en.yaml").write_text(en, encoding="utf-8")
    return root


class TestInAppSheetInTheBase:
    @pytest.mark.parametrize(
        "sheet", ['{title: "{title}"}', "{}"], ids=["texts", "empty"],
    )
    def test_refused_by_name(self, tmp_path: Path, sheet: str) -> None:
        root = _base(tmp_path, f"unit_event:\n  in_app: {sheet}\n")
        with pytest.raises(ProfileError) as caught:
            load_profile(FileProfileSource(root))
        assert _unrendered_complaint("unit_event.in_app") in str(caught.value)

    def test_every_problem_at_once(self, tmp_path: Path) -> None:
        root = _base(tmp_path, (
            "unit_event:\n  in_app: {title: x}\n"
            "unit_presented:\n  in_app: {body: y}\n  telegramm: {body: z}\n"
        ))
        with pytest.raises(ProfileError) as caught:
            load_profile(FileProfileSource(root))
        message = str(caught.value)
        assert "(3 problems)" in message
        assert _unrendered_complaint("unit_presented.in_app") in message
        assert "channel key 'telegramm'" in message

    def test_the_channels_that_render_still_load(self, tmp_path: Path) -> None:
        """The pair: telegram and email sheets load, and in_app is out of
        the set a sheet may name."""
        root = _base(tmp_path, (
            "unit_event:\n  telegram: {title: x}\n  email: {subject: y}\n"
        ))
        assert load_profile(FileProfileSource(root)).templates["en"]
        assert "in_app" not in TEMPLATE_CHANNELS
        assert set(TEMPLATE_CHANNELS) == {"telegram", "email"}


# ---------------------------------------------------------------------------
# Item 5 -- the reader's transaction is READ ONLY
# ---------------------------------------------------------------------------


class TestTheReaderReadsOnly:
    async def test_a_write_is_refused(self) -> None:
        """SQLSTATE 25006 for a write inside the reader; the pair: a
        read works and the transaction says read only."""
        reader = get_db_reader()
        session = await reader.__anext__()
        try:
            assert (await session.execute(text("SELECT 1"))).scalar_one() == 1
            assert (await session.execute(
                text("SHOW transaction_read_only"),
            )).scalar_one() == "on"
            with pytest.raises(DBAPIError) as caught:
                await session.execute(text(
                    "UPDATE recipients SET locale = locale WHERE false"
                ))
            assert getattr(caught.value.orig, "pgcode", None) == "25006"
        finally:
            await reader.aclose()

    async def test_the_writer_is_not_read_only(self) -> None:
        """The twin: the read-write session is untouched."""
        writer = get_db_session()
        session = await writer.__anext__()
        try:
            assert (await session.execute(
                text("SHOW transaction_read_only"),
            )).scalar_one() == "off"
        finally:
            await writer.aclose()


# ---------------------------------------------------------------------------
# Item 6 -- no proxy from the environment
# ---------------------------------------------------------------------------


class TestNoProxyFromTheEnvironment:
    @pytest.mark.parametrize(
        "factory",
        ["_real_http_client_factory", "_real_llm_http_client_factory"],
    )
    async def test_the_client_ignores_proxy_variables(
        self, monkeypatch: pytest.MonkeyPatch, factory: str,
    ) -> None:
        """The pair: under the same environment a client that trusts it
        does mount the proxy -- the variables are live."""
        monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:3128")
        monkeypatch.setenv("ALL_PROXY", "http://proxy.invalid:3128")
        client = getattr(formatters, factory)()
        trusting = httpx.AsyncClient(trust_env=True)
        try:
            assert client._trust_env is False
            assert dict(client._mounts) == {}
            assert dict(trusting._mounts) != {}
        finally:
            await client.aclose()
            await trusting.aclose()


# ---------------------------------------------------------------------------
# Item 7 -- the render error logs the class, never the text
# ---------------------------------------------------------------------------

_SECRET_WORD = "kumquat-4471"


class TestTheRenderErrorLog:
    def test_the_letter_stays_out_of_the_log(self) -> None:
        """A nested spec taken from the payload puts the value into the
        exception's text ("Invalid format specifier '<value>' ..."). The
        log carries the class and the coordinates; the pair: the class is
        there and non-empty."""
        registry.register_templates(
            "en", {"unit_event": {"telegram": {"title": "{x:{w}}"}}},
        )
        with capture_logs() as logs:
            rendered = render(
                "unit_event", "telegram", "title", "en",
                {"x": "a", "w": _SECRET_WORD},
            )
        assert rendered is None
        (entry,) = [e for e in logs if e["event"] == "template_render_error"]
        assert entry["error_class"] == "ValueError"
        assert (entry["type"], entry["channel"], entry["field"]) == (
            "unit_event", "telegram", "title",
        )
        assert _SECRET_WORD not in repr(entry)


# ---------------------------------------------------------------------------
# Item 8 -- a query key given twice is refused
# ---------------------------------------------------------------------------


class TestRepeatedQueryKey:
    @pytest.mark.parametrize(
        ("url", "key"),
        [
            ("/api/v1/recipients?limit=5&limit=50", "limit"),
            ("/api/v1/recipients?cursor=a&cursor=b", "cursor"),
            ("/api/v1/channels/health?window_minutes=5&window_minutes=6",
             "window_minutes"),
            ("/api/v1/recipients?zzz=1&zzz=2", "zzz"),
        ],
    )
    async def test_refused(self, client: AsyncClient, url: str, key: str) -> None:
        response = await client.get(url)
        assert response.status_code == 422
        error = response.json()["error"]
        assert error["class"] == "validation"
        assert f"given more than once: {key}" in error["message"]

    @pytest.mark.parametrize(
        "url",
        ["/api/v1/recipients?limit=5", "/api/v1/recipients?limit=5&zzz=1"],
    )
    async def test_once_each_passes(self, client: AsyncClient, url: str) -> None:
        """The pair: one value per key is read as before."""
        response = await client.get(url)
        assert response.status_code == 200
        assert "items" in response.json()


# ---------------------------------------------------------------------------
# Item 9 -- the insert race's one rule in messaging
# ---------------------------------------------------------------------------


async def _until_waiting(pid: list[int]) -> None:
    """Until THE session with this backend pid waits on a lock -- not
    any session of the database."""
    for _ in range(int(_BOUND / 0.02)):
        if pid:
            async with get_session_factory()() as session:
                waiting = (await session.execute(text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE pid = :pid AND wait_event_type = 'Lock' "
                    "AND datname = current_database()"
                ), {"pid": pid[0]})).scalar_one()
            if waiting:
                return
        await asyncio.sleep(0.02)
    raise AssertionError("the second session never waited on a lock")


async def _pid(session: Any) -> int:
    pid: int = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
    return pid


class TestTheInsertRace:
    async def test_two_creators_of_one_section(self) -> None:
        """The first creator's insert is uncommitted; the second finds
        nothing, inserts, waits on the key, loses -- and gets the first
        one's row, logged as the race. One row in the end."""
        factory = get_session_factory()
        async with factory() as first:
            first.add(Section(key="sec-race", label="A"))
            await first.flush()

            pid: list[int] = []

            async def second() -> Any:
                async with factory() as session:
                    pid.append(await _pid(session))
                    section = await get_or_create_section(
                        session, key="sec-race", label="B",
                    )
                    await session.commit()
                    return section.id

            with capture_logs() as logs:
                task = asyncio.create_task(second())
                await _until_waiting(pid)
                await first.commit()
                won = await asyncio.wait_for(task, _BOUND)
        async with factory() as session:
            rows = (await session.execute(
                select(Section.id, Section.label).where(Section.key == "sec-race"),
            )).all()
        assert rows == [(won, "A")]
        assert [e["event"] for e in logs].count("section_create_lost_race") == 1

    async def test_two_identical_roster_events(self) -> None:
        factory = get_session_factory()
        async with factory() as session:
            operator = await create_recipient(session)
            section = await get_or_create_section(session, key="sec-m", label="M")
            await session.commit()
        async with factory() as first:
            first.add(SectionMember(section_id=section.id, operator_id=operator.id))
            await first.flush()

            pid: list[int] = []

            async def second() -> None:
                async with factory() as session:
                    pid.append(await _pid(session))
                    await set_membership(
                        session, section_key="sec-m", section_label="M",
                        operator_id=operator.id, member=True,
                    )
                    await session.commit()

            with capture_logs() as logs:
                task = asyncio.create_task(second())
                await _until_waiting(pid)
                await first.commit()
                await asyncio.wait_for(task, _BOUND)
        async with factory() as session:
            count = (await session.execute(
                select(func.count()).select_from(SectionMember).where(
                    SectionMember.section_id == section.id,
                ),
            )).scalar_one()
        assert count == 1
        events = [e["event"] for e in logs]
        assert events.count("section_member_add_raced") == 1
        assert "section_member_added" not in events

    async def test_another_integrity_error_is_not_a_race(self) -> None:
        """A NOT NULL violation is raised at once, with no race line:
        only a unique violation is the race."""
        factory = get_session_factory()
        async with factory() as session:
            with capture_logs() as logs, pytest.raises(IntegrityError) as caught:
                await get_or_create_section(
                    session, key="sec-null", label=None,  # type: ignore[arg-type]
                )
        assert getattr(caught.value.orig, "pgcode", None) == "23502"
        assert "section_create_lost_race" not in [e["event"] for e in logs]
