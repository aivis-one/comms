# =============================================================================
# COMMS Service -- dlq-scrub: the dead letters already kept, brought to
# today's rule (ST-1)
# =============================================================================
#
# app/transport/dlq_scrub.py on a real stream (fakeredis), every form of a
# dead letter an older comms wrote (the module header names them):
#
#   1  a0 and a1 -- written before G4, the entry verbatim and an
#      exception's text for a reason: a sentinel in the reason, in `data`
#      and in an extra field is nowhere after --apply; the reason is
#      SCRUBBED_REASON; the attempts and the source entry id are kept
#      under their names; a0's own three fields are not "dropped" names;
#      event and data are dead_letter_fields' (TestTheOldForms);
#   2  current -- an address-book entry with contacts is narrowed (the
#      fingerprint and the reason stay); an entry by the rule, one
#      without `_dlq_data` (an unknown event) and one of no known form
#      are left exactly as they were (TestTheCurrentForm);
#   3  one write, nothing lost: ids, order and number kept; the check
#      changes nothing; a second --apply has nothing to do; a dead letter
#      written meanwhile restarts the pass and is kept; a stream that
#      keeps changing is left as it was; a stream with a consumer group
#      is refused, untouched, the group named; no stream -- nothing to do
#      (TestTheWrite);
#   4  the verb and the contract (TestTheVerb).
#
# Mutations (built once, by hand):
#   M8   the rewrite with XADD "*"                 -> TestTheWrite (ids)
#   M9   the rewrite without WATCH                 -> TestTheWrite (meanwhile)
#   M11  the old reason carried into _dlq_error    -> TestTheOldForms
# =============================================================================

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fakeredis import aioredis as fakeaioredis

from app.engine.service import stream_fingerprint
from app.transport import dlq_scrub
from app.transport.consumer import DLQ_FIELDS
from app.transport.dlq_scrub import (
    EXIT_CANNOT,
    EXIT_DONE,
    EXIT_REFUSED,
    EXIT_TO_DO,
    SCRUBBED_REASON,
    run,
)

_REPO = Path(__file__).resolve().parents[1]
_DOC = (_REPO / "deploy" / "INTEGRATION.md").read_text(encoding="utf-8")
_SCRIPT = (_REPO / "deploy" / "comms-deploy.sh").read_text(encoding="utf-8")
_KEY = "comms:events:dlq"
_S = "sentinel" + uuid4().hex


@pytest.fixture
def redis() -> fakeaioredis.FakeRedis:
    return fakeaioredis.FakeRedis()


def _upsert_data() -> bytes:
    return json.dumps({
        "v": 1, "recipient_id": str(uuid4()), "version": 3,
        "telegram_id": 42, "email": f"{_S}@example.test", "locale": f"{_S}",
        "timezone": "Europe/Berlin", "active": True,
    }).encode()


def _request_data() -> bytes:
    return json.dumps({
        "v": 1, "idempotency_key": "k-1", "type": "t",
        "target_type": "all", "target_value": "*",
        "title": f"title {_S}", "body": f"body {_S}",
        "llm_audio": {"url": f"http://x/?sig={_S}", "format": "wav"},
    }).encode()


def _a0(data: bytes) -> dict[str, bytes]:
    return {
        "event": b"user_upserted", "data": data, "note": _S.encode(),
        "error": f"ValueError: bad value {_S}".encode(),
        "attempts": b"3", "source_entry_id": b"9-0",
    }


def _a1(data: bytes) -> dict[str, bytes]:
    return {
        "event": b"notification_request", "data": data, "note": _S.encode(),
        "_dlq_error": f"INSERT ... VALUES ('{_S}')".encode(),
        "_dlq_attempts": b"6", "_dlq_source_entry_id": b"10-0",
    }


def _g4_upsert(data: bytes) -> dict[str, str]:
    """An address-book dead letter as G4 wrote it: every field kept."""
    return {
        "event": "user_upserted",
        "_dlq_data_fingerprint": stream_fingerprint(data),
        "_dlq_data_bytes": str(len(data)),
        "_dlq_data": json.dumps(json.loads(data), sort_keys=True),
        "_dlq_dropped": json.dumps(["note"]),
        "_dlq_error": (
            "user_upserted: field 'locale' is a string the database cannot store"
        ),
        "_dlq_attempts": "0",
        "_dlq_source_entry_id": "11-0",
    }


async def _entries(redis: fakeaioredis.FakeRedis) -> list[tuple[str, dict[str, str]]]:
    return [
        (i.decode(), {k.decode(): v.decode() for k, v in f.items()})
        for i, f in await redis.xrange(_KEY)
    ]


async def _seed(redis: fakeaioredis.FakeRedis, *entries: dict[str, Any]) -> list[str]:
    ids = []
    for number, fields in enumerate(entries, start=1):
        ids.append(f"{1000 + number}-{number}")
        await redis.xadd(_KEY, fields, id=ids[-1])
    return ids


def _assert_no_sentinel(fields: dict[str, str]) -> None:
    for name, value in fields.items():
        assert _S not in name
        assert _S not in value, name


async def _apply(redis: fakeaioredis.FakeRedis) -> int:
    return await run(redis, _KEY, apply=True, script="comms-deploy.sh")


class TestTheOldForms:
    async def test_a0(self, redis: fakeaioredis.FakeRedis) -> None:
        data = _upsert_data()
        await _seed(redis, _a0(data))
        assert await _apply(redis) == EXIT_DONE
        ((_, dead),) = await _entries(redis)
        _assert_no_sentinel(dead)
        assert set(dead) <= set(DLQ_FIELDS)
        assert dead["_dlq_error"] == SCRUBBED_REASON
        assert (dead["_dlq_attempts"], dead["_dlq_source_entry_id"]) == ("3", "9-0")
        dropped = json.loads(dead["_dlq_dropped"])
        assert not {"error", "attempts", "source_entry_id"} & set(dropped)
        assert "note" in dropped and "data.email" in dropped
        # The pair: the address the product finds the event by.
        assert dead["event"] == "user_upserted"
        assert set(json.loads(dead["_dlq_data"])) == {"v", "recipient_id", "version"}
        assert dead["_dlq_data_fingerprint"] == stream_fingerprint(data)

    async def test_a1(self, redis: fakeaioredis.FakeRedis) -> None:
        data = _request_data()
        await _seed(redis, _a1(data))
        assert await _apply(redis) == EXIT_DONE
        ((_, dead),) = await _entries(redis)
        _assert_no_sentinel(dead)
        assert dead["_dlq_error"] == SCRUBBED_REASON
        assert (dead["_dlq_attempts"], dead["_dlq_source_entry_id"]) == ("6", "10-0")
        kept = json.loads(dead["_dlq_data"])
        assert kept["idempotency_key"] == "k-1"
        assert "llm_audio" not in kept and "title" not in kept
        assert dead["_dlq_data_fingerprint"] == stream_fingerprint(data)
        assert dead["_dlq_data_bytes"] == str(len(data))


class TestTheCurrentForm:
    async def test_contacts_are_narrowed_away(
        self, redis: fakeaioredis.FakeRedis,
    ) -> None:
        data = _upsert_data()
        before = _g4_upsert(data)
        await _seed(redis, before)
        assert await _apply(redis) == EXIT_DONE
        ((_, dead),) = await _entries(redis)
        _assert_no_sentinel(dead)
        assert json.loads(dead["_dlq_data"]) == {
            k: v for k, v in json.loads(data).items()
            if k in ("v", "recipient_id", "version")
        }
        assert json.loads(dead["_dlq_dropped"]) == [
            "data.active", "data.email", "data.locale", "data.telegram_id",
            "data.timezone", "note",
        ]
        for name in ("_dlq_data_fingerprint", "_dlq_data_bytes", "_dlq_error",
                     "_dlq_attempts", "_dlq_source_entry_id", "event"):
            assert dead[name] == before[name], name

    @pytest.mark.parametrize("fields", [
        {"event": "user_deleted",
         "_dlq_data": '{"recipient_id": "r", "v": 1, "version": 2}',
         "_dlq_dropped": "[]", "_dlq_error": "e", "_dlq_attempts": "0",
         "_dlq_source_entry_id": "1-0"},
        # G4 writes no _dlq_data for an unknown event; its reason is
        # comms' own and stays (an a1 by "_dlq_error without _dlq_data"
        # would have lost it).
        {"event": "user_renamed", "_dlq_dropped": '["data"]',
         "_dlq_error": "unknown event", "_dlq_attempts": "0",
         "_dlq_source_entry_id": "2-0", "_dlq_data_fingerprint": "f",
         "_dlq_data_bytes": "2"},
        # Of no known form: left as it is.
        {"something": _S},
        {"error": _S, "attempts": "x", "source_entry_id": "1-0"},
    ], ids=["by_the_rule", "no_data_unknown_event", "other",
            "a0_attempts_not_a_number"])
    async def test_left_as_it_was(
        self, redis: fakeaioredis.FakeRedis, fields: dict[str, str],
    ) -> None:
        await _seed(redis, fields)
        assert await _apply(redis) == EXIT_DONE
        assert await _entries(redis) == [("1001-1", fields)]


class TestTheWrite:
    async def test_ids_order_and_number(self, redis: fakeaioredis.FakeRedis) -> None:
        data = _upsert_data()
        mixed = [
            _a0(data), {"something": "else"}, _g4_upsert(data),
            _a1(_request_data()),
        ]
        ids = await _seed(redis, *mixed)
        assert await _apply(redis) == EXIT_DONE
        after = await _entries(redis)
        assert [i for i, _ in after] == ids
        assert after[1][1] == {"something": "else"}
        for _, fields in after:
            _assert_no_sentinel(fields)
        # The consumer's next dead letter still goes after them.
        new = (await redis.xadd(_KEY, {"event": "x"})).decode()
        assert [int(p) for p in new.split("-")] > [int(p) for p in ids[-1].split("-")]

    async def test_the_check_changes_nothing(
        self, redis: fakeaioredis.FakeRedis, capsys: pytest.CaptureFixture[str],
    ) -> None:
        await _seed(redis, _a0(_upsert_data()))
        before = await _entries(redis)
        assert await run(redis, _KEY, apply=False, script="S") == EXIT_TO_DO
        assert await _entries(redis) == before
        out = capsys.readouterr().out
        assert "to bring to the rule: 1 (a0, user_upserted)" in out
        assert "Next: S dlq-scrub --apply" in out
        assert _S not in out

    async def test_a_second_apply_has_nothing_to_do(
        self, redis: fakeaioredis.FakeRedis,
    ) -> None:
        await _seed(redis, _a0(_upsert_data()), _g4_upsert(_upsert_data()))
        assert await _apply(redis) == EXIT_DONE
        once = await _entries(redis)
        assert await run(redis, _KEY, apply=False, script="S") == EXIT_DONE
        assert await _apply(redis) == EXIT_DONE
        assert await _entries(redis) == once

    async def test_no_stream(self, redis: fakeaioredis.FakeRedis) -> None:
        assert await _apply(redis) == EXIT_DONE
        assert not await redis.exists(_KEY)

    def _meanwhile(
        self, redis: fakeaioredis.FakeRedis, monkeypatch: pytest.MonkeyPatch,
        times: int,
    ) -> list[int]:
        """A dead letter the consumer writes between the read and the
        EXEC, on the first `times` passes."""
        made = [0]
        real_pipeline = redis.pipeline

        def pipeline(*args: Any, **kwargs: Any) -> Any:
            pipe = real_pipeline(*args, **kwargs)
            real_xrange = pipe.xrange

            async def xrange(*a: Any, **k: Any) -> Any:
                read = await real_xrange(*a, **k)
                if made[0] < times:
                    made[0] += 1
                    await redis.xadd(_KEY, {"meanwhile": str(made[0])})
                return read

            pipe.xrange = xrange
            return pipe

        monkeypatch.setattr(redis, "pipeline", pipeline)
        return made

    async def test_a_dead_letter_written_meanwhile_is_kept(
        self, redis: fakeaioredis.FakeRedis, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        await _seed(redis, _a0(_upsert_data()))
        made = self._meanwhile(redis, monkeypatch, times=1)
        assert await _apply(redis) == EXIT_DONE
        assert made == [1]
        after = await _entries(redis)
        assert len(after) == 2
        assert after[1][1] == {"meanwhile": "1"}
        assert after[0][1]["_dlq_error"] == SCRUBBED_REASON

    async def test_a_stream_that_keeps_changing_is_left(
        self, redis: fakeaioredis.FakeRedis, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        await _seed(redis, _a0(_upsert_data()))
        self._meanwhile(redis, monkeypatch, times=100)
        assert await _apply(redis) == EXIT_CANNOT
        first = (await _entries(redis))[0][1]
        assert "error" in first and "_dlq_error" not in first  # untouched
        assert "kept changing" in capsys.readouterr().out

    async def test_a_stream_with_a_group_is_refused(
        self, redis: fakeaioredis.FakeRedis, capsys: pytest.CaptureFixture[str],
    ) -> None:
        await _seed(redis, _a0(_upsert_data()))
        await redis.xgroup_create(_KEY, "someones-reader", id="0")
        before = await _entries(redis)
        assert await _apply(redis) == EXIT_REFUSED
        assert await _entries(redis) == before
        out = capsys.readouterr().out
        assert out.startswith("✗") and "someones-reader" in out


class TestTheVerb:
    def test_the_verb_runs_this_module(self) -> None:
        assert "    dlq-scrub) shift; cmd_dlq_scrub \"$@\" ;;" in _SCRIPT
        assert "-m app.transport.dlq_scrub" in _SCRIPT
        assert f"[ \"$rc\" -ne {EXIT_TO_DO} ]" in _SCRIPT

    def test_the_contract(self) -> None:
        section = " ".join(_DOC[_DOC.index("## How long comms keeps things"):].split())
        assert f"it reads `{SCRUBBED_REASON}`" in section
        for code, word in ((EXIT_DONE, "done"), (EXIT_REFUSED, "refused"),
                           (EXIT_CANNOT, "cannot"), (EXIT_TO_DO, "the check found")):
            assert f"**{code}** = {word}" in section
        assert "the same ids, in the same order, as many entries" in section

    def test_the_main_reads_the_settings(
        self, redis: fakeaioredis.FakeRedis, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The entry point: the stream is settings.dlq_stream, the client
        comms' own (both replaced here -- no global effect is left)."""
        import app.transport.connection as connection
        from app.core.config import settings

        seen: list[str] = []

        async def fake_run(client: Any, key: str, **kwargs: Any) -> int:
            seen.append(key)
            return EXIT_DONE

        monkeypatch.setattr(connection, "redis_client", lambda: redis)
        monkeypatch.setattr(dlq_scrub, "run", fake_run)
        assert dlq_scrub.main([]) == EXIT_DONE
        assert seen == [settings.dlq_stream]
