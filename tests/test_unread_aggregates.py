# =============================================================================
# COMMS Service -- unread state exposed as aggregates (T-51)
# =============================================================================
# Band 92140-92179. Three additive read contracts over the frozen
# unread semantics:
#
#   GET  /api/v1/participants/{pid}/unread-summary
#   POST /api/v1/threads/unread-counts
#   GET  /api/v1/threads?operator=&with_unread=true
#
# WHAT THESE TESTS ARE ACTUALLY GUARDING:
#
# 1. ONE SEMANTICS, THREE SHAPES. Every aggregate must agree with the
#    pre-existing GET /threads/{id}/unread-count on the same data. The
#    check is close to a tautology now that all four are built on the
#    single unread_condition -- which is the point: it is written to
#    catch the day someone expresses "unread" a second time.
#
# 2. TWO PREDICATES, NOT ONE. The per-thread endpoint does NOT check
#    participation and must keep returning a real NUMBER to a
#    non-participant. If a refactor ever folds participation into the
#    shared unread builder, that number becomes a silent zero and the
#    frozen contract breaks without changing shape. That is the single
#    most valuable assertion in this file.
#
# 3. ABSENCE MEANS "NOT A PARTICIPANT" -- in all three shapes, by two
#    different routes into the state: the UNCLAIMED SECTION THREAD an
#    ordinary operator sees in the pool (the production path -- the
#    product reads its list with is_supervisor=False), and the foreign
#    thread a supervisor sees (the exotic one).
# =============================================================================

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.messaging.constants import OperatorKind, ThreadKind
from app.messaging.operators import claim_thread
from app.messaging.read_state import mark_read
from app.messaging.threads import create_or_get_thread_detailed, post_message
from tests.helpers import create_recipient, create_section, next_t51_telegram_id

_T0 = datetime(2026, 8, 17, 10, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(minutes=5)
_T2 = _T0 + timedelta(minutes=10)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------
async def _recipient(session: AsyncSession) -> UUID:
    recipient = await create_recipient(
        session, telegram_id=next_t51_telegram_id()
    )
    return recipient.id


async def _dm_thread(
    session: AsyncSession, *, client: UUID, master: UUID
) -> UUID:
    """A DM thread: assignee is PRE-ASSIGNED to operator_value (D1(i)),
    so the master is a participant by two roles at once."""
    thread, _ = await create_or_get_thread_detailed(
        session,
        client=client,
        operator_kind=OperatorKind.USER,
        operator_value=master,
        kind=ThreadKind.DM,
    )
    return thread.id


async def _section_thread(
    session: AsyncSession, *, client: UUID, section: UUID
) -> UUID:
    """An UNCLAIMED section thread: assignee empty, operator_value is a
    section id. Visible to every operator, participant to none."""
    thread, _ = await create_or_get_thread_detailed(
        session,
        client=client,
        operator_kind=OperatorKind.SECTION,
        operator_value=section,
        kind=ThreadKind.TICKET,
        subject_type="topic",
        subject_id=f"s-{uuid4().hex[:8]}",
    )
    return thread.id


async def _summary(api: AsyncClient, participant: UUID) -> dict[str, Any]:
    resp = await api.get(
        f"/api/v1/participants/{participant}/unread-summary"
    )
    assert resp.status_code == 200, resp.text
    payload: dict[str, Any] = resp.json()
    return payload


async def _batch(
    api: AsyncClient, participant: UUID, thread_ids: list[UUID]
) -> dict[str, int]:
    resp = await api.post(
        "/api/v1/threads/unread-counts",
        json={
            "participant": str(participant),
            "thread_ids": [str(t) for t in thread_ids],
        },
    )
    assert resp.status_code == 200, resp.text
    counts: dict[str, int] = resp.json()["counts"]
    return counts


async def _per_thread(
    api: AsyncClient, thread_id: UUID, participant: UUID
) -> int:
    resp = await api.get(
        f"/api/v1/threads/{thread_id}/unread-count",
        params={"participant": str(participant)},
    )
    assert resp.status_code == 200, resp.text
    unread: int = resp.json()["unread"]
    return unread


async def _list(
    api: AsyncClient, operator: UUID, **params: Any
) -> list[dict[str, Any]]:
    resp = await api.get(
        "/api/v1/threads", params={"operator": str(operator), **params}
    )
    assert resp.status_code == 200, resp.text
    threads: list[dict[str, Any]] = resp.json()["threads"]
    return threads


# ---------------------------------------------------------------------------
# The consistency invariant -- one semantics behind all four shapes
# ---------------------------------------------------------------------------
class TestConsistencyWithPerThreadEndpoint:
    async def test_all_shapes_agree_on_the_same_data(
        self, client: AsyncClient
    ) -> None:
        """Summary, batch and with_unread report the same numbers as the
        pre-existing per-thread endpoint, on threads where the reader
        takes part."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_a = await _dm_thread(s, client=customer, master=master)
            other_customer = await _recipient(s)
            thread_b = await _dm_thread(
                s, client=other_customer, master=master
            )
            # two unread for the master in A, one in B
            await post_message(
                s, thread_id=thread_a, sender=customer, body="a1",
                created_at=_T0,
            )
            await post_message(
                s, thread_id=thread_a, sender=customer, body="a2",
                created_at=_T1,
            )
            await post_message(
                s, thread_id=thread_b, sender=other_customer, body="b1",
                created_at=_T0,
            )
            await s.commit()

        per_thread = {
            thread_a: await _per_thread(client, thread_a, master),
            thread_b: await _per_thread(client, thread_b, master),
        }
        assert per_thread == {thread_a: 2, thread_b: 1}

        batch = await _batch(client, master, [thread_a, thread_b])
        assert batch == {str(thread_a): 2, str(thread_b): 1}

        summary = await _summary(client, master)
        assert summary == {
            "has_unread": True,
            "threads_with_unread": 2,
            "unread_messages": 3,
        }

        rows = await _list(client, master, with_unread="true")
        assert {row["id"]: row["unread"] for row in rows} == {
            str(thread_a): 2,
            str(thread_b): 1,
        }

    async def test_pointer_moves_and_every_shape_follows(
        self, client: AsyncClient
    ) -> None:
        """Marking read is the ONLY thing that clears unread -- and it
        clears it in all four shapes at once."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="m",
                created_at=_T0,
            )
            await s.commit()

        assert (await _summary(client, master))["unread_messages"] == 1

        async with factory() as s:
            await mark_read(
                s, thread_id=thread_id, participant=master, last_read_at=_T2
            )
            await s.commit()

        assert await _per_thread(client, thread_id, master) == 0
        assert await _batch(client, master, [thread_id]) == {
            str(thread_id): 0
        }
        assert await _summary(client, master) == {
            "has_unread": False,
            "threads_with_unread": 0,
            "unread_messages": 0,
        }
        rows = await _list(client, master, with_unread="true")
        assert [row["unread"] for row in rows] == [0]


# ---------------------------------------------------------------------------
# Two predicates, not one -- the seam that breaks the frozen endpoint
# ---------------------------------------------------------------------------
class TestFrozenPerThreadEndpointKeepsItsContract:
    async def test_non_participant_still_gets_a_number_not_zero(
        self, client: AsyncClient
    ) -> None:
        """GET /threads/{id}/unread-count does NOT check participation.

        A stranger asking about a thread they have no role in gets the
        real count for that (thread, participant) pair. If participation
        ever gets folded into the shared unread builder, this returns 0
        and a frozen contract has changed behaviour without changing
        shape -- which is exactly the failure this test exists for.
        """
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            stranger = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="one",
                created_at=_T0,
            )
            await post_message(
                s, thread_id=thread_id, sender=master, body="two",
                created_at=_T1,
            )
            await s.commit()

        # both messages are "not sent by the stranger" -> 2, not 0
        assert await _per_thread(client, thread_id, stranger) == 2
        # ...while the aggregates, which DO check participation, omit it
        assert await _batch(client, stranger, [thread_id]) == {}
        assert (await _summary(client, stranger))["has_unread"] is False


# ---------------------------------------------------------------------------
# Role 3 -- the DM operator sees their own unread in all three contracts
# ---------------------------------------------------------------------------
class TestDmOperatorRole:
    async def test_dm_operator_sees_own_unread_in_all_three_contracts(
        self, client: AsyncClient
    ) -> None:
        """The observable fact, not the OR branch: a master's own unread
        shows up in the summary, the batch and the list.

        (The branch itself cannot be isolated: a user thread's assignee
        is pre-assigned to operator_value at creation and nothing in the
        code can part them, so a fixture that split them would document
        a state that does not exist. See the KNOWN CEILING on
        participation_clause.)
        """
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="hi",
                created_at=_T0,
            )
            await s.commit()

        assert await _summary(client, master) == {
            "has_unread": True,
            "threads_with_unread": 1,
            "unread_messages": 1,
        }
        assert await _batch(client, master, [thread_id]) == {
            str(thread_id): 1
        }
        rows = await _list(client, master, with_unread="true")
        assert [(row["id"], row["unread"]) for row in rows] == [
            (str(thread_id), 1)
        ]

    async def test_client_role_and_own_messages_excluded(
        self, client: AsyncClient
    ) -> None:
        """The client side of the same thread: their own message is not
        unread to them, the master's reply is."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="mine",
                created_at=_T0,
            )
            await post_message(
                s, thread_id=thread_id, sender=master, body="theirs",
                created_at=_T1,
            )
            await s.commit()

        assert (await _summary(client, customer))["unread_messages"] == 1
        assert await _batch(client, customer, [thread_id]) == {
            str(thread_id): 1
        }

    async def test_claimed_section_thread_counts_for_the_claimer(
        self, client: AsyncClient
    ) -> None:
        """Role 2: an agent who claimed a section thread takes part in
        it and gets its count everywhere."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            agent = await _recipient(s)
            section = await create_section(s, key=f"t51-{uuid4().hex[:8]}")
            thread_id = await _section_thread(
                s, client=customer, section=section.id
            )
            await claim_thread(s, thread_id=thread_id, operator=agent)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="help",
                created_at=_T0,
            )
            await s.commit()

        assert (await _summary(client, agent))["threads_with_unread"] == 1
        assert await _batch(client, agent, [thread_id]) == {
            str(thread_id): 1
        }


# ---------------------------------------------------------------------------
# Absence means "not a participant" -- BOTH routes into the state
# ---------------------------------------------------------------------------
class TestAbsenceMeansNotAParticipant:
    async def test_unclaimed_section_pool_row_has_no_unread_key(
        self, client: AsyncClient
    ) -> None:
        """THE PRODUCTION PATH, with is_supervisor=False.

        An unclaimed section thread is in every operator's pool
        (list_visible_threads), but nobody takes part in it: assignee is
        empty and operator_value is a SECTION id, not a recipient. The
        row therefore carries NO `unread` key -- a zero would be
        indistinguishable from a thread the operator has read.
        """
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            agent = await _recipient(s)
            section = await create_section(s, key=f"t51-{uuid4().hex[:8]}")
            pool_thread = await _section_thread(
                s, client=customer, section=section.id
            )
            await post_message(
                s, thread_id=pool_thread, sender=customer, body="queued",
                created_at=_T0,
            )
            await s.commit()

        rows = await _list(client, agent, with_unread="true")
        assert [row["id"] for row in rows] == [str(pool_thread)]
        assert "unread" not in rows[0]

        # and the same rule in the other two shapes
        assert await _batch(client, agent, [pool_thread]) == {}
        assert await _summary(client, agent) == {
            "has_unread": False,
            "threads_with_unread": 0,
            "unread_messages": 0,
        }

    async def test_supervisor_foreign_thread_has_no_unread_key(
        self, client: AsyncClient
    ) -> None:
        """The widening path: is_supervisor=true shows threads the
        operator has no role in. Same rule, second route."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            supervisor = await _recipient(s)
            foreign = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=foreign, sender=customer, body="private",
                created_at=_T0,
            )
            await s.commit()

        rows = await _list(
            client, supervisor, is_supervisor="true", with_unread="true"
        )
        assert [row["id"] for row in rows] == [str(foreign)]
        assert "unread" not in rows[0]

    async def test_unknown_thread_id_is_absent_not_an_error(
        self, client: AsyncClient
    ) -> None:
        """One rule, not two: an id that matches no thread is absent
        exactly like a non-participant thread -- and does not cost the
        caller the rest of the batch."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            mine = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=mine, sender=customer, body="x", created_at=_T0,
            )
            await s.commit()

        counts = await _batch(client, master, [mine, uuid4()])
        assert counts == {str(mine): 1}


# ---------------------------------------------------------------------------
# Opt-in: without the parameter, not one byte moves
# ---------------------------------------------------------------------------
class TestWithUnreadIsStrictlyOptIn:
    async def test_response_is_byte_identical_without_the_parameter(
        self, client: AsyncClient
    ) -> None:
        """The 3b shape is frozen: the default response must be
        unchanged, key for key, byte for byte."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="hi",
                created_at=_T0,
            )
            await s.commit()

        default = await client.get(
            "/api/v1/threads", params={"operator": str(master)}
        )
        explicit_off = await client.get(
            "/api/v1/threads",
            params={"operator": str(master), "with_unread": "false"},
        )
        assert default.status_code == 200
        assert default.text == explicit_off.text
        row = default.json()["threads"][0]
        assert "unread" not in row
        assert set(row) == {
            "id",
            "client",
            "operator_kind",
            "operator_value",
            "assignee",
            "kind",
            "status",
            "subject_type",
            "subject_id",
            "title",
            "priority",
            "last_message_at",
            "created_at",
        }

    async def test_with_unread_adds_only_the_unread_key(
        self, client: AsyncClient
    ) -> None:
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="hi",
                created_at=_T0,
            )
            await s.commit()

        off = (
            await _list(client, master)
        )[0]
        on = (await _list(client, master, with_unread="true"))[0]
        assert set(on) - set(off) == {"unread"}
        assert {k: v for k, v in on.items() if k != "unread"} == off


# ---------------------------------------------------------------------------
# Axis REPEAT
# ---------------------------------------------------------------------------
class TestRepeat:
    async def test_identical_requests_return_identical_answers(
        self, client: AsyncClient
    ) -> None:
        """Semantically a read: repeating it changes nothing."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="hi",
                created_at=_T0,
            )
            await s.commit()

        assert await _summary(client, master) == await _summary(
            client, master
        )
        assert await _batch(client, master, [thread_id]) == await _batch(
            client, master, [thread_id]
        )

    async def test_duplicate_thread_id_collapses_to_one_entry(
        self, client: AsyncClient
    ) -> None:
        """A repeated id is one entry with the true count -- not a
        doubled number and not a failure."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await post_message(
                s, thread_id=thread_id, sender=customer, body="a",
                created_at=_T0,
            )
            await post_message(
                s, thread_id=thread_id, sender=customer, body="b",
                created_at=_T1,
            )
            await s.commit()

        counts = await _batch(
            client, master, [thread_id, thread_id, thread_id]
        )
        assert counts == {str(thread_id): 2}

    async def test_limit_is_measured_before_dedup(
        self, client: AsyncClient
    ) -> None:
        """120 ids of which 80 are distinct -> 422.

        The limit is a property of the REQUEST SHAPE, not of its
        content: a caller must be able to tell from the contract alone
        whether their list is acceptable, without knowing how many
        duplicates it happens to contain.
        """
        distinct = [str(uuid4()) for _ in range(80)]
        raw = distinct + distinct[:40]  # 120 entries, 80 distinct
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={"participant": str(uuid4()), "thread_ids": raw},
        )
        assert len(raw) == 120
        assert len(set(raw)) == 80
        assert resp.status_code == 422

    async def test_exactly_the_limit_is_accepted(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={
                "participant": str(uuid4()),
                "thread_ids": [str(uuid4()) for _ in range(100)],
            },
        )
        assert resp.status_code == 200
        assert resp.json() == {"counts": {}}


# ---------------------------------------------------------------------------
# Axis EMPTY
# ---------------------------------------------------------------------------
class TestEmpty:
    async def test_empty_thread_ids_is_an_empty_map_not_an_error(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={"participant": str(uuid4()), "thread_ids": []},
        )
        assert resp.status_code == 200
        assert resp.json() == {"counts": {}}

    async def test_participant_with_no_threads_gets_zeros_not_404(
        self, client: AsyncClient
    ) -> None:
        factory = get_session_factory()
        async with factory() as s:
            lonely = await _recipient(s)
            await s.commit()

        assert await _summary(client, lonely) == {
            "has_unread": False,
            "threads_with_unread": 0,
            "unread_messages": 0,
        }

    async def test_unknown_participant_gets_zeros_not_404(
        self, client: AsyncClient
    ) -> None:
        """There is no foreign key on the way in and we do not start
        checking one: "you have nothing unread" is the true answer."""
        assert await _summary(client, uuid4()) == {
            "has_unread": False,
            "threads_with_unread": 0,
            "unread_messages": 0,
        }

    async def test_has_unread_tracks_threads_with_unread(
        self, client: AsyncClient
    ) -> None:
        """has_unread is sugar over threads_with_unread > 0 -- kept so a
        consumer drawing only a dot never has to read the numbers."""
        factory = get_session_factory()
        async with factory() as s:
            customer = await _recipient(s)
            master = await _recipient(s)
            thread_id = await _dm_thread(s, client=customer, master=master)
            await s.commit()

        empty = await _summary(client, master)
        assert empty["has_unread"] is False
        assert empty["threads_with_unread"] == 0

        async with factory() as s:
            await post_message(
                s, thread_id=thread_id, sender=customer, body="hi",
                created_at=_T0,
            )
            await s.commit()

        loaded = await _summary(client, master)
        assert loaded["has_unread"] is True
        assert loaded["threads_with_unread"] == 1


# ---------------------------------------------------------------------------
# Axis MISSING FIELDS
# ---------------------------------------------------------------------------
class TestMissingFields:
    async def test_body_without_participant_is_422(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={"thread_ids": [str(uuid4())]},
        )
        assert resp.status_code == 422

    async def test_body_without_thread_ids_is_422(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={"participant": str(uuid4())},
        )
        assert resp.status_code == 422

    async def test_garbage_instead_of_uuid_is_422(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={"participant": "not-a-uuid", "thread_ids": ["also-not"]},
        )
        assert resp.status_code == 422

    async def test_over_the_limit_is_422_not_silent_truncation(
        self, client: AsyncClient
    ) -> None:
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={
                "participant": str(uuid4()),
                "thread_ids": [str(uuid4()) for _ in range(101)],
            },
        )
        assert resp.status_code == 422

    async def test_garbage_participant_id_in_path_is_422(
        self, client: AsyncClient
    ) -> None:
        resp = await client.get(
            "/api/v1/participants/not-a-uuid/unread-summary"
        )
        assert resp.status_code == 422

    async def test_non_boolean_with_unread_is_422(
        self, client: AsyncClient
    ) -> None:
        resp = await client.get(
            "/api/v1/threads",
            params={"operator": str(uuid4()), "with_unread": "maybe"},
        )
        assert resp.status_code == 422

    async def test_list_without_operator_is_422(
        self, client: AsyncClient
    ) -> None:
        """`operator` is required by the signature, so "with_unread and
        no operator" is UNREACHABLE -- there is no branch in our code
        for it, and this test is what says so out loud."""
        resp = await client.get(
            "/api/v1/threads", params={"with_unread": "true"}
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------
class TestRouteIsNotShadowed:
    async def test_unread_counts_is_not_parsed_as_a_thread_id(
        self, client: AsyncClient
    ) -> None:
        """The literal segment must win over any /{thread_id} route --
        a 422 here would mean FastAPI tried to read "unread-counts" as
        a UUID."""
        resp = await client.post(
            "/api/v1/threads/unread-counts",
            json={"participant": str(uuid4()), "thread_ids": []},
        )
        assert resp.status_code == 200
