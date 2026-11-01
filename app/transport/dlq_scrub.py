# =============================================================================
# COMMS Service -- bring what the dead-letter stream holds to its rule (ST-1)
# =============================================================================
#
# `deploy/comms-deploy.sh dlq-scrub [--apply]` runs this module in a
# one-off container of the built image: python -m app.transport.dlq_scrub.
#
# WHY. The dead letters have no expiry (app/transport/consumer.py, DEAD
# LETTERS): an entry written by an older comms stays, under its older
# rule, until DLQ_MAXLEN newer ones push it out -- on a quiet box, never.
# Each rule since has narrowed what a dead letter keeps; this brings the
# entries already there to the rule of today, once, by the operator's
# hand. It is no second format and no transitional period: the code
# writes one form, and this makes the old data that form.
#
# THE RULE IS THE CONSUMER'S, NOT A COPY. Every entry is brought to
# dead_letter_fields / dlq_kept_data_fields of app/transport/consumer.py.
#
# THE FORMS, by the entry's FIELDS -- the text of a reason never decides:
#   current  `_dlq_dropped` (every dead letter since G4 has it) and every
#            field a dead letter has (DLQ_FIELDS): `_dlq_data` narrowed to
#            what its event keeps today, each key it loses listed as
#            `data.<key>` in `_dlq_dropped`; an entry already by the rule
#            is left as it is;
#   a1       `_dlq_error` without `_dlq_dropped` (before G4): the entry's
#            own fields verbatim, the reason an exception's text;
#   a0       `error`, `attempts` and `source_entry_id` without any `_dlq_`
#            field (before the prefix of review 3c.1): the same, its
#            three fields unprefixed;
#   other    anything else -- left untouched, counted on its own line.
# a0 and a1 are rebuilt from their own `event` and `data` by
# dead_letter_fields; their other fields are dropped by name; their
# attempts and source entry id are kept; their reason is NOT: an
# exception's text may hold the letter or a statement's values, and
# turning it into a class and a place would mean parsing it -- it becomes
# SCRUBBED_REASON.
#
# ONE WRITE, NOTHING LOST. The stream is read and rewritten in one
# optimistic transaction: WATCH, XRANGE, MULTI, DEL, an XADD of every
# entry under ITS OWN ID, EXEC. The ids, their order and their number
# stay; what changes is the stream's entries-added counter (XINFO STREAM),
# which nothing of comms reads. A dead letter the consumer adds meanwhile
# aborts the EXEC (WatchError), and the pass starts over -- at most
# _ATTEMPTS times.
#
# A STREAM WITH A CONSUMER GROUP IS NOT TOUCHED. Nothing of comms reads
# the dead letters through a group; a group on the stream would be
# someone's reader, and a DEL would take its pending list and cursor with
# it -- the verb refuses instead, naming the group.
#
# NOTHING OF AN ENTRY IS PRINTED: counts by form, and the names of the
# known events only.
# =============================================================================

import argparse
import asyncio
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import WatchError

from app.core.sanitize import pipeline_error_of
from app.transport.consumer import (
    DLQ_FIELDS,
    dead_letter_fields,
    dlq_kept_data_fields,
)
from app.transport.events import (
    ENVELOPE_DATA_FIELD,
    ENVELOPE_EVENT_FIELD,
    EVENT_FIELDS,
    entry_field_name,
)

# The reason of an entry written before G4: its text is not kept (ST-1).
# ONE copy -- the contract names it (deploy/INTEGRATION.md, "How long
# comms keeps things"), held there by tests/test_dlq_scrub.py.
SCRUBBED_REASON = (
    "written by an older comms: its reason was an exception's text "
    "and is not kept"
)

# The forms, by their fields (module header).
FORM_CURRENT = "current"
FORM_A1 = "a1"
FORM_A0 = "a0"
FORM_OTHER = "other"

_A1_META = ("_dlq_error", "_dlq_attempts", "_dlq_source_entry_id")
_A0_META = ("error", "attempts", "source_entry_id")
_DLQ_PREFIX = "_dlq_"

# Whole passes before the scrub gives up on a stream the consumer keeps
# writing to.
_ATTEMPTS = 5

# Exit codes of the verb (deploy/comms-deploy.sh cmd_dlq_scrub).
EXIT_DONE = 0  # nothing to do, or done
EXIT_REFUSED = 1  # a consumer group on the stream
EXIT_CANNOT = 2  # redis down, the stream kept changing
EXIT_TO_DO = 3  # the check found entries to bring to the rule


@dataclass
class Scrub:
    """What one pass found and, with --apply, did."""

    total: int = 0
    # (form, event) -> entries that the rule changes
    changed: Counter[tuple[str, str]] = field(default_factory=Counter)
    by_rule: int = 0
    other: int = 0
    applied: bool = False


def _text(raw: Any) -> str | None:
    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return str(raw)


def form_of(fields: dict[Any, Any]) -> str:
    """The form of one dead letter, by the NAMES of its fields."""
    names = {entry_field_name(key) for key in fields}
    if "_dlq_dropped" in names:
        return FORM_CURRENT if names <= set(DLQ_FIELDS) else FORM_OTHER
    if "_dlq_error" in names:
        return FORM_A1 if set(_A1_META) <= names else FORM_OTHER
    if set(_A0_META) <= names and not any(
        name.startswith(_DLQ_PREFIX) for name in names
    ):
        return FORM_A0
    return FORM_OTHER


def _event_label(event: str | None) -> str:
    """An event's name for the report -- a known one only: the name of
    an unknown event is the producer's text."""
    return event if event in EVENT_FIELDS else "(other)"


def _narrowed(fields: dict[Any, Any]) -> dict[str, str] | None:
    """A current dead letter under today's rule; None when it is by the
    rule already. Raises ValueError for one whose values cannot be read
    -- left to FORM_OTHER by the caller."""
    dead: dict[str, str] = {}
    for key, value in fields.items():
        text = _text(value)
        if text is None:
            raise ValueError("a value is not UTF-8")
        dead[entry_field_name(key)] = text
    if "_dlq_data" not in dead:
        return None
    data = json.loads(dead["_dlq_data"])
    dropped = json.loads(dead["_dlq_dropped"])
    if not isinstance(data, dict) or not isinstance(dropped, list):
        raise ValueError("not the form of a dead letter")
    kept = dlq_kept_data_fields(dead.get(ENVELOPE_EVENT_FIELD))
    if kept is None:
        # The event keeps nothing of its data any more.
        del dead["_dlq_data"]
        dead["_dlq_dropped"] = json.dumps(sorted({*dropped, ENVELOPE_DATA_FIELD}))
        return dead
    lost = [key for key in data if key not in kept]
    if not lost:
        return None
    dead["_dlq_data"] = json.dumps(
        {k: v for k, v in data.items() if k in kept}, sort_keys=True,
    )
    dead["_dlq_dropped"] = json.dumps(
        sorted({*dropped, *(f"{ENVELOPE_DATA_FIELD}.{k}" for k in lost)}),
    )
    return dead


def _rebuilt(fields: dict[Any, Any], meta: tuple[str, str, str]) -> dict[str, str]:
    """An entry of a0 or a1, rebuilt by dead_letter_fields from its own
    fields; its reason is SCRUBBED_REASON. Raises ValueError when its
    attempts are not a number -- left to FORM_OTHER by the caller."""
    _reason, attempts_name, source_name = meta
    named = {entry_field_name(key): key for key in fields}
    attempts_text = _text(fields[named[attempts_name]])
    source = _text(fields[named[source_name]])
    if attempts_text is None or not attempts_text.isdigit() or source is None:
        raise ValueError("not the form of a dead letter")
    original = {
        key: value for key, value in fields.items()
        if entry_field_name(key) not in meta
    }
    return dead_letter_fields(
        original,
        reason=SCRUBBED_REASON,
        attempts=int(attempts_text),
        entry_id=source,
    )


def scrubbed(fields: dict[Any, Any]) -> tuple[str, dict[str, str] | None]:
    """(the entry's form, its fields under today's rule -- None when it is
    left as it is)."""
    form = form_of(fields)
    try:
        if form == FORM_CURRENT:
            return form, _narrowed(fields)
        if form == FORM_A1:
            return form, _rebuilt(fields, _A1_META)
        if form == FORM_A0:
            return form, _rebuilt(fields, _A0_META)
    except (ValueError, KeyError):
        return FORM_OTHER, None
    return FORM_OTHER, None


async def groups_of(redis: Redis, key: str) -> list[str]:
    """The consumer groups on the stream; none when there is no stream
    (XINFO GROUPS refuses a key that does not exist)."""
    if not await redis.exists(key):
        return []
    groups = await redis.xinfo_groups(key)
    return [_text(group["name"]) or repr(group["name"]) for group in groups]


async def scrub(redis: Redis, key: str, *, apply: bool) -> Scrub:
    """One pass over the stream: what the rule changes, and with `apply`
    the rewrite (module header, ONE WRITE). Raises WatchError when the
    stream changed under every one of _ATTEMPTS passes."""
    for attempt in range(1, _ATTEMPTS + 1):
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            entries = await pipe.xrange(key)
            report = Scrub(total=len(entries))
            rewritten: list[tuple[Any, dict[Any, Any]]] = []
            for entry_id, fields in entries:
                form, new = scrubbed(fields)
                if form == FORM_OTHER:
                    report.other += 1
                elif new is None:
                    report.by_rule += 1
                else:
                    event = _text(new.get(ENVELOPE_EVENT_FIELD, ""))
                    report.changed[(form, _event_label(event))] += 1
                rewritten.append((entry_id, fields if new is None else new))
            if not apply or not report.changed:
                # Leaving the block resets the pipeline: the WATCH ends.
                return report
            # redis-py leaves Pipeline.multi untyped.
            pipe.multi()  # type: ignore[no-untyped-call]
            pipe.delete(key)
            for entry_id, fields in rewritten:
                pipe.xadd(key, fields, id=entry_id)
            try:
                await pipe.execute()
            except WatchError:
                if attempt == _ATTEMPTS:
                    raise
                continue
            report.applied = True
            return report
    raise AssertionError("unreachable: the loop returns or raises")


def _print_report(report: Scrub, key: str, apply: bool, script: str) -> None:
    print(f"Dead letters in {key}: {report.total}")
    print(f"  by the rule already: {report.by_rule}")
    for (form, event), count in sorted(report.changed.items()):
        verb = "brought to the rule" if report.applied else "to bring to the rule"
        print(f"  {verb}: {count} ({form}, {event})")
    if report.other:
        print(f"  of no known form, left untouched: {report.other}")
    if not report.changed:
        print("✓ nothing to do")
    elif report.applied:
        print(f"✓ {sum(report.changed.values())} brought to the rule; "
              "ids, order and number kept")
    elif not apply:
        print(f"  Next: {script} dlq-scrub --apply")


async def run(redis: Redis, key: str, *, apply: bool, script: str) -> int:
    """The verb: refuse a stream with a group, report, rewrite with
    `apply`. Returns the exit code."""
    try:
        groups = await groups_of(redis, key)
        if groups:
            print(f"✗ {key} has consumer groups: {', '.join(sorted(groups))} "
                  "-- someone reads it; nothing touched")
            return EXIT_REFUSED
        report = await scrub(redis, key, apply=apply)
    except WatchError:
        print(f"✗ {key} kept changing during {_ATTEMPTS} passes -- nothing "
              "touched")
        print(f"  Next: {script} dlq-scrub --apply, again")
        return EXIT_CANNOT
    except Exception as exc:
        print(f"✗ cannot read {key}: {pipeline_error_of(exc)} -- nothing touched")
        print(f"  Next: {script} status")
        return EXIT_CANNOT
    _print_report(report, key, apply, script)
    if report.changed and not report.applied:
        return EXIT_TO_DO
    return EXIT_DONE


def main(argv: list[str] | None = None) -> int:
    from app.core.config import settings
    from app.transport.connection import redis_client

    parser = argparse.ArgumentParser(prog="dlq-scrub")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--script", default="comms-deploy.sh")
    args = parser.parse_args(argv)

    async def _main() -> int:
        redis = redis_client()
        try:
            return await run(
                redis, settings.dlq_stream, apply=args.apply, script=args.script,
            )
        finally:
            await redis.aclose()

    return asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(main())
