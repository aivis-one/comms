# =============================================================================
# COMMS Service -- A string the database can store (EM-1, API-2)
# =============================================================================
#
# ONE RULE FOR EVERY DOOR. Postgres keeps no NUL in text or jsonb, and
# the driver sends UTF-8, which a lone surrogate ("\ud800" -- valid JSON,
# a Python string) does not have. Such a string reaches the INSERT and
# the driver fails there: a 500 for what is the caller's input. So it is
# refused where it comes in, by this rule, written once:
#   - the stream (app/transport/events.py check_event_data);
#   - the REST bodies (app/api/deps.py refuse_unstorable_body).
# Both doors are rank 5 and may not import each other (app/__init__.py,
# H9); the rule lives here, below both.
#
# THE RULE DOES NOT READ THE LETTER: it asks whether the database can
# hold the string, never what the string says, and a refusal names the
# field, never the value.
# =============================================================================

from typing import Any

UNSTORABLE = (
    "a string the database cannot store (a NUL character or a lone surrogate)"
)


def storable(text: str) -> bool:
    """Whether Postgres can hold this string: no NUL, and UTF-8 for it
    exists (no lone surrogate)."""
    if "\x00" in text:
        return False
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def holds_unstorable(value: Any) -> bool:
    """Whether `value` holds a string the database cannot store -- a
    value, or a key of a mapping, at any depth. Iterative: a stack,
    never a recursion."""
    stack: list[Any] = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            if not storable(item):
                return True
        elif isinstance(item, dict):
            for key, child in item.items():
                if isinstance(key, str) and not storable(key):
                    return True
                stack.append(child)
        elif isinstance(item, list):
            stack.extend(item)
    return False
