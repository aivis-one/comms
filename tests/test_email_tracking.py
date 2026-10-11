# =============================================================================
# H3 Б2 -- every message to the mail provider goes without tracking
# =============================================================================
#
# Asserted on the request as it was BUILT, on a stand-in transport (the
# provider stub of tests/test_email_channel.py): the three per-message
# options switching tracking off, the same for every type and message.
#
# MUTATIONS these tests were written against (each turns one red):
#   M7  the tracking options removed          -> every test here
#   M8  o:tracking alone, without -clicks and
#       -opens                                -> test_..._the_three_options
#   M9  a value other than "no"               -> test_..._the_three_options
# =============================================================================

import re
from pathlib import Path

import pytest

from tests.test_email_channel import _Provider, _recipient, _send

_DOC = (
    Path(__file__).resolve().parents[1] / "deploy" / "INTEGRATION.md"
).read_text(encoding="utf-8")

_NO_TRACKING = {
    "o:tracking": "no",
    "o:tracking-clicks": "no",
    "o:tracking-opens": "no",
}


class TestTheRequest:
    async def test_every_message_carries_the_three_options(self) -> None:
        provider = _Provider()
        assert await _send(provider) is True
        fields = provider.fields()
        tracking = {k: v for k, v in fields.items() if k.startswith("o:tracking")}
        assert tracking == _NO_TRACKING
        # The pair: the message itself is there, untouched.
        assert fields["text"] == "World"
        assert fields["to"] == "user@example.test"

    @pytest.mark.parametrize("overrides", [
        {"type": "unit_event", "body": "plain words"},
        {"type": "unit_event", "body": "a link https://example.test/x?code=1"},
        {"type": "unit_routed", "body": "another type"},
    ])
    async def test_the_same_for_any_type_and_any_body(
        self, overrides: dict[str, str],
    ) -> None:
        """No knob: the options do not depend on the type, the body or a
        link in it."""
        provider = _Provider()
        await _send(provider, **overrides)
        fields = provider.fields()
        assert {k: fields[k] for k in _NO_TRACKING} == _NO_TRACKING

    async def test_the_same_for_any_recipient_locale(self) -> None:
        for locale in ("en", "ru", None):
            provider = _Provider()
            await _send(provider, recipient=_recipient(locale=locale))
            fields = provider.fields()
            assert {k: fields[k] for k in _NO_TRACKING} == _NO_TRACKING


class TestTheContract:
    def _paragraph(self) -> str:
        match = re.search(r"\*\*No tracking\.\*\*(.*?)\n\n\*\*", _DOC, re.S)
        assert match is not None, "the no-tracking paragraph is missing"
        return match.group(1)

    def test_the_document_says_it_once(self) -> None:
        assert _DOC.count("**No tracking.**") == 1
        paragraph = self._paragraph()
        assert "clicks and opens are switched off in each request" in paragraph
        assert "whatever\nthe domain's own tracking settings say" in paragraph

    def test_the_document_names_the_unsubscribe_footer_as_the_domain_s(
        self,
    ) -> None:
        """What a request cannot switch off is said, with the reading
        command the product runs to check its domain."""
        paragraph = self._paragraph()
        assert "unsubscribe" in paragraph
        assert "DOMAIN" in paragraph
        assert "/tracking" in _DOC
        assert "['unsubscribe']['active']" in _DOC
        assert "It must print `False`." in _DOC
