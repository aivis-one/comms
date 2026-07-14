# =============================================================================
# COMMS Service -- Domain-literal fence tests (Phase 3a item 4)
# =============================================================================
# The fence itself has logic (docstring skipping, whitelisting, the
# scheme requirement), so it gets tests: a broken fence is a silently
# open gate. The last test scans the REAL app/ tree -- the same check
# CI runs -- so a violation fails locally first.
# =============================================================================

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "check_domain_literals",
    REPO_ROOT / "scripts" / "check_domain_literals.py",
)
assert _spec is not None and _spec.loader is not None
fence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fence)


class TestScanSource:
    """Pattern + docstring semantics on synthetic sources."""

    def test_scheme_url_in_code_string_is_flagged(self) -> None:
        source = 'URL = "https://example.com/callback"\n'
        violations = fence.scan_source(source)
        assert len(violations) == 1
        assert violations[0][0] == 1

    def test_bare_telegram_host_is_flagged(self) -> None:
        source = 'HOST = "t.me"\nOTHER = "telegram.org"\n'
        assert len(fence.scan_source(source)) == 2

    def test_fstring_part_is_flagged(self) -> None:
        """f-string literal parts are Constant nodes inside JoinedStr
        -- the fence must see through them."""
        source = 'link = f"https://t.me/{bot}?startapp={x}"\n'
        assert len(fence.scan_source(source)) == 1

    def test_docstring_is_whitelisted(self) -> None:
        """A docstring may SHOW a URL as an example of an env value --
        that is documentation, not a binding."""
        source = (
            "def f() -> None:\n"
            '    """Example: \'https://t.me/velo_testbot\'."""\n'
        )
        assert fence.scan_source(source) == []

    def test_comment_is_invisible(self) -> None:
        source = "# see https://t.me/whatever\nx = 1\n"
        assert fence.scan_source(source) == []

    def test_schemeless_separator_is_clean(self) -> None:
        """The secret sanitizer's regex contains a bare '://' -- a
        scheme is REQUIRED for the fence to trip (the known grep
        false-positive this script exists to avoid)."""
        source = r'_RE = re.compile(r"(://[^/\s:@]+:)[^@/\s]+(@)")' + "\n"
        assert fence.scan_source(source) == []

    def test_plain_strings_are_clean(self) -> None:
        source = 'x = "hello"\ny = "config.py"\n'
        assert fence.scan_source(source) == []


class TestScanTree:
    """Whitelisting and tree traversal."""

    def test_whitelisted_file_is_skipped(self, tmp_path: Path) -> None:
        core = tmp_path / "core"
        core.mkdir()
        (core / "config.py").write_text(
            'default = "https://t.me/velo_testbot"\n', encoding="utf-8",
        )
        (tmp_path / "other.py").write_text(
            'url = "https://evil.example"\n', encoding="utf-8",
        )
        findings = fence.scan_tree(tmp_path)
        assert len(findings) == 1
        assert "other.py:1" in findings[0]

    def test_real_app_tree_is_clean(self) -> None:
        """The check CI runs, run locally: app/ carries no domain
        literals outside core/config.py."""
        assert fence.scan_tree() == []
