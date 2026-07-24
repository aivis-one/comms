# =============================================================================
# COMMS Service -- Product-literal fence tests (Release-Hardening item 4)
# =============================================================================
# The fence has logic worth guarding (segment matching, docstring
# skipping, identifier coverage): a broken fence is a silently open
# gate on invariant #15. The last test scans the REAL app/ tree -- the
# same check CI runs -- so a violation fails locally first.
# =============================================================================

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "check_product_literals",
    REPO_ROOT / "scripts" / "check_product_literals.py",
)
assert _spec is not None and _spec.loader is not None
fence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fence)


class TestSegmentMatching:
    """Token equality over segments -- neither substring nor \\b."""

    def test_product_type_key_string_is_flagged(self) -> None:
        """The injection the fence exists for: a product dictionary."""
        source = 'TYPES = {"booking_confirmed": "new_booking"}\n'
        assert len(fence.scan_source(source)) == 2

    def test_underscored_identifier_is_flagged(self) -> None:
        """\\bpractice\\b would MISS practice_id -- segments do not."""
        source = "def cancel(practice_id: str) -> None: ...\n"
        assert fence.scan_source(source) == [(1, "identifier 'practice_id'")]

    def test_group_key_string_is_flagged(self) -> None:
        """Colon-separated product group keys are segment hits."""
        source = 'GROUP = "practice:42"\n'
        assert len(fence.scan_source(source)) == 1

    def test_envelope_and_development_are_clean(self) -> None:
        """The classic substring false positives: 'velo' inside
        'envelope' / 'development' must NOT trip the fence."""
        source = (
            'FIELD = "envelope"\n'
            'MODE = "development"\n'
            "def develop_envelope(developer: str) -> None: ...\n"
        )
        assert fence.scan_source(source) == []

    def test_case_insensitive(self) -> None:
        source = 'PRODUCT = "VELO"\n'
        assert len(fence.scan_source(source)) == 1


class TestScopeBoundaries:
    """Comments and docstrings stay free (heritage lives there)."""

    def test_docstring_mention_is_free(self) -> None:
        source = (
            "def f() -> None:\n"
            '    """Ported from the cbshome donor (canonical base)."""\n'
        )
        assert fence.scan_source(source) == []

    def test_comment_mention_is_free(self) -> None:
        source = "# velo heritage of joining values with underscores\nx = 1\n"
        assert fence.scan_source(source) == []

    def test_fstring_part_is_flagged(self) -> None:
        """f-string literal parts are Constants inside JoinedStr."""
        source = 'key = f"practice_{ident}"\n'
        assert len(fence.scan_source(source)) == 1

    def test_import_alias_is_flagged(self) -> None:
        source = "import velo_helpers\n"
        assert len(fence.scan_source(source)) >= 1


class TestTreeScan:
    """scan_tree formatting + the real-tree gate."""

    def test_findings_carry_file_and_line(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.py"
        bad.write_text('TYPE = "booking_confirmed"\n', encoding="utf-8")
        findings = fence.scan_tree(tmp_path)
        assert len(findings) == 1
        assert "bad.py:1" in findings[0]

    def test_real_app_tree_is_clean(self) -> None:
        """The check CI runs: app/ behavior carries no product
        vocabulary (invariant #15, confirmed by the release audit)."""
        assert fence.scan_tree() == []


class TestDeployScan:
    """The deploy/ text pass (Phase 5) -- same discipline, no AST."""

    def test_behavior_line_is_flagged(self) -> None:
        """The injection this pass exists for: a product literal in
        deploy behavior (compose/env/shell)."""
        assert len(fence.scan_deploy_text("STREAM=booking_confirmed\n")) == 1

    def test_full_line_comment_is_free(self) -> None:
        """Heritage notes mirror the AST scan's comment invisibility."""
        assert fence.scan_deploy_text("# velo pattern note\nKEY=value\n") == []

    def test_tree_skips_markdown_and_formats_findings(self, tmp_path: Path) -> None:
        """.md is documentation (the docstring-whitelist analogue);
        everything else is behavior and carries file:line."""
        (tmp_path / "doc.md").write_text("velo is named here\n", encoding="utf-8")
        (tmp_path / "bad.env").write_text("STREAM=booking_confirmed\n", encoding="utf-8")
        findings = fence.scan_deploy_tree(tmp_path)
        assert len(findings) == 1
        assert "bad.env:1" in findings[0]

    def test_real_deploy_tree_is_clean(self) -> None:
        """The check CI runs: deploy/ behavior is product-agnostic
        (DD §7) -- no whitelists, no pragmas."""
        assert fence.scan_deploy_tree() == []
