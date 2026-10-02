# =============================================================================
# D1 / R9 -- the product-literal fence sees the repository root.
# =============================================================================
#
# MUTATIONS:
#   M16 the root left out of main()      -> test_main_fails_on_a_literal_at_the_root
#   M17 the empty-root gate removed      -> test_main_fails_when_a_root_is_empty
# =============================================================================

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "check_product_literals",
    REPO_ROOT / "scripts" / "check_product_literals.py",
)
assert _spec is not None and _spec.loader is not None
fence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fence)

# A product literal built at runtime, so this file is not itself one.
_LITERAL = "https://t.me/" + "velo" + "_testbot"


def _tree(tmp_path: Path, root_line: str) -> Path:
    """A minimal repository: one file in app/, one in deploy/, and a
    .env.example at the root holding `root_line`."""
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "x.py").write_text("X = 1\n")
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "compose.yml").write_text("name: comms\n")
    (tmp_path / ".env.example").write_text(root_line + "\n")
    return tmp_path


class TestTheRoot:
    def test_a_literal_outside_a_comment_is_caught(self, tmp_path: Path) -> None:
        """done-when (1)."""
        root = _tree(tmp_path, f"TELEGRAM_BOT_URL={_LITERAL}")
        (finding,) = fence.scan_root_files(root)
        assert finding.startswith(".env.example:1:")

    def test_a_literal_in_a_full_line_comment_is_not(self, tmp_path: Path) -> None:
        """By design (D1 gate, R9): the root's comments carry heritage
        notes today, as deploy/'s and app/'s do -- see the script's
        ROOTS paragraph. Pinned so the choice cannot change silently."""
        root = _tree(tmp_path, f"# e.g. {_LITERAL}")
        assert fence.scan_root_files(root) == []

    def test_only_files_at_the_top_level(self, tmp_path: Path) -> None:
        """The directories at the root are other trees: a literal in
        one of them is not this scan's."""
        root = _tree(tmp_path, "OK=1")
        (root / "scripts").mkdir()
        (root / "scripts" / "tool.sh").write_text(f"URL={_LITERAL}\n")
        assert fence.scan_root_files(root) == []
        assert [p.name for p in fence.root_files(root)] == [".env.example"]

    def test_documentation_at_the_root_is_not_scanned(self, tmp_path: Path) -> None:
        root = _tree(tmp_path, "OK=1")
        (root / "README.md").write_text(f"see {_LITERAL}\n")
        assert fence.scan_root_files(root) == []

    def test_the_real_root_is_clean_and_not_empty(self) -> None:
        """The pair: clean because something was scanned."""
        assert fence.root_files()
        assert fence.scan_root_files() == []


class TestTheGateOnItself:
    def test_counts_name_every_root(self, tmp_path: Path) -> None:
        assert fence.scanned_counts(_tree(tmp_path, "OK=1")) == {
            "app/": 1, "deploy/": 1, "the repository root": 1,
        }

    def test_main_fails_when_a_root_is_empty(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    ) -> None:
        """done-when (2): zero files in a root is a failure, not clean."""
        monkeypatch.setattr(
            fence, "scanned_counts",
            lambda: {"app/": 5, "deploy/": 3, "the repository root": 0},
        )
        assert fence.main() == 1
        assert "no file to scan in the repository root" in capsys.readouterr().out

    def test_main_fails_on_a_literal_at_the_root(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setattr(
            fence, "scan_root_files", lambda: [".env.example:1: line 'x'"],
        )
        assert fence.main() == 1
        assert ".env.example:1:" in capsys.readouterr().out

    def test_main_passes_on_the_real_tree(
        self, capsys: pytest.CaptureFixture[str],
    ) -> None:
        assert fence.main() == 0
        assert "root files" in capsys.readouterr().out


def test_the_contract_title_names_no_product() -> None:
    """done-when (3). The pair: the title line exists and the matcher
    does see a product token when one is there."""
    title = (REPO_ROOT / "deploy" / "INTEGRATION.md").read_text().splitlines()[0]
    assert title.startswith("# COMMS")
    assert fence._hits(title) == set()
    assert fence._hits("# COMMS x " + "VELO".lower())
