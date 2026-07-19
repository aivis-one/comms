#!/usr/bin/env python3
# =============================================================================
# COMMS Service -- Domain-literal fence (Phase 3a item 4)
# =============================================================================
#
# CI gate for the late-binding invariant (arch doc §2.6 / decision 13,
# confirmed by the t.me incident of 2026-07-14): external domains live
# in ENV, never in data and never in code -- URLs are assembled at the
# edge, at send time. This script fails CI when a domain literal
# appears in app/ outside app/core/config.py (the one whitelisted
# place, where env defaults and their docs legitimately live).
#
# WHY AST AND NOT GREP:
#   - grep cannot whitelist docstrings (a docstring may show
#     "https://t.me/<bot>" as an EXAMPLE of the env value -- that is
#     documentation, not a binding);
#   - grep false-positives on scheme-less "://" inside the secret
#     sanitizer's regex (app/engine/formatters.py) -- the fence
#     requires an actual scheme or a known bare Telegram host;
#   - comments are invisible to AST, so commented examples are free.
#
# WHAT COUNTS AS A DOMAIN LITERAL (in any non-docstring string
# constant, f-string parts included):
#   - any scheme URL: https?://...  (the general fence);
#   - bare Telegram hosts: t.me / telegram.org / telegram.me -- the
#     incident class appears WITHOUT a scheme in message text.
#
# Exit code 0 = clean, 1 = violations (printed as file:line: snippet).
# Unit-tested in tests/test_domain_literal_fence.py, which also scans
# the real tree -- a violation fails locally before it fails CI.
# =============================================================================

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_DIR = REPO_ROOT / "app"

# The patterns are SHARED with the profile loader's data fence
# (Release-Hardening item 3b) and live in app/core/constants.py -- the
# Docker image ships app/ but not scripts/, so the import must point
# this way. The bootstrap makes `app` importable when the script is
# run as `python scripts/check_domain_literals.py` from the repo root.
sys.path.insert(0, str(REPO_ROOT))

from app.core.constants import DOMAIN_LITERAL_PATTERNS  # noqa: E402

# Files under app/ exempt from the fence (posix paths relative to
# app/). config.py is the ONE place a domain may live: env defaults
# and their documentation.
DEFAULT_WHITELIST = frozenset({"core/config.py"})

# Local alias -- scan_source below and the unit test address the
# patterns through this historical name.
_PATTERNS = DOMAIN_LITERAL_PATTERNS


def _docstring_ids(tree: ast.AST) -> set[int]:
    """id()s of Constant nodes that are docstrings (whitelisted)."""
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node,
            ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
        ):
            body = node.body
            if body and isinstance(body[0], ast.Expr):
                value = body[0].value
                if isinstance(value, ast.Constant) and isinstance(
                    value.value, str
                ):
                    ids.add(id(value))
    return ids


def scan_source(source: str, filename: str = "<string>") -> list[tuple[int, str]]:
    """Scan one Python source; return (lineno, snippet) violations.

    Docstrings are skipped structurally; every other string constant
    (f-string literal parts included -- they are Constant nodes inside
    JoinedStr) is checked against the patterns.
    """
    tree = ast.parse(source, filename=filename)
    doc_ids = _docstring_ids(tree)
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant):
            continue
        if not isinstance(node.value, str):
            continue
        if id(node) in doc_ids:
            continue
        if any(pattern.search(node.value) for pattern in _PATTERNS):
            violations.append((node.lineno, node.value[:80]))
    return violations


def scan_tree(
    app_dir: Path = APP_DIR,
    whitelist: frozenset[str] = DEFAULT_WHITELIST,
) -> list[str]:
    """Scan a package tree; return human-readable violation lines."""
    findings: list[str] = []
    for path in sorted(app_dir.rglob("*.py")):
        rel = path.relative_to(app_dir).as_posix()
        if rel in whitelist:
            continue
        source = path.read_text(encoding="utf-8")
        for lineno, snippet in scan_source(source, filename=rel):
            findings.append(f"{app_dir.name}/{rel}:{lineno}: {snippet!r}")
    return findings


def main() -> int:
    """CI entry point."""
    findings = scan_tree()
    if findings:
        print(
            "Domain literals in app/ outside core/config.py "
            "(arch doc §2.6 / decision 13: domains live in env, "
            "URLs are assembled at the edge):"
        )
        for finding in findings:
            print(f"  {finding}")
        return 1
    print("domain-literal fence: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
