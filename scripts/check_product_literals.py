#!/usr/bin/env python3
# =============================================================================
# COMMS Service -- Product-literal fence (Release-Hardening item 4)
# =============================================================================
#
# CI gate for the universality invariant (arch doc #15): capability ->
# comms, fact -> profile. Product dictionaries and hardcoded domain
# type keys are FORBIDDEN in comms code; the product registers its
# types / categories / templates through the per-deploy profile. This
# script fails CI when a product-vocabulary token appears in app/
# BEHAVIOR -- string constants (docstrings excluded) or identifiers.
#
# SCOPE -- deliberately narrower than the audit:
#   - COMMENTS are invisible to AST and stay free: heritage notes
#     ("velo heritage of...", "cbshome base behavior") explain WHY the
#     merged design is what it is and are explicitly kept.
#   - DOCSTRINGS are whitelisted like in the sibling domain fence:
#     they document contracts and may name the consumer. The cost is
#     honest -- product flavor drifting back into docstrings is caught
#     by release audits, not by this fence.
#   - Everything else (string constants incl. f-string parts, and
#     identifiers: names, def/class names, attributes, arguments,
#     import aliases, keyword names) is BEHAVIOR and is fenced.
#
# MATCHING -- token equality over word segments, not substring/regex:
#   \b-style regex fails both ways here: "velo" substring-matches
#   "envelope"/"development" (false positive), while \bpractice\b
#   misses "practice_id" (underscore is a word char -- false
#   negative). Instead every scanned string/identifier is split on
#   non-alphanumeric characters AND underscores; a hit is a segment
#   exactly equal to a token. "envelope" -> ["envelope"] (clean);
#   "booking_confirmed" -> ["booking", "confirmed"] (hit);
#   "practice:42" -> ["practice", "42"] (hit).
#
# TOKENS -- product vocabulary of the two known products. NOT
# included, on purpose:
#   - "master": in messaging it is design vocabulary for the user-form
#     operator (docstrings/comments) and would be pure noise;
#   - "feedback": a generic English word with legitimate future uses.
#
# Exit code 0 = clean, 1 = violations (printed as file:line: snippet).
# Unit-tested in tests/test_product_literal_fence.py, which also scans
# the real tree -- a violation fails locally before it fails CI.
# =============================================================================

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_DIR = REPO_ROOT / "app"

# Product vocabulary (see header for what is deliberately absent).
PRODUCT_TOKENS = frozenset(
    {"velo", "cbshome", "booking", "practice", "checkin", "waitlist"}
)

# No whitelist: unlike domains (which legitimately live in
# core/config.py env defaults), product vocabulary has NO legitimate
# home anywhere in app/ behavior.

_SEGMENT_SPLIT = re.compile(r"[^a-zA-Z0-9]+|_")

# AST node attributes that carry identifier strings.
_IDENTIFIER_ATTRS = ("id", "name", "attr", "arg", "asname", "module")


def _segments(text: str) -> set[str]:
    """Lowercase word segments of a string (underscores split too)."""
    return {seg.lower() for seg in _SEGMENT_SPLIT.split(text) if seg}


def _hits(text: str) -> set[str]:
    """Product tokens present in the text as whole segments."""
    return _segments(text) & PRODUCT_TOKENS


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
    (f-string literal parts included) and every identifier-carrying
    node attribute is segment-matched against the token set.
    """
    tree = ast.parse(source, filename=filename)
    doc_ids = _docstring_ids(tree)
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        lineno = getattr(node, "lineno", 0)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) not in doc_ids and _hits(node.value):
                violations.append((lineno, f"string {node.value[:60]!r}"))
            continue
        for attr in _IDENTIFIER_ATTRS:
            value = getattr(node, attr, None)
            if isinstance(value, str) and _hits(value):
                violations.append((lineno, f"identifier {value!r}"))
    return violations


def scan_tree(app_dir: Path = APP_DIR) -> list[str]:
    """Scan a package tree; return human-readable violation lines."""
    findings: list[str] = []
    for path in sorted(app_dir.rglob("*.py")):
        rel = path.relative_to(app_dir).as_posix()
        source = path.read_text(encoding="utf-8")
        for lineno, snippet in scan_source(source, filename=rel):
            findings.append(f"{app_dir.name}/{rel}:{lineno}: {snippet}")
    return findings


def main() -> int:
    """CI entry point."""
    findings = scan_tree()
    if findings:
        print(
            "Product literals in app/ behavior (arch doc #15: "
            "capability -> comms, fact -> profile; product "
            "dictionaries in comms code are forbidden):"
        )
        for finding in findings:
            print(f"  {finding}")
        return 1
    print("product-literal fence: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
