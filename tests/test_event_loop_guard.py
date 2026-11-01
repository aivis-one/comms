# =============================================================================
# COMMS Service -- no test takes the session's event loop away (FIX ST-1 #0)
# =============================================================================
#
# The suite runs on ONE event loop for the session, the main thread's
# current loop between tests too (tests/conftest.py, THE SESSION'S EVENT
# LOOP). A test of ST-1 called dlq_scrub.main([]) in the suite's process;
# main() is asyncio.run, which on its way out leaves the main thread
# without a current loop -- and 1763 tests after it failed on the stand.
# tests/conftest.py makes such a test fail by itself; this guard keeps the
# tree from writing one, by the AST of tests/ and app/:
#
#   (a) no test calls a form that ends or replaces the loop -- asyncio.run,
#       asyncio.Runner, new_event_loop, set_event_loop, run_until_complete,
#       anyio.run, uvicorn.run, a loop's close(). A form inside a STRING is
#       a script for a subprocess, and passes by construction;
#   (b) the DANGEROUS functions of app/ are those that call such a form,
#       and -- by closure -- those that call a dangerous function of app/,
#       the names resolved by each module's imports (an entry point's
#       main() and every wrapper of it);
#   (c) no test calls a dangerous function, unless the same test function
#       replaces that module's asyncio.run (patch("app.worker.asyncio.run",
#       ...) -- tests/test_channel_key_sets.py checks what main() builds
#       BEFORE its loop, which only the suite's process can see).
#
# The one allowed form: tests/conftest.py's pytest_runtest_call puts the
# session's loop BACK after a test that removed it.
#
# LIMIT: a call the AST cannot resolve -- getattr(module, "main"),
# importlib -- is not seen. There is none today (getattr(...main,
# import_module: no match in tests/).
#
# Mutations (built once, by hand):
#   M1  dlq_scrub.main([]) back in the suite's process -> test_the_tree
#       (and the culprit fails by itself in one process: conftest)
#   M2  the patch of app.worker.asyncio.run dropped in
#       test_channel_key_sets                         -> test_the_tree
#   M3  entrypoint.main() of the consumer back in the process -> test_the_tree
#   M4  the closure finds nothing                      -> the pairs
# =============================================================================

import ast
import asyncio
from dataclasses import dataclass
from pathlib import Path

from tests.conftest import current_loop

_REPO = Path(__file__).resolve().parents[1]

# Calls by their resolved dotted name...
_LOOP_CALLS = frozenset({
    "asyncio.run", "asyncio.Runner", "asyncio.new_event_loop",
    "asyncio.set_event_loop", "anyio.run", "uvicorn.run",
})
# ...and by the attribute called, whatever its receiver.
_LOOP_ATTRIBUTES = frozenset({
    "run_until_complete", "set_event_loop", "new_event_loop",
})
_ALLOWED = frozenset({("tests/conftest.py", "pytest_runtest_call")})


@dataclass(frozen=True)
class Call:
    path: str
    line: int
    function: str  # the enclosing top-level function or method; "" at module level
    name: str  # resolved dotted name, or the bare text when unresolved
    is_form: bool


def _imports(nodes: list[ast.stmt], module: str) -> dict[str, str]:
    """Local name -> dotted name, from the import statements of `nodes`:
    a module's top-level statements, or everything inside one function."""
    statements: list[ast.AST] = []
    for node in nodes:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            statements.extend(ast.walk(node))
        else:
            statements.append(node)
    names: dict[str, str] = {}
    for sub in statements:
        if isinstance(sub, ast.Import):
            for alias in sub.names:
                head = alias.name.split(".")[0]
                names[alias.asname or head] = alias.name if alias.asname else head
        elif isinstance(sub, ast.ImportFrom):
            base = sub.module or ""
            if sub.level:
                parts = module.split(".")[: -sub.level]
                base = ".".join([*parts, base] if base else parts)
            for alias in sub.names:
                names[alias.asname or alias.name] = (
                    f"{base}.{alias.name}" if base else alias.name
                )
    return names


def _dotted(node: ast.expr) -> list[str] | None:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return list(reversed(parts))
    return None


def _calls(path: str, module: str, source: str) -> list[Call]:
    tree = ast.parse(source)
    module_names = _imports(tree.body, module)
    defined = {
        n.name for n in tree.body
        if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
    }
    found: list[Call] = []

    def resolve(func: ast.expr, local: dict[str, str], owner: str) -> str:
        parts = _dotted(func)
        if parts is None:
            return ast.unparse(func)
        head, rest = parts[0], parts[1:]
        if head == "self" and owner and rest:
            return ".".join([module, owner, *rest])
        if head in local:
            return ".".join([local[head], *rest])
        if head in module_names:
            return ".".join([module_names[head], *rest])
        if head in defined:
            return ".".join([module, head, *rest])
        return ".".join(parts)

    def visit(
        body: list[ast.stmt], function: str, owner: str, local: dict[str, str],
    ) -> None:
        for node in body:
            for sub in ast.walk(node):
                if not isinstance(sub, ast.Call):
                    continue
                name = resolve(sub.func, local, owner)
                func = sub.func
                attribute = func.attr if isinstance(func, ast.Attribute) else ""
                receiver = (
                    ast.unparse(func.value) if isinstance(func, ast.Attribute) else ""
                )
                is_form = (
                    name in _LOOP_CALLS
                    or attribute in _LOOP_ATTRIBUTES
                    or (attribute == "close" and "loop" in receiver.lower())
                )
                found.append(Call(path, sub.lineno, function, name, is_form))

    top_level: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            visit(node.body, node.name, "", _imports([node], module))
        elif isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                    visit(item.body, f"{node.name}.{item.name}", node.name,
                          _imports([item], module))
        else:
            top_level.append(node)
    visit(top_level, "", "", {})
    return found


def _module_of(path: Path) -> str:
    return ".".join(path.relative_to(_REPO).with_suffix("").parts)


def sources(root: str) -> dict[str, tuple[str, str]]:
    """module -> (repo-relative path, source), for every .py under root."""
    return {
        _module_of(path): (
            path.relative_to(_REPO).as_posix(), path.read_text(encoding="utf-8"),
        )
        for path in sorted((_REPO / root).rglob("*.py"))
    }


def dangerous(app: dict[str, tuple[str, str]]) -> set[str]:
    """The functions of app/ that end or replace the loop, by closure."""
    calls: dict[str, set[str]] = {}
    seeds: set[str] = set()
    for module, (path, source) in app.items():
        for call in _calls(path, module, source):
            if not call.function:
                continue
            owner = f"{module}.{call.function}"
            calls.setdefault(owner, set()).add(call.name)
            if call.is_form:
                seeds.add(owner)
    found = set(seeds)
    while True:
        more = {
            owner for owner, names in calls.items()
            if owner not in found and names & found
        }
        if not more:
            return found
        found |= more


def violations(
    tests: dict[str, tuple[str, str]], danger: set[str],
) -> list[str]:
    found: list[str] = []
    for module, (path, source) in tests.items():
        tree = ast.parse(source)
        patched: dict[str, set[str]] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                patched.setdefault(node.name, set()).update(
                    sub.value for sub in ast.walk(node)
                    if isinstance(sub, ast.Constant) and isinstance(sub.value, str)
                )
        for call in _calls(path, module, source):
            function = call.function.rpartition(".")[2]
            if call.is_form and (path, function) not in _ALLOWED:
                found.append(f"{path}:{call.line} {call.name}")
            elif call.name in danger:
                owner_module = call.name.rpartition(".")[0]
                if f"{owner_module}.asyncio.run" not in patched.get(function, set()):
                    found.append(f"{path}:{call.line} {call.name} (asyncio.run)")
    return found


_SYNTHETIC_APP = {
    "app._wrapper": ("<synthetic app module>", (
        "from app.transport import dlq_scrub\n"
        "def wrap():\n"
        "    return dlq_scrub.main([])\n"
        "def outer():\n"
        "    return wrap()\n"
    )),
}


# Not paths of the tree: tests/test_g2_hygiene.py resolves those.
_SYNTHETIC = "<synthetic test>"


def _one_test(source: str) -> dict[str, tuple[str, str]]:
    return {"tests.test_synthetic": (_SYNTHETIC, source)}


class TestTheTree:
    def test_the_tree(self) -> None:
        assert violations(sources("tests"), dangerous(sources("app"))) == []

    def test_the_entry_points_are_dangerous(self) -> None:
        """The pair of the above: the closure finds what is there."""
        assert {
            "app.worker.main", "app.consumer.main", "app.transport.dlq_scrub.main",
        } <= dangerous(sources("app"))


class TestThePairs:
    def test_a_wrapper_in_app_is_dangerous_by_closure(self) -> None:
        danger = dangerous({**sources("app"), **_SYNTHETIC_APP})
        assert {"app._wrapper.wrap", "app._wrapper.outer"} <= danger
        found = violations(_one_test(
            "from app._wrapper import outer\n"
            "def test_x():\n"
            "    outer()\n"
        ), danger)
        assert found == ["<synthetic test>:3 app._wrapper.outer (asyncio.run)"]

    def test_main_in_the_process_is_refused(self) -> None:
        found = violations(_one_test(
            "from app.transport import dlq_scrub\n"
            "def test_x():\n"
            "    dlq_scrub.main([])\n"
        ), dangerous(sources("app")))
        assert found == [
            "<synthetic test>:3 app.transport.dlq_scrub.main (asyncio.run)",
        ]

    def test_a_form_in_a_test_is_refused(self) -> None:
        found = violations(_one_test(
            "import asyncio\n"
            "def test_x():\n"
            "    asyncio.run(f())\n"
            "    asyncio.set_event_loop(None)\n"
            "    loop.close()\n"
        ), set())
        assert found == [
            "<synthetic test>:3 asyncio.run",
            "<synthetic test>:4 asyncio.set_event_loop",
            "<synthetic test>:5 loop.close",
        ]

    def test_a_script_for_a_subprocess_passes(self) -> None:
        assert violations(_one_test(
            "def test_x():\n"
            "    script = 'import asyncio\\nasyncio.run(main())\\n'\n"
            "    run([python, '-c', script])\n"
        ), dangerous(sources("app"))) == []

    def test_the_patched_loop_passes_and_only_it(self) -> None:
        patched = (
            "from unittest.mock import patch\n"
            "def test_x():\n"
            "    with patch('app.worker.asyncio.run', fake):\n"
            "        from app.worker import main\n"
            "        main()\n"
        )
        danger = dangerous(sources("app"))
        assert violations(_one_test(patched), danger) == []
        unpatched = patched.replace("'app.worker.asyncio.run'", "'app.worker.other'")
        assert violations(_one_test(unpatched), danger) == [
            "<synthetic test>:5 app.worker.main (asyncio.run)",
        ]


class TestTheSlot:
    """tests/conftest.py reads the main thread's current loop from the
    policy's own slot -- held to this Python here."""

    def test_a_sync_test_sees_the_sessions_loop(self) -> None:
        loop = current_loop()
        assert loop is not None and not loop.is_closed() and not loop.is_running()

    async def test_an_async_test_runs_on_it(self) -> None:
        assert current_loop() is asyncio.get_running_loop()
