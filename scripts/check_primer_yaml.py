#!/usr/bin/env python3
# =============================================================================
# COMMS Service -- primer.yaml checked by the machine's own code (W2-2)
# =============================================================================
# CI job `machine` (.github/workflows/ci.yml) checks out Primer's machine at
# a pinned commit, installs it from its own lock and wheels into a venv of
# its own -- not comms' locks: its pydantic is another version -- and runs
# this script with the machine on PYTHONPATH. It imports the machine; it
# runs nowhere else.
#
# WHAT IS ASKED OF THE MACHINE, by the functions its steps call:
#   1. the loader (pridem/stacks/schema.py load_primer_yaml): both layers,
#      every field and every rule of contract 3 the file alone can break;
#   2. the seams, as seams_check asks them of a box: half seams (a name of
#      `provides` without its key of `env`), a seam under an option the
#      product takes without it, routes of a co-located service, data the
#      service does not accept, two pairs on one network -- against a
#      product that takes comms with every option of comms on. The product
#      is a stand-in, parsed by the machine's loader too; it names nothing
#      of any real product;
#   3. stop_for_migrate against the compose file: every service named is a
#      service of compose, none of them one `migrate` needs (the machine's
#      compose check, pridem/steps/compose.py forbidden).
# AND ONE THING THE MACHINE DOES NOT CHECK: `persistent: true`. No rule of
# the machine refuses a service without it -- the machine only reads it
# when it destroys -- so it is asserted here, on the machine's own model of
# the file (a misspelt key never gets this far: the loader refuses any
# unknown key).
#
# KNOWN CEILING (acknowledged by design -- W2-2, the compose findings):
#   1. Mechanics: the compose check (3) reads deploy/docker-compose.yml as
#      written, not as `docker compose config` renders it, and keeps only
#      its findings about stop_for_migrate. The compose file is not yet the
#      machine's form (env_file, container_name, the build context ..
#      and more: two dozen findings), so the rest of what the machine would
#      refuse there is not asserted, and a service named only through an
#      interpolation would be read as written.
#   2. Status: acknowledged by design.
#   3. Backlog ref: W1-2 -- the compose file to the machine's rules, and
#      `pridem check` in CI.
#   4. Promotion trigger (observable): `pridem check .` of this repository
#      ends with no finding of its compose step.
#   5. Agreed fix: CI runs `pridem check .` whole; this filter goes.
#   6. Rejected: `pridem check .` now (red until W1-2); the machine's JSON
#      Schema (the form only -- none of the three properties above); the
#      machine in comms' locks (another pydantic, a git dependency cannot
#      be hash-checked).
#
# Exit 0 = the machine finds nothing, 1 = findings (printed one per line).
# =============================================================================

from __future__ import annotations

from pathlib import Path

import yaml
from pridem.stacks.loader import ContractError
from pridem.stacks.schema import (
    Product,
    Service,
    load_primer_yaml,
    parse_primer_yaml,
)
from pridem.stacks.seams import (
    half_seams,
    installed,
    network_collisions,
    pairs,
    routes_on_co_located,
    seams_under_option,
    unaccepted,
)
from pridem.steps.compose import ComposeStack, forbidden

REPO_ROOT = Path(__file__).resolve().parents[1]

# The product that takes comms in check (2): a stand-in, every option of
# comms switched on by the caller.
_PRODUCT = """\
schema: 1
name: stand-in
kind: product
options: [{options}]
stacks:
  stand-in: {{ compose: docker-compose.yml }}
dependencies:
  {name}: {{ repo: {name}, version: ">=4.0.0,<5.0.0" }}
report: {{ health: /health, ready: /ready, logs: json, junit: true }}
"""

# The findings of the compose check (3) that are stop_for_migrate's
# (pridem/steps/compose.py _stop_problems).
_STOP_FINDINGS = ("stop_for_migrate: ", ": stopped for migrate, migrate needs it")


def _stop_finding(finding: str) -> bool:
    head, tail = _STOP_FINDINGS
    return finding.startswith(head) or finding.endswith(tail)


def check(root: Path = REPO_ROOT) -> tuple[list[str], list[str]]:
    """What the machine finds in `root`/primer.yaml, and what was checked
    (the pair: a check that ran on nothing reports nothing)."""
    try:
        stack = load_primer_yaml(root / "primer.yaml")
    except ContractError as error:
        return [f"loader: {problem}" for problem in error.problems], []
    if not isinstance(stack, Service):
        return [f"loader: comms is a service, the file says {stack.kind}"], []
    found: list[str] = []
    checked = [f"loader: {stack.kind} {stack.name}, {len(stack.env)} env keys"]
    if not stack.provides:
        found.append("provides is empty: the seams below would check nothing")

    product = parse_primer_yaml(
        _PRODUCT.format(options=", ".join(stack.options), name=stack.name),
        "the stand-in product",
    )
    assert isinstance(product, Product)
    on = frozenset(stack.options)
    services = {stack.name: stack}
    if installed(product, None, on=on) != [stack.name]:
        return [f"seams: the stand-in product does not install {stack.name}"], []
    for name, findings in (
        ("half seam", half_seams(product, services, None, on=on)),
        ("seam under option", seams_under_option(product, services, None, on=on)),
        ("routes", routes_on_co_located(product, services, None, on=on)),
        ("not accepted", unaccepted(product, services, None, on=on)),
        ("network", network_collisions(pairs(product, services, None, on=on))),
    ):
        found += [f"seams: {name}: {finding}" for finding in findings]
    checked.append(f"seams: provides {', '.join(stack.provides)}")

    for entry, stage in stack.stacks.items():
        if not stage.persistent:
            found.append(
                f"stacks.{entry}.persistent is not true -- not a finding of the "
                f"machine, which has no rule for it: comms keeps people's "
                f"conversations"
            )
        if not stage.stop_for_migrate:
            found.append(
                f"stacks.{entry}.stop_for_migrate is empty: nothing of comms "
                f"may run on a schema being migrated"
            )
        document = yaml.safe_load((root / stage.compose).read_text(encoding="utf-8"))
        compose = ComposeStack(stack, entry, root, "check", "check")
        found += [
            f"compose: {finding}"
            for finding in forbidden(compose, document)
            if _stop_finding(finding)
        ]
        checked.append(
            f"compose: stop_for_migrate {', '.join(stage.stop_for_migrate)} "
            f"against {len(document['services'])} services of {stage.compose}"
        )
    return found, checked


def main() -> int:
    found, checked = check()
    for line in found:
        print(line)
    if found:
        return 1
    for line in checked:
        print(f"ok -- {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
