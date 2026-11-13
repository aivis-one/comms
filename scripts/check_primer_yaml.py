#!/usr/bin/env python3
# =============================================================================
# COMMS Service -- primer.yaml checked by the machine's own code (W2-2, W1-2)
# =============================================================================
# CI job `machine` (.github/workflows/ci.yml) checks out Primer's machine at
# a pinned commit, installs it from its own lock and wheels into a venv of
# its own -- not comms' locks: its pydantic is another version -- and runs
# this script with the machine on PYTHONPATH. It imports the machine; it
# runs nowhere else.
#
# WHAT IS ASKED OF THE MACHINE, by its own code:
#   1. `pridem check` of this repository (pridem/verbs/check.py), run as
#      the machine's CLI runs it, in this process: primer.yaml by the
#      loader, primer.lock, and the compose file of every stack by the
#      machine's lists -- `docker compose config` of the file (the binary
#      alone, no daemon), every variable `${KEY:?}` and a key of `env`,
#      no env_file, no host path, no name of another project,
#      stop_for_migrate against the services. Every step must be green
#      but `compose`, whose findings must be EXACTLY the list CEILING
#      below: a finding beyond it is a defect of the compose file, one of
#      it missing is a ceiling gone stale -- both red, by name;
#   2. the seams, as seams_check asks them of a box: half seams (a name of
#      `provides` without its key of `env`), a seam under an option the
#      product takes without it, routes of a co-located service, data the
#      service does not accept, two pairs on one network -- against a
#      product that takes comms with every option of comms on. The product
#      is a stand-in, parsed by the machine's loader too; it names nothing
#      of any real product. `pridem check` reads one repository and asks
#      no seam.
#   3. comms' /health (W3-1), as the machine reads it after `up`: the
#      document tests/fixtures/health.json -- held to a live answer of
#      comms by key and type in the suite (tests/test_report.py), where the
#      machine cannot be imported -- by the machine's conformance of a
#      stack (check_health: the eleven fields of protocol 9.1, their
#      types, nothing else, `details` within its limit, `service` the
#      stack's name, `alembic_head` for a stack that migrates) and by
#      `seams_verify`'s own reader (_seams): every name of primer.yaml
#      `provides` among `seams.provides`, `requires_satisfied` true. The
#      pair: the document is `ok`, so a degraded one cannot pass by its
#      degradation.
# AND ONE THING THE MACHINE DOES NOT CHECK: `persistent: true`. No rule of
# the machine refuses a service without it -- the machine only reads it
# when it destroys -- so it is asserted here, on the machine's own model of
# the file (a misspelt key never gets this far: the loader refuses any
# unknown key).
#
# KNOWN CEILING (acknowledged by design -- W1-2, the profile's mount;
# temporary, until W4-1):
#   1. Mechanics: comms-app, comms-worker and comms-consumer bind the
#      product profile from a literal host path, /srv/comms/profile
#      (deploy/docker-compose.yml). The machine allows a bind of its
#      release only, so `pridem check` and the box's `build` refuse the
#      compose file with exactly CEILING, and the machine installs comms
#      nowhere. Nothing installs comms with the machine before the tag
#      v4.0.0, which comes after W4-1. A variable in the path
#      (${PROFILE_DIR}, as on v3) is no way out: a variable that is not a
#      key of `env` stops the check before `docker compose config`, and
#      every other rule of the compose file would go unchecked. The text
#      overlay has no form in compose until then (comms runs on the
#      profile alone).
#   2. Status: acknowledged by design -- temporary, until W4-1.
#   3. Backlog ref: W4-1 -- the profile as data the service takes (form
#      G, `accepts` of primer.yaml), with the overlay and the languages.
#   4. Promotion trigger (observable): the machine's rule for data a
#      service takes is in the commit of the machine CI pins, and
#      primer.yaml declares `accepts`.
#   5. Agreed fix: the profile and its overlay mounted by the machine
#      (form G); CEILING empty, and with it this marker.
#   6. Rejected: PROFILE_DIR as a key of `env` (a value of the box in a
#      service's file, and a stand-in path of the check resolves into the
#      release: the finding would vanish, the defect would stay); a named
#      volume (no finding, an empty profile at the start -- a ceiling the
#      check cannot see); the profile copied into the image (a profile is
#      the product's, never comms').
#
# Exit 0 = the machine finds nothing but CEILING, 1 = findings (printed one
# per line).
# =============================================================================

from __future__ import annotations

import io
import json
from pathlib import Path

from pridem.cli import run
from pridem.conformance.stack import check_health
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
from pridem.steps.services import _seams

REPO_ROOT = Path(__file__).resolve().parents[1]
# comms' /health as the suite holds it to a live answer (check 3).
HEALTH = Path("tests") / "fixtures" / "health.json"

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

# What `pridem check` may find in the compose file, and nothing else: the
# KNOWN CEILING above, until W4-1. Sorted, as the machine sorts them.
CEILING: tuple[str, ...] = (
    "comms-app: volumes=/srv/comms/profile",
    "comms-consumer: volumes=/srv/comms/profile",
    "comms-worker: volumes=/srv/comms/profile",
)
# The step whose findings CEILING is; every other step must be green.
_COMPOSE_STEP = "compose"


def machine_check(root: Path = REPO_ROOT) -> tuple[list[str], list[str]]:
    """What `pridem check <root>` finds beyond CEILING, and what it ran.

    The verdict's steps, by the machine's own CLI in this process: a step
    that is not green and not the compose step is a finding with its code;
    the compose step must have run (the pair: a check that did not run
    finds nothing) and found exactly CEILING."""
    out, err = io.StringIO(), io.StringIO()
    code = run(["check", str(root), "--json"], out=out, err=err)
    try:
        verdict = json.loads(out.getvalue())
    except json.JSONDecodeError:
        return [f"pridem check: exit {code}, no verdict: {err.getvalue().strip()}"], []
    found: list[str] = []
    checked: list[str] = []
    composed = False
    for step in verdict["steps"]:
        name, status = step["name"], step["status"]
        reason = step.get("reason") or {}
        if name == _COMPOSE_STEP and status in ("ok", "failed"):
            composed = True
            given = sorted(reason.get("found", [])) if status == "failed" else []
            if status == "failed" and reason.get("code") != "compose:compose-forbidden":
                found.append(f"pridem check: compose: {reason.get('code')}")
            found += [f"pridem check: {f}" for f in given if f not in CEILING]
            found += [
                f"pridem check: the ceiling is stale, the machine no longer "
                f"finds: {f}"
                for f in CEILING if f not in given
            ]
            checked.append(
                f"pridem check: compose found exactly the ceiling ({len(given)})"
            )
        elif status != "ok":
            found.append(f"pridem check: {name} {status}: {reason.get('code')}")
        else:
            checked.append(f"pridem check: {name} ok")
    if not composed:
        found.append("pridem check: the compose step did not run")
    return found, checked


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
    return found, checked


def health_check(root: Path = REPO_ROOT) -> tuple[list[str], list[str]]:
    """Check 3: comms' /health by the machine's conformance and its seams'
    reader, against primer.yaml's `provides` (header)."""
    stack = load_primer_yaml(root / "primer.yaml")
    assert isinstance(stack, Service)  # check() names it otherwise
    body = (root / HEALTH).read_bytes()
    found = [
        f"health: {problem.code} {problem.params}"
        for problem in check_health(body, migrate=True, stack_name=stack.name)
    ]
    document = json.loads(body)
    if document.get("status") != "ok":
        found.append("health: the document is not ok -- it would pass degraded")
    seams = _seams(body)
    if seams is None:
        found.append("health: the machine reads no seams in it")
    else:
        provides, satisfied = seams
        missing = [name for name in stack.provides if name not in provides]
        extra = [name for name in provides if name not in stack.provides]
        if missing or extra:
            found.append(f"health: seams.provides missing {missing}, extra {extra}")
        if not satisfied:
            found.append("health: requires_satisfied is not true")
    checked = [f"health: {HEALTH} conforms, provides {len(stack.provides)} seams"]
    return found, ([] if found else checked)


def main() -> int:
    found, checked = machine_check()
    seams, seen = check()
    found += seams
    checked += seen
    health, read = health_check()
    found += health
    checked += read
    for line in found:
        print(line)
    if found:
        return 1
    for line in checked:
        print(f"ok -- {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
