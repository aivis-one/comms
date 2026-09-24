#!/bin/bash
# -u: abort on unset variables. pipefail: a pipeline fails if any stage
# fails. No -e: deliberately absent (inherited discipline from
# velo-manage.sh, verified there): `set -e` is suspended inside
# if/&&/|| contexts, so every command whose failure matters is checked
# EXPLICITLY instead, right where it runs.
set -uo pipefail

# ==============================================================================
# COMMS Deploy CLI (Phase 5) -- sibling of velo-manage.sh
# ==============================================================================
#
# Repeatable bring-up and lifecycle of the comms stack on a product
# VPS, NEXT TO the product stack (DD-1/DD-3): dedicated containers on
# the shared external network "aivis-shared". Product-agnostic by
# design: no product vocabulary in here (enforced by
# scripts/check_product_literals.py, which scans deploy/).
#
# TRACKED in the repo, next to the compose it drives (velo lesson:
# provisioned-once copies drift; `update` pulls this file like any
# other, so a fix here reaches every server on the next update).
#
# Layout on the VPS (mirrors the product's install):
#   /opt/comms/                INSTALL_BASE -- per-instance state
#   /opt/comms/.env            master env (secrets; written ONCE)
#   /opt/comms/profile/        per-product profile (survives update)
#   /opt/comms/backups/        db dumps
#   /opt/comms/repo/           the comms checkout
#   /opt/comms/repo/deploy/    compose + this script
#   /opt/comms/repo/deploy/.env -> /opt/comms/.env   (symlink; compose
#                              reads ./.env for env_file AND for
#                              ${PROFILE_DIR} interpolation)
#
# Subcommands: install | update | start | stop | restart | logs | db |
# status.
#
# THREE LIFECYCLE VERBS, THREE DIFFERENT WIDTHS -- the difference is
# deliberate and easy to erase by "unifying" them later:
#   restart  RECREATES the THREE app containers only, and waits for
#            health. Narrow because its job is delivering what the
#            service only reads when a container starts -- the profile
#            AND the environment (see cmd_restart for why a signal
#            cannot deliver the second); the datastores hold state and
#            bouncing them for an application-level change is
#            gratuitous risk.
#   stop     takes the WHOLE stack down, postgres and redis included.
#            It is the switch you throw when the machine goes off, not
#            an application-level operation.
#   start    brings the whole stack back up and waits for health.
# A product CLI driving this registry-style reaches for start/stop to
# cover a whole box, and for restart to reload data. Collapsing
# restart into stop+start would make a template edit cost a database
# bounce.
# Operational tails (dlq, lag) are deliberately ABSENT -- deferred
# with a trigger (DD §0), do not add them here ahead of it.
#
# Usage: comms-deploy.sh {install|update|restart|logs|db|status} [args]
# ==============================================================================

INSTALL_BASE="/opt/comms"
REPO_DIR="$INSTALL_BASE/repo"
COMPOSE_DIR="$REPO_DIR/deploy"
ENV_FILE="$INSTALL_BASE/.env"
ENV_LINK="$COMPOSE_DIR/.env"
PROFILE_DIR_DEFAULT="$INSTALL_BASE/profile"
BACKUP_DIR="$INSTALL_BASE/backups"
NETWORK_NAME="aivis-shared"
COMPOSE_CMD="docker compose"
APP_PORT=8000

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Ensure we're in the right directory for docker compose.
cd_compose() {
    cd "$COMPOSE_DIR" || {
        echo -e "${RED}ERROR: $COMPOSE_DIR not found -- is the comms repo cloned to $REPO_DIR?${NC}"
        exit 1
    }
}

# Source the master env file (simple KEY=VALUE lines, values are
# openssl-hex or plain paths -- safe to source).
load_env() {
    if [ ! -f "$ENV_FILE" ]; then
        echo -e "${RED}ERROR: $ENV_FILE not found -- run 'install' first.${NC}"
        exit 1
    fi
    # shellcheck source=/dev/null
    source "$ENV_FILE"
}

# ------------------------------------------------------------------------------
# install steps -- each one IDEMPOTENT on its own, so a re-run after a
# partial failure resumes instead of wrecking existing state.
# ------------------------------------------------------------------------------

# Step 1: the shared external network. No-op if it already exists
# (the product's installer may have created it first -- either side
# may win the race, the result is identical).
ensure_network() {
    if docker network inspect "$NETWORK_NAME" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Network '$NETWORK_NAME' already exists${NC}"
    else
        if docker network create "$NETWORK_NAME" > /dev/null; then
            echo -e "${GREEN}✓ Network '$NETWORK_NAME' created${NC}"
        else
            echo -e "${RED}✗ Failed to create network '$NETWORK_NAME'${NC}"
            exit 1
        fi
    fi
}

# Step 2: the master env with secrets. THE GUARD (velo pattern): an
# existing file is NEVER regenerated -- secrets are minted exactly
# once; re-minting while data volumes exist would lock the stack out
# of its own database.
generate_env() {
    if [ -f "$ENV_FILE" ]; then
        echo -e "${GREEN}✓ $ENV_FILE already exists -- secrets NOT re-minted${NC}"
        return 0
    fi

    local pg_pass redis_pass service_token
    pg_pass=$(openssl rand -hex 24) || { echo -e "${RED}✗ openssl failed${NC}"; exit 1; }
    redis_pass=$(openssl rand -hex 24) || { echo -e "${RED}✗ openssl failed${NC}"; exit 1; }
    service_token=$(openssl rand -hex 32) || { echo -e "${RED}✗ openssl failed${NC}"; exit 1; }

    mkdir -p "$INSTALL_BASE"
    # Written with a heredoc in one shot; 600 before secrets land.
    touch "$ENV_FILE" && chmod 600 "$ENV_FILE"
    cat > "$ENV_FILE" <<EOF
# COMMS deploy env -- GENERATED by comms-deploy.sh install $(date -u +%Y-%m-%dT%H:%M:%SZ)
# Secrets are minted ONCE; this file is never regenerated while it
# exists. Reference for every variable: deploy/.env.example.

APP_ENV=production
LOG_LEVEL=INFO

POSTGRES_USER=comms
POSTGRES_DB=comms
POSTGRES_PASSWORD=$pg_pass
DATABASE_URL=postgresql+asyncpg://comms:$pg_pass@comms-postgres:5432/comms

REDIS_PASSWORD=$redis_pass
REDIS_URL=redis://:$redis_pass@comms-redis:6379/0

COMMS_SERVICE_TOKEN=$service_token

# A channel is decided by its key set: all keys empty = the deploy does
# not have that channel (legal); all set = live; some set = comms
# refuses to start. The PRODUCT installer, which owns these
# credentials, writes them here.
TELEGRAM_BOT_TOKEN=
TELEGRAM_BOT_URL=
EMAIL_MAILGUN_API_KEY=
EMAIL_MAILGUN_DOMAIN=
EMAIL_FROM_ADDRESS=

DEFAULT_LOCALE=en
DEFAULT_TIMEZONE=UTC

PROFILE_DIR=$PROFILE_DIR_DEFAULT

# Token hand-over target: ABSOLUTE path of the PRODUCT backend's .env
# on this VPS (per-product CONFIG, DD-8 -- the value for a concrete
# product is documented in deploy/INTEGRATION.md). Empty = install
# prints the COMMS_* block for manual paste instead of writing it.
PRODUCT_ENV_PATH=
EOF
    echo -e "${GREEN}✓ $ENV_FILE generated (postgres/redis/service-token minted)${NC}"
    echo -e "${YELLOW}  Telegram keys are EMPTY: this deploy has no telegram until the${NC}"
    echo -e "${YELLOW}  PRODUCT installer, which owns the bot, writes BOTH of them here.${NC}"
}

# Step 3: compose reads ./.env next to docker-compose.yml -- link it
# to the master outside the checkout, so `update` (git) never touches
# secrets. ln -sfn is idempotent.
ensure_env_link() {
    if ln -sfn "$ENV_FILE" "$ENV_LINK"; then
        echo -e "${GREEN}✓ $ENV_LINK -> $ENV_FILE${NC}"
    else
        echo -e "${RED}✗ Failed to link $ENV_LINK${NC}"
        exit 1
    fi
}

# Step 4: the profile. The generic smoke profile is a FLOOR, not a
# default: it is copied in ONLY when PROFILE_DIR is empty, which is the
# standalone case (this CLI run on its own, before any product is
# wired). A product installer points PROFILE_DIR at the profile it
# ships and this step then finds a populated directory and keeps its
# hands off it. Either way the profile survives `update`: it lives
# outside the checkout, or outside this repo entirely.
seed_profile() {
    local profile_dir="${PROFILE_DIR:-$PROFILE_DIR_DEFAULT}"
    mkdir -p "$profile_dir"
    if [ -n "$(ls -A "$profile_dir" 2>/dev/null)" ]; then
        echo -e "${GREEN}✓ Profile at $profile_dir already present -- left untouched${NC}"
        return 0
    fi
    if cp -r "$COMPOSE_DIR/smoke-profile/." "$profile_dir/"; then
        echo -e "${GREEN}✓ Generic smoke profile seeded into $profile_dir${NC}"
        echo -e "${YELLOW}  Standalone bring-up: three chat types only. A product${NC}"
        echo -e "${YELLOW}  installer supplies PROFILE_DIR and this step is skipped.${NC}"
    else
        echo -e "${RED}✗ Failed to seed the smoke profile${NC}"
        exit 1
    fi
}

# Idempotent KEY=VALUE write into an env file: update in place when
# the key exists, append when it does not. Values here are hex/URLs
# without '|', which is the sed delimiter.
upsert_env_var() {
    local file="$1" key="$2" value="$3"
    if grep -q "^${key}=" "$file"; then
        if ! sed -i "s|^${key}=.*|${key}=${value}|" "$file"; then
            echo -e "${RED}✗ Failed to update ${key} in ${file}${NC}"
            return 1
        fi
    else
        if ! printf '%s=%s\n' "$key" "$value" >> "$file"; then
            echo -e "${RED}✗ Failed to append ${key} to ${file}${NC}"
            return 1
        fi
    fi
    return 0
}

# Step 5: the trust seam (DD-6). The three COMMS_* variables the
# product backend needs, delivered from the SINGLE source (our env).
# The target is pure CONFIG (PRODUCT_ENV_PATH, DD-8): set -> written
# straight into the product's .env, idempotently; empty (the shipped
# default) -> the block is printed for manual paste. No product path
# lives in this code -- per-product values belong to INTEGRATION.md.
handover_token() {
    load_env
    local api_url="http://comms-app:${APP_PORT}"
    local target="${PRODUCT_ENV_PATH:-}"

    if [ -n "$target" ] && [ -f "$target" ]; then
        local ok=0
        upsert_env_var "$target" "COMMS_SERVICE_TOKEN" "$COMMS_SERVICE_TOKEN" || ok=1
        upsert_env_var "$target" "COMMS_API_URL" "$api_url" || ok=1
        upsert_env_var "$target" "COMMS_REDIS_URL" "$REDIS_URL" || ok=1
        if [ "$ok" -eq 0 ]; then
            echo -e "${GREEN}✓ COMMS_SERVICE_TOKEN / COMMS_API_URL / COMMS_REDIS_URL written into $target${NC}"
            echo -e "${YELLOW}  Restart the product backend to pick them up${NC}"
            return 0
        fi
        echo -e "${RED}✗ Could not write all variables into $target -- paste the block below manually${NC}"
    elif [ -n "$target" ]; then
        echo -e "${YELLOW}PRODUCT_ENV_PATH is set but '$target' does not exist -- paste this block into the product's .env manually:${NC}"
    else
        echo -e "${YELLOW}PRODUCT_ENV_PATH is empty (see deploy/INTEGRATION.md for the product's value) -- paste this block into the product's .env manually:${NC}"
    fi
    echo
    echo "COMMS_SERVICE_TOKEN=$COMMS_SERVICE_TOKEN"
    echo "COMMS_API_URL=$api_url"
    echo "COMMS_REDIS_URL=$REDIS_URL"
    echo
}

# Poll the comms-app container health until healthy or timeout. The
# API is INTERNAL (no host port), so the probe reads docker's own
# health state instead of curling from the host.
wait_for_app() {
    local attempts=60 status
    echo "Waiting for comms-app to become healthy (migration runs first)..."
    for i in $(seq 1 "$attempts"); do
        status=$(docker inspect --format '{{.State.Health.Status}}' comms-app 2>/dev/null)
        if [ "$status" = "healthy" ]; then
            echo -e "${GREEN}✓ comms-app is healthy (after ${i} checks)${NC}"
            return 0
        fi
        sleep 2
    done
    echo -e "${RED}✗ comms-app did not become healthy${NC}"
    show_app_log_tail
    return 1
}

# Print the last lines of comms-app and where the full log is.
#
# The reason for a refused start is in the container log -- comms-app
# prints a readable message naming the broken keys. Shown where the
# person running the command is looking, instead of pointing away.
# ONE copy, called from every place a start can fail: wait_for_app, and
# each `compose up` whose failure exits before wait_for_app runs --
# there compose's own last word is "dependency failed to start", which
# names the container but not the reason.
#
# When comms-app has printed NOTHING -- it was created but never
# started because a datastore it depends on did not become healthy --
# its empty tail is a dead end, so the datastores' tails follow. Only
# then: on an ordinary refused start the output is what it always was.
show_app_log_tail() {
    local app_tail service service_tail
    app_tail=$($COMPOSE_CMD logs --no-color --tail=20 comms-app 2>&1)
    echo "Last lines of comms-app:"
    if [ -n "${app_tail//[[:space:]]/}" ]; then
        echo "$app_tail" | sed 's/^/  /'
        echo "Full logs: $0 logs comms-app"
        return 0
    fi
    echo "  (no output -- comms-app never ran; its dependencies follow)"
    for service in comms-postgres comms-redis; do
        service_tail=$($COMPOSE_CMD logs --no-color --tail=20 "$service" 2>&1)
        echo "Last lines of $service:"
        if [ -n "${service_tail//[[:space:]]/}" ]; then
            echo "$service_tail" | sed 's/^/  /'
        else
            echo "  (no output)"
        fi
    done
    echo "Full logs: $0 logs <service>"
}

# ------------------------------------------------------------------------------
# Subcommands
# ------------------------------------------------------------------------------

cmd_install() {
    echo -e "${CYAN}== comms install ==${NC}"
    cd_compose
    ensure_network
    generate_env
    ensure_env_link
    load_env
    seed_profile
    handover_token
    echo "Building and starting the comms stack..."
    if ! $COMPOSE_CMD up -d --build; then
        echo -e "${RED}✗ compose up failed${NC}"
        show_app_log_tail
        exit 1
    fi
    if ! wait_for_app; then
        exit 1
    fi
    echo -e "${GREEN}✓ comms stack is up on '$NETWORK_NAME'${NC}"
}

cmd_update() {
    echo -e "${CYAN}== comms update ==${NC}"
    cd "$REPO_DIR" || {
        echo -e "${RED}ERROR: $REPO_DIR not found${NC}"
        exit 1
    }
    # Explicit check (the whole point of this file's ancestor): a
    # failed pull must not silently rebuild stale code as "updated".
    if ! git pull --ff-only; then
        echo -e "${RED}✗ git pull failed -- update aborted, nothing rebuilt${NC}"
        exit 1
    fi
    cd_compose
    ensure_env_link
    if ! $COMPOSE_CMD build; then
        echo -e "${RED}✗ image build failed -- containers left as they were${NC}"
        exit 1
    fi
    # Recreated comms-app re-runs `alembic upgrade head` in its
    # command before serving -- the migration IS the restart path.
    # The three app containers are recreated ALWAYS, by name. Whether
    # plain `up -d` notices an edited .env depends on how the installed
    # compose version hashes a service, and this path must not depend
    # on it: an undetected environment change costs an hour, a
    # recreate costs seconds (cmd_restart says why a signal is not
    # enough either).
    # postgres and redis are NOT named, so compose applies its default
    # to them -- recreated only when their own definition changed --
    # which is what `up -d` did before.
    if ! $COMPOSE_CMD up -d --force-recreate comms-app comms-worker comms-consumer; then
        echo -e "${RED}✗ compose up failed${NC}"
        show_app_log_tail
        exit 1
    fi
    if ! wait_for_app; then
        exit 1
    fi
    echo -e "${GREEN}✓ comms updated (pulled, rebuilt, migrated)${NC}"
}

# Recreate the three application containers -- API, worker, consumer --
# and wait for the API to be healthy again.
#
# NOT a hot reload. The processes read their profile and their
# environment once, when the container starts; restart delivers both by
# starting new containers. Naming it after the profile would promise a
# reload endpoint that is deliberately not built.
#
# RECREATE, NOT SIGNAL. `compose restart` stops and starts the SAME
# container, and a container's environment is fixed when it is
# CREATED: env_file (deploy/.env) is read at creation, not at start. A
# signalled restart therefore re-reads the profile from its bind mount
# but runs on the OLD environment -- an operator who fixed .env and ran
# restart would get the old process back without a word. `up
# --force-recreate` creates the containers anew and so reads .env
# again.
#
# postgres and redis are left alone on purpose: they hold the data, and
# bouncing them for an application-level change is gratuitous risk.
# --no-deps is what guarantees it: without it compose also converges
# the datastores, recreating them if their definition changed since
# they were created.
cmd_restart() {
    echo -e "${CYAN}== comms restart ==${NC}"
    cd_compose
    if ! $COMPOSE_CMD up -d --force-recreate --no-deps comms-app comms-worker comms-consumer; then
        echo -e "${RED}✗ restart failed${NC}"
        show_app_log_tail
        exit 1
    fi
    # `up -d` can return before comms-app has finished starting: it
    # validates its profile and environment during startup and dies on
    # a bad one, which may be a health failure a few seconds later
    # rather than a non-zero exit here.
    if ! wait_for_app; then
        exit 1
    fi
    echo -e "${GREEN}✓ comms-app / comms-worker / comms-consumer recreated${NC}"
}

# Bring the whole stack up and wait until it is actually serving.
#
# ORDER IS NOT WRITTEN HERE ON PURPOSE. deploy/docker-compose.yml
# already declares it -- comms-app depends_on comms-postgres and
# comms-redis with `condition: service_healthy` -- so `up -d` starts
# things in dependency order by itself. Spelling the sequence out again
# in this script would be a second copy of that graph, and the two
# would drift the first time the compose changed.
#
# Idempotent: `up -d` on an already-running stack is a no-op, and
# wait_for_app returns on its first check when the container is
# already healthy. Safe to call from product automation that does not
# know the current state.
#
# `install` is NOT implied. A box that never ran install has no
# compose and no env, and cd_compose / load_env already say so in
# their own words -- adding a third phrasing of the same state would
# just be one more sentence to keep in step.
cmd_start() {
    echo -e "${CYAN}== comms start ==${NC}"
    load_env
    cd_compose
    # The shared network is EXTERNAL: compose will refuse to start if
    # nobody has created it. Either side may have created it (see
    # ensure_network) -- but on a machine that came up from cold, the
    # product's installer is not necessarily the one that ran first.
    ensure_network
    if ! $COMPOSE_CMD up -d; then
        echo -e "${RED}✗ start failed${NC}"
        show_app_log_tail
        exit 1
    fi
    if ! wait_for_app; then
        exit 1
    fi
    echo -e "${GREEN}✓ comms stack is up${NC}"
}

# Take the WHOLE stack down -- app, worker, consumer AND the datastores.
#
# Wider than restart by design (see the header): this is called when a
# box is being shut down, and leaving a database running behind a
# `stop` would be a lie the operator only discovers in `docker ps`.
#
# Data survives: `down` removes containers and the compose-managed
# network, NOT the named volumes. The shared external network is not
# ours to remove and compose leaves it alone.
#
# Idempotent: `down` on an already-stopped stack removes nothing and
# exits 0.
cmd_stop() {
    echo -e "${CYAN}== comms stop ==${NC}"
    cd_compose
    if ! $COMPOSE_CMD down; then
        echo -e "${RED}✗ stop failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ comms stack is down (volumes kept)${NC}"
}

cmd_logs() {
    cd_compose
    $COMPOSE_CMD logs -f --tail=200 "$@"
}

cmd_db() {
    cd_compose
    load_env
    local action="${1:-}"
    case "$action" in
        dump)
            mkdir -p "$BACKUP_DIR"
            local out
            out="$BACKUP_DIR/comms-$(date -u +%Y%m%d-%H%M%S).sql"
            if $COMPOSE_CMD exec -T comms-postgres pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > "$out"; then
                echo -e "${GREEN}✓ Dumped to $out${NC}"
            else
                rm -f "$out"
                echo -e "${RED}✗ pg_dump failed${NC}"
                exit 1
            fi
            ;;
        restore)
            local src="${2:-}"
            if [ -z "$src" ] || [ ! -f "$src" ]; then
                echo -e "${RED}Usage: $0 db restore <dump.sql>${NC}"
                exit 1
            fi
            echo -e "${YELLOW}This OVERWRITES the comms database from $src.${NC}"
            read -r -p "Type 'yes' to proceed: " answer
            if [ "$answer" != "yes" ]; then
                echo "Aborted."
                exit 1
            fi
            if $COMPOSE_CMD exec -T comms-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < "$src"; then
                echo -e "${GREEN}✓ Restored from $src${NC}"
            else
                echo -e "${RED}✗ restore failed${NC}"
                exit 1
            fi
            ;;
        migrate)
            # Manual migration outside the restart path (the normal
            # one runs inside comms-app's command on every start).
            if $COMPOSE_CMD exec comms-app alembic upgrade head; then
                echo -e "${GREEN}✓ Migrations applied${NC}"
            else
                echo -e "${RED}✗ alembic failed${NC}"
                exit 1
            fi
            ;;
        *)
            echo "Usage: $0 db {dump|restore <file>|migrate}"
            exit 1
            ;;
    esac
}

# Drain the rows migration 0013 refuses on (F1.5) -- the exit of the
# protocol update window (deploy/INTEGRATION.md, "The protocol update
# window"). ONE source for the breakdown: the queries are migration
# 0013's own, _BLOCKING_KINDS to count and _DRAIN_DELETES to delete, run
# by a one-off container of the image `update` built -- the same file
# the refusing migration reads, so a kind added there is never missing
# here (tests/test_drain_source.py).
#
#   drain           CHECK: counts per kind, deletes nothing, stops
#                   nothing. It only reads; if it touches a table while
#                   the migration runs it waits, it does not break --
#                   so do not stop comms-app here "for symmetry".
#   drain --apply   DELETE, in this order and in no other:
#                   1. stop comms-app -- its restart loop re-runs the
#                      migration, whose ALTER TABLE would race the
#                      deletion; stopped, the race cannot exist. Not
#                      stopped -> nothing is deleted (code 2);
#                   2. dump the database -- AFTER the stop, so the dump
#                      is not of a base the migration may still touch.
#                      No dump -> nothing is deleted (code 2);
#                   3. confirm: the operator types `yes` (no flag skips
#                      it -- a deletion is seen by a person);
#                   4. delete every kind in ONE transaction -- an
#                      interruption rolls it all back;
#                   5. check again and report before -> after, with the
#                      dump's path and the next command: `start`.
#
# Exit codes: 0 clean, 1 rows to delete (or the deletion was declined),
# 2 cannot check (database down, no schema, revision at or past 0013,
# an image without migration 0013, a failed stop or dump).
cmd_drain() {
    cd_compose
    load_env
    local apply=0 arg
    for arg in "$@"; do
        case "$arg" in
            --apply) apply=1 ;;
            *)
                echo -e "${RED}Usage: $0 drain [--apply]${NC}"
                exit 2
                ;;
        esac
    done

    if ! $COMPOSE_CMD ps --status running --services 2>/dev/null \
            | grep -qx comms-postgres; then
        echo -e "${RED}✗ comms-postgres is not running -- start the database first${NC}"
        exit 2
    fi

    local rc=0
    drain_driver check || rc=$?
    if [ "$apply" -eq 0 ] || [ "$rc" -ne 1 ]; then
        exit "$rc"
    fi

    echo -e "${YELLOW}This stops comms-app, dumps the database and DELETES the rows above.${NC}"
    read -r -p "Type 'yes' to proceed: " answer
    if [ "$answer" != "yes" ]; then
        echo "Aborted -- nothing stopped, nothing deleted."
        exit 1
    fi

    if ! $COMPOSE_CMD stop comms-app; then
        echo -e "${RED}✗ could not stop comms-app -- nothing deleted${NC}"
        exit 2
    fi
    echo -e "${CYAN}comms-app stopped.${NC}"

    mkdir -p "$BACKUP_DIR"
    local dump
    dump="$BACKUP_DIR/comms-predrain-$(date -u +%Y%m%d-%H%M%S).sql"
    if ! $COMPOSE_CMD exec -T comms-postgres \
            pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > "$dump"; then
        rm -f "$dump"
        echo -e "${RED}✗ pg_dump failed -- nothing deleted (comms-app stays stopped)${NC}"
        exit 2
    fi
    echo -e "${GREEN}✓ Dumped to $dump${NC}"

    rc=0
    drain_driver apply || rc=$?
    echo "Dump taken before the deletion: $dump"
    echo -e "${YELLOW}comms-app is STOPPED. Next: $0 start${NC}"
    exit "$rc"
}

# The driver: one-off container of the built image, python on stdin.
# Prints its own report; its exit code is cmd_drain's (see above).
drain_driver() {
    $COMPOSE_CMD run --rm --no-deps -T --entrypoint python comms-app - "$1" <<'PY'
import asyncio
import importlib.util
import os
import re
import sys
from pathlib import Path

VERSIONS = Path("migrations/versions")


def fail(message: str) -> None:
    print(f"✗ {message}")
    sys.exit(2)


def migration_0013():
    found = sorted(VERSIONS.glob("*_0013_*.py"))
    if not found:
        fail("the built image has no migration 0013 -- run update first")
    spec = importlib.util.spec_from_file_location("m0013", found[0])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def known_revisions() -> set[str]:
    pattern = re.compile(r'^revision: str = "([^"]+)"', re.M)
    return {
        m.group(1)
        for path in VERSIONS.glob("*.py")
        if (m := pattern.search(path.read_text(encoding="utf-8")))
    }


async def counts(conn, kinds: dict[str, str]) -> dict[str, int]:
    return {kind: await conn.fetchval(query) for kind, query in kinds.items()}


async def main(mode: str) -> int:
    import asyncpg

    module = migration_0013()
    url = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(url)
    try:
        if await conn.fetchval("SELECT to_regclass('alembic_version')") is None:
            fail("no schema here (no alembic_version) -- nothing to drain")
        revision = await conn.fetchval("SELECT version_num FROM alembic_version")
        if revision not in known_revisions():
            fail(f"revision {revision!r} is not in this image's migration chain")
        if int(revision.split("_", 1)[0]) >= 13:
            fail(f"the schema is at {revision}, at or past 0013 -- nothing to drain")
        before = await counts(conn, module._BLOCKING_KINDS)
        if mode == "check":
            print(f"schema at {revision}; rows migration 0013 refuses on:")
            for kind, count in before.items():
                print(f"  {kind}={count}")
            return 0 if not any(before.values()) else 1
        async with conn.transaction():
            for kind in module._BLOCKING_KINDS:
                await conn.execute(module._DRAIN_DELETES[kind])
        after = await counts(conn, module._BLOCKING_KINDS)
        print("deleted, per kind (before -> after):")
        for kind in before:
            print(f"  {kind}: {before[kind]} -> {after[kind]}")
        return 0 if not any(after.values()) else 1
    finally:
        await conn.close()


sys.exit(asyncio.run(main(sys.argv[1])))
PY
}

# Run the suite on THIS box, against an isolated database.
#
# WHY A BOX RUN EXISTS AT ALL, next to a CI that already runs the same
# suite: CI checks what does not depend on the machine -- lint, types,
# fences -- on every push, and it does it on a runner that is not this
# server. This checks the other half: the real Postgres, the image that
# was actually built here, the environment as it is actually projected.
# Neither replaces the other.
#
# THE ONE PROGRAM, RUN TWICE. The environment below mirrors the CI
# workflow exactly -- same APP_ENV, same shape of DATABASE_URL -- so
# that a difference in the result means a difference in the MACHINE and
# nothing else. APP_ENV=ci on a server reads oddly,
# and it is deliberate: the alternative (development) would make the two
# runs two different programs, which is the one thing this must not be.
#
# NO CHANNEL FENCE HERE, on purpose: the container's own env carries
# the live bot token, and the fence against it lives in the suite
# itself (tests/conftest.py blanks every channel key before settings
# are built, and a tripwire fails any real Telegram request) -- so it
# holds wherever pytest runs, not only where someone remembered a flag.
#
# NO MIGRATIONS HERE. The suite brings the schema up itself
# (tests/conftest.py shells `alembic upgrade head` in a session
# fixture), so running it here too would be a second copy of a step that
# already has an owner.
#
# NO REDIS EITHER, and this was checked rather than assumed: every test
# that speaks to a stream does it over fakeredis, in-process. The suite
# never opens a connection to comms-redis.
cmd_test() {
    echo -e "${CYAN}== comms test ==${NC}"
    cd_compose
    load_env

    # The app's own URL is the source of truth for credentials and host;
    # only the database NAME is swapped. `%` strips the SHORTEST
    # matching suffix, so a password containing "comms" survives intact.
    local app_db_url test_db_url
    app_db_url=$($COMPOSE_CMD exec -T comms-app printenv DATABASE_URL | tr -d '\r\n')
    if [ -z "$app_db_url" ]; then
        echo -e "${RED}✗ Could not read DATABASE_URL from comms-app (is the stack up? try '$0 start')${NC}"
        exit 1
    fi
    test_db_url="${app_db_url%/${POSTGRES_DB}}/${POSTGRES_DB}_test"

    echo "Provisioning isolated test database (${POSTGRES_DB}_test)..."
    # FORCE terminates connections left by a previous run (PG13+), so a
    # second run in a row does not depend on the first having exited
    # cleanly.
    if ! $COMPOSE_CMD exec -T comms-postgres psql -U "$POSTGRES_USER" -d postgres \
        -c "DROP DATABASE IF EXISTS ${POSTGRES_DB}_test WITH (FORCE);" \
        -c "CREATE DATABASE ${POSTGRES_DB}_test OWNER $POSTGRES_USER;" >/dev/null; then
        echo -e "${RED}✗ Could not (re)create ${POSTGRES_DB}_test${NC}"
        exit 1
    fi

    echo "Running the suite inside comms-app..."
    if ! $COMPOSE_CMD exec -T \
        -e DATABASE_URL="$test_db_url" \
        -e APP_ENV=ci \
        comms-app python -m pytest -q; then
        echo -e "${RED}✗ tests failed${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ suite passed against ${POSTGRES_DB}_test${NC}"
}

cmd_status() {
    cd_compose
    $COMPOSE_CMD ps
}

# ------------------------------------------------------------------------------
# Dispatch
# ------------------------------------------------------------------------------

# A driving product CLI discovers what this service implements by
# reading the labels of THIS case -- the one at column zero. Labels of
# the nested case inside cmd_db (dump/restore/migrate) are arguments to
# `db`, not verbs of the service, and are correctly not seen. Keep new
# lifecycle verbs here, in this block: a verb added anywhere else is
# invisible to the product, and the product will keep reporting that
# this service cannot do it. Products parse these labels with a regular
# expression: the verbs are the case at column zero, and EVERY nested
# case in this file is indented (cmd_db, cmd_drain) so its labels are
# never read as verbs.
case "${1:-}" in
    install) shift; cmd_install "$@" ;;
    update)  shift; cmd_update "$@" ;;
    start)   shift; cmd_start "$@" ;;
    stop)    shift; cmd_stop "$@" ;;
    restart) shift; cmd_restart "$@" ;;
    logs)    shift; cmd_logs "$@" ;;
    db)      shift; cmd_db "$@" ;;
    test)    shift; cmd_test "$@" ;;
    status)  shift; cmd_status "$@" ;;
    drain)   shift; cmd_drain "$@" ;;
    *)
        echo "Usage: $0 {install|update|start|stop|restart|logs [service]|db {dump|restore <file>|migrate}|test|status|drain [--apply]}"
        exit 1
        ;;
esac
