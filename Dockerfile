# =============================================================================
# COMMS Service -- Dockerfile
# =============================================================================
#
# MULTI-STAGE BUILD (cbshome/velo pattern, trimmed -- no cairo/pdf deps):
#   Stage 1 ("builder"): installs ALL dependencies (prod + dev) into a venv.
#   Stage 2 ("runtime"): copies venv + app code + tests + migrations.
#
# Dev dependencies (pytest, ruff, mypy, httpx) are ALWAYS installed
# because tests run inside this container on the VPS.
#
# ONE IMAGE, THREE PROCESSES (handoff item 1; deploy/ Phase 5):
#   API:      default CMD (uvicorn on :8000)
#   Worker:   same image, different command:
#               python -m app.worker
#   Consumer: same image, different command:
#               python -m app.consumer
#           (both set via docker-compose `command:` in deploy/)
#
# PYTHONPATH=/app ensures `from app.core.config import settings` works
# everywhere: uvicorn, alembic, pytest -- without `pip install -e .`.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Builder -- install all dependencies
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

COPY pyproject.toml requirements.lock ./

# Install DEPENDENCIES ONLY -- the project itself is never pip-installed
# (runtime imports the `app` package via PYTHONPATH, see below; that is
# this file's own stated design). The previous form -- `pip install
# ".[dev]"`, copied from the velo donor -- installs the PROJECT, which
# needs the `app/` sources that are deliberately absent from this stage:
# it died with "package directory 'app' does not exist" on the first real
# build (2026-07-27). The donor survives the same pattern only by
# accident: velo's pyproject has no [tool.setuptools] packages
# declaration, so setuptools silently builds an EMPTY distribution from a
# bare pyproject; comms declares `packages = ["app"]`, which turns that
# accident into a hard error. Extracting the dependency list keeps this
# layer keyed on pyproject.toml alone -- source changes do not reinstall
# dependencies, which is the whole point of the two-stage split.
#
# FROM THE LOCK, NOT FROM THE RANGES. pyproject states ranges, so
# extracting its dependency list here -- which is what this layer used
# to do -- resolved them afresh on every build: the image built today
# and the image built next month carried different versions of every
# transitive package, and nothing recorded which. requirements.lock is
# the resolution itself, regenerated deliberately (see its header) in
# the same commit as any dependency change.
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.lock

# ---------------------------------------------------------------------------
# Stage 2: Runtime
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

WORKDIR /app

# Non-root user for security.
RUN groupadd --gid 1000 comms && \
    useradd --uid 1000 --gid comms --shell /bin/sh comms

COPY --from=builder /opt/venv /opt/venv

# Copy everything the service needs.
COPY app/ ./app/
COPY tests/ ./tests/
COPY pyproject.toml ./
COPY alembic.ini ./
COPY migrations/ ./migrations/

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# CRITICAL: alembic, pytest, and CLI tools must find the `app` package.
ENV PYTHONPATH="/app"

USER comms
EXPOSE 8000

CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2", \
     "--proxy-headers"]
