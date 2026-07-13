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
# ONE IMAGE, TWO PROCESSES (handoff item 1):
#   API:    default CMD (uvicorn on :8000)
#   Worker: same image, different command:
#             python -m app.worker
#           (set via docker-compose `command:` for the worker service)
#
# PYTHONPATH=/app ensures `from app.core.config import settings` works
# everywhere: uvicorn, alembic, pytest -- without `pip install -e .`.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Builder -- install all dependencies
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

COPY pyproject.toml ./

RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir ".[dev]"

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
