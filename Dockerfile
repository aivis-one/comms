# =============================================================================
# COMMS Service -- Dockerfile
# =============================================================================
#
# THREE STAGES (W1-1), named so because the deploy machine builds them by
# name:
#   builder  installs the RUNTIME dependencies (requirements.lock) into a
#            venv; nothing else.
#   test     the builder's venv plus the dev dependencies
#            (requirements-dev.lock), and the whole tree: the suite is run
#            from here -- by CI's `image` job and by the machine, which
#            builds `--target test` from the same release and runs the
#            suite in a one-off container. It carries everything the suite
#            reads (tests/, deploy/, scripts/, .github/, the *.md files),
#            so nothing is mounted over it.
#   runtime  the builder's venv, the service's code and its migrations;
#            the LAST stage, so a build without --target (compose, the
#            machine's service build) gives it. No test tool is in it:
#            pytest, ruff, mypy and fakeredis are the test stage's only
#            (tests/test_image_stages.py holds both sides).
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
#
# THE MACHINE'S TWO ARGUMENTS (W1-1): PRIMER_RELEASE (a tag or a branch)
# and PRIMER_COMMIT (a sha) are build arguments the machine passes to every
# build; each stage that runs a process of comms -- runtime and test -- takes
# them as ARG and hands them to the process as ENV. No default and no
# refusal here: a build without them carries them empty, and the process
# that reads them refuses an empty value at its start. CI passes them too
# (.github/workflows/ci.yml, job `image`).
# =============================================================================

# THE BASE IMAGE IS PINNED BY DIGEST (H10), the tag kept for the reader:
# a tag moves -- python:3.12-slim moved to Debian 13 under every build that
# pulled it -- and only the digest says which image a build gets. All three
# stages name the same one (tests/test_image_pins.py holds that).

# ---------------------------------------------------------------------------
# Stage 1: builder -- the runtime dependencies
# ---------------------------------------------------------------------------
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS builder

WORKDIR /build

COPY requirements.lock ./

# Install DEPENDENCIES ONLY -- the project itself is never pip-installed
# (the stages import the `app` package via PYTHONPATH, see below). The
# form `pip install ".[dev]"`, copied from the velo donor, installed the
# PROJECT, which needs the `app/` sources deliberately absent from this
# stage: it died with "package directory 'app' does not exist" on the
# first real build (2026-07-27). This layer is keyed on the lock alone --
# source changes do not reinstall dependencies.
#
# FROM THE LOCK, NOT FROM THE RANGES: requirements.lock is the resolution
# itself (how it is made: pyproject.toml, above [project.optional-
# dependencies]). --require-hashes is written out: every artifact is
# checked against the lock's hashes, and a lock that lost them fails here
# instead of installing unchecked.
#
# NO `pip install --upgrade pip` (TX-4): it fetched whatever pip was
# newest on the day of the build -- the one unpinned download of the
# image. The venv's pip is the base image's, pinned by its digest above.
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --require-hashes -r requirements.lock

# ---------------------------------------------------------------------------
# Stage 2: test -- the runtime's venv, the dev dependencies, the whole tree
# ---------------------------------------------------------------------------
# Built FROM the base, not from runtime (runtime is the last stage), so it
# repeats what runtime sets -- user, WORKDIR, the four ENV, the machine's
# two arguments -- and the suite runs in the environment the service runs
# in (tests/test_image_stages.py holds the two alike). It sets no CMD and
# no EXPOSE: the machine gives the suite's command, CI its own.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS test

WORKDIR /app

RUN groupadd --gid 1000 comms && \
    useradd --uid 1000 --gid comms --shell /bin/sh comms

# The runtime's packages, the very files, then the dev ones on top: what
# both locks pin is already installed at the same version and is kept.
COPY --from=builder /opt/venv /opt/venv
COPY requirements-dev.lock ./
RUN /opt/venv/bin/pip install --no-cache-dir --require-hashes -r requirements-dev.lock

# The whole build context: .dockerignore keeps out only .git, the .env
# files and caches (its header says why).
COPY . ./

ARG PRIMER_RELEASE
ARG PRIMER_COMMIT
ENV PRIMER_RELEASE=${PRIMER_RELEASE}
ENV PRIMER_COMMIT=${PRIMER_COMMIT}
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"

# Not root, as runtime: the tree belongs to root and is read, not written
# (the pytest cache and --junitxml go to /tmp).
USER comms

# ---------------------------------------------------------------------------
# Stage 3: runtime -- the service
# ---------------------------------------------------------------------------
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS runtime

WORKDIR /app

# Non-root user for security.
RUN groupadd --gid 1000 comms && \
    useradd --uid 1000 --gid comms --shell /bin/sh comms

COPY --from=builder /opt/venv /opt/venv

# Copy everything the service needs -- a white list: the build context
# holds the whole tree for the test stage, and nothing else of it comes
# here. NO tests/ (TX-4): the suite runs from the test stage.
COPY app/ ./app/
COPY pyproject.toml ./
COPY alembic.ini ./
COPY migrations/ ./migrations/

ARG PRIMER_RELEASE
ARG PRIMER_COMMIT
ENV PRIMER_RELEASE=${PRIMER_RELEASE}
ENV PRIMER_COMMIT=${PRIMER_COMMIT}
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# CRITICAL: alembic and CLI tools must find the `app` package.
ENV PYTHONPATH="/app"

USER comms
EXPOSE 8000

CMD ["uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2", \
     "--proxy-headers"]
