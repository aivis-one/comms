# =============================================================================
# W1-2 -- the API's command: one source, no migration, uvicorn's own lines
# in the machine's form
# =============================================================================
#
# Item 2 of W1-2:
#   the command  comms-app has no `command:` in deploy/docker-compose.yml;
#                it runs the image's CMD (Dockerfile), the one source --
#                uvicorn with --log-config, and no alembic: the schema is
#                the deploy machine's step `migrate` (primer.yaml);
#   the config   app/core/log_config.json names, for uvicorn's loggers, a
#                formatter whose module imports nothing of comms: uvicorn
#                configures logging in every worker BEFORE the thread that
#                answers its supervisor's healthcheck starts, and the
#                settings imported there took seconds -- a worker killed
#                as hung (app/core/log_launcher.py);
#   the lines    the image's command, run as it is (its host and port
#                aside), with --workers 2: every line of the supervisor
#                and of each worker, from the first to the last, is the
#                line of protocol 9.4 with the release.
#
# MUTATIONS (one run, each red here):
#   M1 `sh -c "alembic upgrade head && exec uvicorn ..."` as the CMD
#                                               -> TestTheCommand
#   M2 a `command:` for comms-app back in compose  -> TestTheCommand
#   M3 the config's formatter uvicorn's own        -> TestTheLines
#   M4 the config's formatter SanitizingFormatter  -> TestTheConfig
# =============================================================================

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from app.core.logging import LOG_CTX, LOG_FIELDS, RELEASE_VARIABLE
from tests.conftest import FIXTURE_PROFILE_DIR, REPO_ROOT
from tests.helpers import image_command, service_command

_COMPOSE = REPO_ROOT / "deploy" / "docker-compose.yml"
_RELEASE = "w1-2-probe"


def _services() -> dict[str, Any]:
    services: dict[str, Any] = yaml.safe_load(
        _COMPOSE.read_text(encoding="utf-8"),
    )["services"]
    return services


def _option(command: list[str], name: str) -> str:
    assert command.count(name) == 1, f"{name} not once in {command}"
    return command[command.index(name) + 1]


class TestTheCommand:
    def test_comms_app_runs_the_images_command(self) -> None:
        """M2. The pair: the worker and the consumer keep their own."""
        services = _services()
        assert "command" not in services["comms-app"]
        assert service_command(services["comms-app"]) == image_command()
        assert service_command(services["comms-worker"])[-1] == "app.worker"
        assert service_command(services["comms-consumer"])[-1] == "app.consumer"

    def test_no_migration_at_the_start(self) -> None:
        """M1. The pair: the command is read, and it is uvicorn's."""
        command = image_command()
        assert command[:2] == ["uvicorn", "app.main:app"]
        assert [part for part in command if "alembic" in part] == []

    def test_the_log_config_is_in_the_image(self) -> None:
        """Relative to WORKDIR /app, where the runtime stage copies app/
        (COPY app/ ./app/): the path exists in the tree under app/."""
        path = _option(image_command(), "--log-config")
        assert path.startswith("app/") and path.endswith(".json"), path
        assert (REPO_ROOT / path).is_file()
        dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
        assert "WORKDIR /app" in dockerfile and "COPY app/ ./app/" in dockerfile

    def test_the_reader_of_cmd_sees_every_form(self) -> None:
        """The helper's own twin: continuations joined, the last CMD
        taken, a shell form refused."""
        text = 'FROM x\nCMD ["a"]\nCMD ["uvicorn", \\\n     "b"]\n'
        assert image_command(text) == ["uvicorn", "b"]
        with pytest.raises(json.JSONDecodeError):
            image_command("CMD uvicorn app.main:app\n")


class TestTheConfig:
    def test_configuring_logging_imports_nothing_of_comms(self) -> None:
        """M4. What uvicorn does in a worker before its healthcheck
        answers -- dictConfig of the file -- in a clean interpreter: the
        modules of comms it leaves loaded are the formatter's alone. The
        pair: the config names that formatter for uvicorn's loggers."""
        path = _option(image_command(), "--log-config")
        config = json.loads((REPO_ROOT / path).read_text(encoding="utf-8"))
        (formatter,) = config["formatters"].values()
        assert formatter == {"()": "app.core.log_launcher.LauncherFormatter"}
        assert set(config["loggers"]) == {"uvicorn", "uvicorn.error", "uvicorn.access"}
        probe = (
            "import json, logging.config, sys\n"
            f"logging.config.dictConfig(json.load(open({path!r})))\n"
            "print(json.dumps(sorted(m for m in sys.modules\n"
            "                        if m == 'app' or m.startswith('app.'))))\n"
        )
        done = subprocess.run(
            [sys.executable, "-c", probe], cwd=REPO_ROOT, capture_output=True,
            text=True, timeout=60, check=False,
        )
        assert done.returncode == 0, done.stderr
        assert json.loads(done.stdout) == ["app", "app.core", "app.core.log_launcher"]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def _run_the_image_command(tmp_path: Path) -> str:
    """The CMD as it is but its host and port, until both workers have
    started (or 60 s), then SIGTERM, as `docker stop` sends it; the
    output of the supervisor and the workers, stdout and stderr in one."""
    command = image_command()
    command[command.index("--host") + 1] = "127.0.0.1"
    command[command.index("--port") + 1] = str(_free_port())
    assert command[0] == "uvicorn"
    env = {
        **os.environ, RELEASE_VARIABLE: _RELEASE,
        "TEMPLATES_DIR": str(FIXTURE_PROFILE_DIR), "TEMPLATES_OVERLAY_DIR": "",
    }
    out = tmp_path / "out"
    with out.open("w") as sink:
        process = subprocess.Popen(
            [sys.executable, "-m", *command], cwd=REPO_ROOT, env=env,
            stdout=sink, stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and process.poll() is None:
                if out.read_text().count("Application startup complete.") == 2:
                    break
                time.sleep(0.2)
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=30)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    return out.read_text()


class TestTheLines:
    def test_every_line_of_the_api_is_the_machines(self, tmp_path: Path) -> None:
        """M3. Every line, the supervisor's first to its last, is one JSON
        object of the seven fields (and `ctx` when it has any) with the
        release. The pair: the lines that were outside the form before
        W1-2 are among them -- the supervisor's, each worker's before its
        lifespan -- and no worker was killed as hung."""
        output = _run_the_image_command(tmp_path)
        lines = [line for line in output.splitlines() if line.strip()]
        assert lines, "the pair: the process wrote lines"
        parsed = []
        for line in lines:
            try:
                parsed.append(json.loads(line))
            except json.JSONDecodeError:
                pytest.fail(f"a line outside the form: {line!r}")
        for line in parsed:
            assert set(line) - {LOG_CTX} == set(LOG_FIELDS), line
            assert line["release"] == _RELEASE, line
        messages = [line["msg"] for line in parsed]
        assert any(m.startswith("Started parent process") for m in messages), messages
        assert any(m.startswith("Stopping parent process") for m in messages), messages
        assert messages.count("Waiting for application startup.") == 2, messages
        assert messages.count("Application startup complete.") == 2, messages
        assert [m for m in messages if m.endswith(" died")] == [], messages
