from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from dockfleet.cli.main import app

runner = CliRunner()


def test_cli_validate_success():
    result = runner.invoke(app, ["validate", "examples/dockfleet.yaml"])
    assert result.exit_code == 0
    assert "Config valid" in result.stdout


def test_cli_validate_invalid_schema(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("""
services:
  web:
    image: nginx
    resources:
      cpu: -1.0
""")
    result = runner.invoke(app, ["validate", str(bad_config)])
    assert result.exit_code == 1
    assert "Unexpected error:" not in result.stdout
    assert "Configuration Validation Error" in result.output


def test_cli_validate_out_of_range_port(tmp_path):
    bad_config = tmp_path / "bad_port.yaml"
    bad_config.write_text("""
services:
  web:
    image: nginx
    restart: always
    ports:
      - "80:70000"
""")
    result = runner.invoke(app, ["validate", str(bad_config)])
    assert result.exit_code == 1
    assert "Configuration Validation Error" in result.output
    assert "Port values must be between 1 and 65535" in result.output



@patch("dockfleet.cli.main.Orchestrator.restart")
def test_cli_restart(mock_restart):
    """Test that the restart command executes successfully without crashing."""
    result = runner.invoke(app, ["restart", "examples/dockfleet.yaml"])
    assert result.exit_code == 0
    assert "Restarting services from" in result.stdout
    mock_restart.assert_called_once()


@patch("dockfleet.cli.main.Orchestrator.restart")
def test_cli_restart_failure(mock_restart):
    """Test that the restart command handles and exits with code 1."""
    mock_restart.side_effect = RuntimeError("Failed to stop services")
    result = runner.invoke(app, ["restart", "examples/dockfleet.yaml"])
    assert result.exit_code == 1
    assert "Error restarting services" in result.stdout


@patch("dockfleet.core.orchestrator.mark_service_stopped")
@patch("dockfleet.core.docker.DockerManager.remove_container")
@patch("dockfleet.core.docker.DockerManager.stop_container")
@patch("dockfleet.core.orchestrator.Orchestrator.up")
def test_cli_restart_absent_container(mock_up, mock_stop, mock_remove, mock_mark):
    """Regression test: restart proceeds when the configured container does not exist."""
    # Simulate Docker throwing a "No such container" error during down()
    mock_stop.side_effect = Exception("Error: No such container: dockfleet_api")

    # Run the restart command
    result = runner.invoke(app, ["restart", "examples/dockfleet.yaml"])

    # Ensure it didn't crash and successfully reached up()
    assert result.exit_code == 0
    mock_up.assert_called_once()


@patch("dockfleet.cli.main.importlib.metadata.version")
def test_cli_version(mock_version):
    """Test that the --version option outputs the version and exits."""
    mock_version.return_value = "1.2.3"
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "DockFleet version 1.2.3" in result.stdout


@patch("dockfleet.cli.main.spawn_background_scheduler")
@patch("dockfleet.cli.main.SchedulerLock._pid_is_running", return_value=False)
@patch("dockfleet.cli.main.SchedulerLock._read_pid_file", return_value=None)
@patch("dockfleet.core.orchestrator.Orchestrator.up")
@patch("dockfleet.cli.main.bootstrap_from_path")
def test_cli_up_detached(mock_bootstrap, mock_up, mock_read_pid, mock_pid_running, mock_spawn):
    """Test that dockfleet up in default detached mode launches background scheduler."""
    result = runner.invoke(app, ["up", "examples/dockfleet.yaml"])
    assert result.exit_code == 0
    assert "Starting services from" in result.stdout
    assert "Health scheduler started in background" in result.stdout
    mock_up.assert_called_once()
    mock_spawn.assert_called_once_with(str(Path("examples/dockfleet.yaml")))


@patch("dockfleet.cli.main.spawn_background_scheduler")
@patch("dockfleet.cli.main.SchedulerLock._pid_is_running", return_value=True)
@patch("dockfleet.cli.main.SchedulerLock._read_pid_file", return_value={"pid": 12345})
@patch("dockfleet.core.orchestrator.Orchestrator.up")
@patch("dockfleet.cli.main.bootstrap_from_path")
def test_cli_up_detached_already_running(mock_bootstrap, mock_up, mock_read_pid, mock_pid_running, mock_spawn):
    """Test that dockfleet up detects an existing running scheduler and does not spawn another."""
    result = runner.invoke(app, ["up", "examples/dockfleet.yaml"])
    assert result.exit_code == 0
    assert "Health scheduler is already running in background (PID 12345)" in result.stdout
    mock_up.assert_called_once()
    mock_spawn.assert_not_called()


@patch("dockfleet.cli.main.HealthScheduler")
@patch("dockfleet.core.orchestrator.Orchestrator.up")
@patch("dockfleet.cli.main.bootstrap_from_path")
@patch("dockfleet.cli.main.time.sleep", side_effect=KeyboardInterrupt)
def test_cli_up_foreground(mock_sleep, mock_bootstrap, mock_up, mock_scheduler_cls):
    """Test that dockfleet up --foreground runs scheduler in foreground until interrupted."""
    mock_scheduler = mock_scheduler_cls.return_value
    result = runner.invoke(app, ["up", "examples/dockfleet.yaml", "--foreground"])
    assert result.exit_code == 0
    assert "Running health scheduler in foreground" in result.stdout
    assert "Stopping health scheduler..." in result.stdout
    mock_up.assert_called_once()
    mock_scheduler.start.assert_called_once()
    mock_scheduler.stop.assert_called_once()


@patch("dockfleet.cli.main.stop_background_scheduler")
@patch("dockfleet.core.orchestrator.Orchestrator.down")
def test_cli_down_stops_scheduler(mock_down, mock_stop_scheduler):
    """Test that dockfleet down stops orchestrator services and any background scheduler."""
    result = runner.invoke(app, ["down", "examples/dockfleet.yaml"])
    assert result.exit_code == 0
    assert "Stopping services from" in result.stdout
    assert "Services stopped" in result.stdout
    mock_down.assert_called_once()
    mock_stop_scheduler.assert_called_once()


@patch("dockfleet.cli.main.subprocess.run")
def test_cli_logs_missing_container_follow(mock_run):
    """Test that dockfleet logs --follow outputs error and exits 1 when container is missing."""
    from unittest.mock import MagicMock

    mock_run.return_value = MagicMock(returncode=1, stderr="Error: No such container: dockfleet_invalid_service\n")
    result = runner.invoke(app, ["logs", "invalid_service", "--follow"])
    assert result.exit_code == 1
    assert "Service 'invalid_service' not found or container not running." in result.stdout
    assert "Streaming logs" not in result.stdout


@patch("dockfleet.cli.main.subprocess.run")
def test_cli_logs_missing_container_no_follow(mock_run):
    """Test that dockfleet logs outputs error and exits 1 when container is missing."""
    from unittest.mock import MagicMock

    mock_run.return_value = MagicMock(returncode=1, stderr="Error: No such container: dockfleet_invalid_service\n")
    result = runner.invoke(app, ["logs", "invalid_service"])
    assert result.exit_code == 1
    assert "Service 'invalid_service' not found or container not running." in result.stdout


@patch("dockfleet.cli.main.subprocess.run")
def test_cli_logs_success_follow(mock_run):
    """Test that dockfleet logs --follow streams logs when container exists."""
    from unittest.mock import MagicMock

    mock_run.return_value = MagicMock(returncode=0, stdout="")
    result = runner.invoke(app, ["logs", "web", "--follow"])
    assert result.exit_code == 0
    assert "Streaming logs for web" in result.stdout


@patch("dockfleet.cli.main.subprocess.run")
def test_cli_logs_success_no_follow(mock_run):
    """Test that dockfleet logs outputs logs when container exists."""
    from unittest.mock import MagicMock

    mock_run.return_value = MagicMock(returncode=0, stdout="Application started successfully\n")
    result = runner.invoke(app, ["logs", "web"])
    assert result.exit_code == 0
    assert "Application started successfully" in result.stdout


