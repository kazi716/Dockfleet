from unittest.mock import MagicMock, patch

import pytest

from dockfleet.core.logs import stream_container_logs, stream_logs
from dockfleet.core.orchestrator import get_container_name


def test_get_container_name():
    """Helper works."""
    assert get_container_name("api") == "dockfleet_api"


# -------------------------------------------------------
# Pre-existing tests for stream_logs (sync generator)
# -------------------------------------------------------


@patch("dockfleet.core.logs.subprocess.run")
def test_stream_logs_container_missing(mock_run):
    """Container not found → error list."""
    mock_run.return_value = MagicMock(returncode=0, stdout="")

    events = list(stream_logs("missing"))
    assert len(events) == 0
    mock_run.assert_called_once()


@patch("dockfleet.core.logs.subprocess.run")
def test_stream_logs_docker_check_fails(mock_run):
    """Docker ps fails → error list."""
    mock_run.side_effect = Exception("Docker error")

    events = list(stream_logs("api"))
    assert len(events) == 0


@patch("dockfleet.core.logs.subprocess.Popen")
@patch("dockfleet.core.logs.subprocess.run")
def test_stream_logs_streaming(mock_run, mock_popen):
    """Container exists → streams log lines."""
    mock_run.side_effect = [
        MagicMock(returncode=0, stdout="dockfleet_test\n"),
    ]

    mock_popen.return_value.stdout = iter(
        [
            "log line 1\n",
            "log line 2\n",
        ]
    )
    mock_popen.return_value.terminate = MagicMock()
    mock_popen.return_value.wait = MagicMock(return_value=0)

    events = list(stream_logs("test"))
    assert len(events) >= 1


# -------------------------------------------------------
# Tests for stream_container_logs exception handling
# -------------------------------------------------------


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_container_not_found_fails_fast(mock_popen, mock_store):
    """Container doesn't exist -> fails fast, does not consume all retries."""
    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(
        side_effect=["Error: No such container: dockfleet_missing\n", ""]
    )
    mock_proc.stderr.read = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=1)
    mock_proc.returncode = 1
    mock_proc.terminate = MagicMock()
    mock_popen.return_value = mock_proc

    events = []
    async for event in stream_container_logs("missing"):
        events.append(event)

    assert len(events) >= 1
    assert "not found" in events[-1].lower()
    assert "missing" in events[-1].lower()
    mock_popen.assert_called_once()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_docker_binary_missing_fails_fast(mock_popen, mock_store):
    """Docker binary not found -> fails fast with appropriate message."""
    mock_popen.side_effect = FileNotFoundError("docker binary not found")

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) == 1
    assert "docker" in events[0].lower()
    assert "not installed" in events[0].lower() or "not in path" in events[0].lower()
    mock_popen.assert_called_once()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_permission_denied_fails_fast(mock_popen, mock_store):
    """Permission denied -> fails fast."""
    mock_popen.side_effect = PermissionError("[Errno 13] Permission denied")

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) == 1
    assert "permission" in events[0].lower()
    mock_popen.assert_called_once()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_transient_failure_recovers(mock_popen, mock_store):
    """Transient failure that resolves -> recovers and streams."""
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        mock_proc = MagicMock()
        if call_count <= 2:
            mock_proc.stdout.readline = MagicMock(return_value="")
            mock_proc.stderr.read = MagicMock(return_value="Connection refused\n")
            mock_proc.wait = MagicMock(return_value=1)
            mock_proc.returncode = 1
        else:
            mock_proc.stdout.readline = MagicMock(
                side_effect=["log line 1\n", "log line 2\n", ""]
            )
            mock_proc.stderr.read = MagicMock(return_value="")
            mock_proc.wait = MagicMock(return_value=0)
            mock_proc.returncode = 0
        mock_proc.terminate = MagicMock()
        return mock_proc

    mock_popen.side_effect = side_effect

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)
        if len(events) >= 2:
            break

    assert len(events) >= 2
    assert "log line 1" in events[0]
    assert "log line 2" in events[1]
    assert mock_popen.call_count == 3


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_all_exception_paths_produce_logs(mock_popen, mock_store, caplog):
    """Every exception path produces a log entry."""
    import logging

    mock_popen.side_effect = FileNotFoundError("docker not found")

    with caplog.at_level(logging.ERROR, logger="dockfleet.core.logs"):
        events = []
        async for event in stream_container_logs("api"):
            events.append(event)
            break

    assert "docker" in caplog.text.lower() or "not found" in caplog.text.lower()

    mock_popen.side_effect = PermissionError("Permission denied")
    caplog.clear()

    with caplog.at_level(logging.ERROR, logger="dockfleet.core.logs"):
        events = []
        async for event in stream_container_logs("api"):
            events.append(event)
            break

    assert "permission" in caplog.text.lower()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_happy_path_unaffected(mock_popen, mock_store):
    """Successful streaming works as before - no regression."""
    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(
        side_effect=["log line 1\n", "log line 2\n", ""]
    )
    mock_proc.stderr.read = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.returncode = 0
    mock_proc.terminate = MagicMock()
    mock_popen.return_value = mock_proc

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)
        if len(events) >= 2:
            break

    assert len(events) >= 2
    assert "log line 1" in events[0]
    assert "log line 2" in events[1]
    assert all("data: " in event for event in events)
    assert all("\n\n" in event for event in events)


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_max_retries_exhausted_message(mock_popen, mock_store):
    """Max retries exhausted -> appropriate message, transient error retried."""
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        mock_proc = MagicMock()
        mock_proc.stdout.readline = MagicMock(return_value="")
        mock_proc.stderr.read = MagicMock(return_value="Connection refused\n")
        mock_proc.wait = MagicMock(return_value=1)
        mock_proc.returncode = 1
        mock_proc.terminate = MagicMock()
        return mock_proc

    mock_popen.side_effect = side_effect

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert any("max retries" in e.lower() or "exhausted" in e.lower() for e in events)
    assert mock_popen.call_count == 20


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_proc_none_cleanup_safety(mock_popen, mock_store):
    """Exception before Popen succeeds -> cleanup handles None proc safely."""
    mock_popen.side_effect = Exception("Unexpected error")

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) == 1
    assert "error" in events[0].lower() or "unexpected" in events[0].lower()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_no_such_image_fails_fast(mock_popen, mock_store):
    """No such image pattern in stderr -> permanent failure."""
    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(
        side_effect=["Error: No such image: dockfleet_api:latest\n", ""]
    )
    mock_proc.stderr.read = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=1)
    mock_proc.returncode = 1
    mock_proc.terminate = MagicMock()
    mock_popen.return_value = mock_proc

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) >= 1
    assert "not found" in events[-1].lower()
    mock_popen.assert_called_once()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_permission_denied_in_stderr_fails_fast(mock_popen, mock_store):
    """Permission denied in stderr -> fails fast without retrying 20 times."""
    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(
        side_effect=["permission denied: docker socket access required\n", ""]
    )
    mock_proc.wait = MagicMock(return_value=1)
    mock_proc.returncode = 1
    mock_proc.terminate = MagicMock()
    mock_proc.poll = MagicMock(return_value=0)
    mock_popen.return_value = mock_proc

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) >= 1
    assert "permission denied" in events[-1].lower()
    mock_popen.assert_called_once()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_unknown_non_zero_exit_fails_fast(mock_popen, mock_store):
    """Unknown non-zero exit code -> generic diagnostic, fails fast."""
    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(
        side_effect=["fatal error: unexpected internal crash\n", ""]
    )
    mock_proc.wait = MagicMock(return_value=1)
    mock_proc.returncode = 1
    mock_proc.terminate = MagicMock()
    mock_proc.poll = MagicMock(return_value=0)
    mock_popen.return_value = mock_proc

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) >= 1
    assert (
        "error streaming logs" in events[-1].lower()
        or "exited with code 1" in events[-1].lower()
    )
    mock_popen.assert_called_once()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_db_persistence_failure_isolated(mock_popen, mock_store):
    """Database persistence exception -> stream continues, error is logged."""
    mock_store.side_effect = Exception("Database connection failure")

    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(
        side_effect=["log line 1\n", "log line 2\n", ""]
    )
    mock_proc.stderr.readline = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.returncode = 0
    mock_proc.terminate = MagicMock()
    mock_proc.poll = MagicMock(return_value=0)
    mock_popen.return_value = mock_proc

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) == 2
    assert "log line 1" in events[0]
    assert "log line 2" in events[1]


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_cleanup_failure_logged(mock_popen, mock_store, caplog):
    """Cleanup failure when terminating/killing process -> logs warning with exc_info."""
    import logging

    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(return_value="")
    mock_proc.stderr.readline = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.returncode = 0
    mock_proc.poll = MagicMock(return_value=None)
    mock_proc.terminate = MagicMock(side_effect=Exception("Terminate failed"))
    mock_proc.kill = MagicMock(side_effect=Exception("Kill failed"))
    mock_popen.return_value = mock_proc

    with caplog.at_level(logging.WARNING, logger="dockfleet.core.logs"):
        events = []
        async for event in stream_container_logs("api"):
            events.append(event)

    assert "failed to kill process" in caplog.text.lower()


@pytest.mark.asyncio
@patch("dockfleet.core.logs.store_log_line_in_db")
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_reader_exception_propagation(mock_popen, mock_store, caplog):
    """Exception during stdout readline -> logs exception with stack trace and yields error notification."""
    import logging

    mock_proc = MagicMock()
    mock_proc.stdout.readline = MagicMock(side_effect=OSError("Read error"))
    mock_proc.stderr.readline = MagicMock(return_value="")
    mock_proc.wait = MagicMock(return_value=0)
    mock_proc.returncode = 0
    mock_proc.terminate = MagicMock()
    mock_proc.poll = MagicMock(return_value=0)
    mock_popen.return_value = mock_proc

    with caplog.at_level(logging.ERROR, logger="dockfleet.core.logs"):
        events = []
        async for event in stream_container_logs("api"):
            events.append(event)

    assert events == [
        "data: [dockfleet] Error reading logs for 'dockfleet_api': Read error\n\n"
    ]
    assert any(
        record.name == "dockfleet.core.logs"
        and record.levelno == logging.ERROR
        and record.getMessage() == "Stdout reader expected exception for dockfleet_api"
        and record.exc_info is not None
        for record in caplog.records
    )


@pytest.mark.asyncio
@patch("dockfleet.core.logs.subprocess.Popen")
async def test_popen_spawn_failure_loop_bound_safely(mock_popen):
    """When subprocess.Popen fails to spawn, loop is bound safely and does not raise UnboundLocalError."""
    mock_popen.side_effect = FileNotFoundError("No docker executable found")

    events = []
    async for event in stream_container_logs("api"):
        events.append(event)

    assert len(events) == 1
    assert "Docker is not installed or not in PATH" in events[0]

