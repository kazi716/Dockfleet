from sqlmodel import select

from dockfleet.health.logs import store_log_line
from dockfleet.health.models import LogEvent, Service, get_session, init_db


def setup_function(_func):
    # Fresh tables for each test (simple version)
    init_db()
    with get_session() as session:
        session.exec(select(LogEvent)).all()  # ensure table exists
        session.exec(select(Service)).all()
        session.query(LogEvent).delete()
        session.query(Service).delete()
        session.commit()


def test_store_log_and_filter_by_service():
    # Arrange: create one service
    with get_session() as session:
        svc = Service(
            name="api",
            image="dummy-image",
            restart_policy="always",
        )
        session.add(svc)
        session.commit()

    # Act: store two log lines
    store_log_line("api", "Service started", level="INFO", source="test")
    store_log_line("api", "Request failed with 500", level="ERROR", source="test")

    # Assert: logs present and filterable by service_name
    with get_session() as session:
        rows = session.query(LogEvent).filter(LogEvent.service_name == "api").all()

    assert len(rows) >= 2
    messages = [row.message for row in rows]
    assert any("Service started" in m for m in messages)
    assert any("Request failed" in m for m in messages)


def test_store_log_skips_unknown_service():
    # Act: call store_log_line with unknown service
    store_log_line(
        "unknown-service", "Should not be stored", level="INFO", source="test"
    )

    # Assert: no LogEvent rows for that name
    with get_session() as session:
        rows = (
            session.query(LogEvent)
            .filter(LogEvent.service_name == "unknown-service")
            .all()
        )

    assert len(rows) == 0


def test_ingest_docker_logs_once_initial_and_incremental(monkeypatch):
    from unittest.mock import MagicMock, patch

    from dockfleet.health.log_ingestor import ingest_docker_logs_once

    with get_session() as session:
        svc = Service(
            name="api",
            image="dummy-image",
            restart_policy="always",
        )
        session.add(svc)
        session.commit()

    recorded_cmds = []

    def mock_subprocess_popen(cmd, *args, **kwargs):
        recorded_cmds.append(cmd)
        mock_process = MagicMock()
        mock_process.wait.return_value = 0
        mock_stdout = MagicMock()
        
        if "--tail" in cmd:
            mock_stdout.__iter__.return_value = ["2026-10-02T22:21:32.123456789Z line 1\n", "2026-10-02T22:21:33.123456789Z line 2\n"]
        elif "--since" in cmd:
            mock_stdout.__iter__.return_value = ["2026-10-02T22:21:34.123456789Z line 3\n"]
        else:
            mock_stdout.__iter__.return_value = []
            
        mock_process.stdout = mock_stdout
        return mock_process

    with patch("subprocess.Popen", side_effect=mock_subprocess_popen):
        # 1. Initial ingest (no prior logs) -> should use --tail
        ingest_docker_logs_once(tail=200)

        with get_session() as session:
            rows = session.exec(
                select(LogEvent).where(LogEvent.service_name == "api")
            ).all()
            assert len(rows) == 2
            messages = [r.message for r in rows]
            assert messages == ["line 1", "line 2"]

        assert len(recorded_cmds) == 1
        assert recorded_cmds[0][:5] == ["docker", "logs", "--timestamps", "--tail", "200"]
        assert recorded_cmds[0][-1] == "dockfleet_api"

        # 2. Subsequent ingest -> should use --since with latest_ts isoformat
        ingest_docker_logs_once(tail=200)

        with get_session() as session:
            rows = session.exec(
                select(LogEvent)
                .where(LogEvent.service_name == "api")
                .order_by(LogEvent.created_at)
            ).all()
            assert len(rows) == 3
            messages = [r.message for r in rows]
            assert messages == ["line 1", "line 2", "line 3"]

        assert len(recorded_cmds) == 2
        assert recorded_cmds[1][:3] == ["docker", "logs", "--timestamps"]
        assert recorded_cmds[1][3] == "--since"
        assert recorded_cmds[1][-1] == "dockfleet_api"

def test_ingest_docker_logs_batching():
    from unittest.mock import MagicMock, patch

    from sqlmodel import Session

    from dockfleet.health.log_ingestor import ingest_docker_logs_once

    with get_session() as session:
        session.exec(select(LogEvent)).all()
        session.exec(select(Service)).all()
        session.query(LogEvent).delete()
        session.query(Service).delete()
        
        svc = Service(
            name="api",
            image="dummy-image",
            restart_policy="always",
        )
        session.add(svc)
        session.commit()

    def mock_subprocess_popen(cmd, *args, **kwargs):
        mock_process = MagicMock()
        mock_process.wait.return_value = 0
        mock_stdout = MagicMock()
        mock_stdout.__iter__.return_value = [f"2026-10-02T22:21:32.123456789Z line {i}\n" for i in range(2001)]
        mock_process.stdout = mock_stdout
        return mock_process

    original_commit = Session.commit

    with patch("subprocess.Popen", side_effect=mock_subprocess_popen):
        with patch.object(Session, "commit", autospec=True, side_effect=original_commit) as mock_commit:
            ingest_docker_logs_once(tail=2001)

            # 2001 logs -> two 1,000-event batch commits + one 1-event remainder commit
            # (there may also be an outer commit from the session context manager, so >= 3)
            assert mock_commit.call_count >= 3

        with get_session() as session:
            rows = session.exec(select(LogEvent)).all()
            assert len(rows) == 2001


def test_ingest_docker_logs_nonzero_exit_discards_streamed_output():
    from unittest.mock import MagicMock, patch

    from sqlmodel import select

    from dockfleet.health.log_ingestor import ingest_docker_logs_once

    with get_session() as session:
        session.exec(select(LogEvent)).all()
        session.exec(select(Service)).all()
        session.query(LogEvent).delete()
        session.query(Service).delete()
        session.add(
            Service(
                name="api",
                image="dummy-image",
                restart_policy="always",
            )
        )
        session.commit()

    mock_process = MagicMock()
    mock_process.wait.return_value = 1
    mock_process.stdout = MagicMock()
    mock_process.stdout.__iter__.return_value = ["2026-10-02T22:21:32.123456789Z partial line\n"]

    with patch("subprocess.Popen", return_value=mock_process):
        ingest_docker_logs_once(tail=200)

    with get_session() as session:
        rows = session.exec(select(LogEvent)).all()
        assert rows == []

