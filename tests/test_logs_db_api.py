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
        mock_process.wait = MagicMock()
        mock_stdout = MagicMock()
        
        if "--tail" in cmd:
            mock_stdout.__iter__.return_value = ["line 1\n", "line 2\n"]
        elif "--since" in cmd:
            mock_stdout.__iter__.return_value = ["line 3\n"]
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
        assert recorded_cmds[0][:4] == ["docker", "logs", "--tail", "200"]
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
        assert recorded_cmds[1][:2] == ["docker", "logs"]
        assert recorded_cmds[1][2] == "--since"
        assert recorded_cmds[1][-1] == "dockfleet_api"
