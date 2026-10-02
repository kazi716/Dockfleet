from __future__ import annotations

import subprocess
import tempfile
from datetime import datetime, timedelta, timezone

from sqlmodel import select

from .models import LogCursor, LogEvent, Service, get_session


def ingest_docker_logs_once(tail: int = 200) -> None:
    """
    Pull last `tail` docker logs for every known Service and store them
    into LogEvent for /logs/db and /logs/download.

    Idempotency-ish guard: we ensure created_at is strictly increasing
    per service so repeated runs don't break ordering.
    """
    with get_session() as session:
        services = session.exec(select(Service)).all()

        for svc in services:
            name = svc.name
            svc_id = svc.id
            container = f"dockfleet_{name}"

            # Fetch docker source timestamp cursor
            cursor_row = session.exec(
                select(LogCursor).where(LogCursor.service_id == svc_id)
            ).one_or_none()
            cursor_ts_str = cursor_row.last_timestamp if cursor_row else None

            # latest log timestamp we already have for this service
            latest_ts: datetime | None = session.exec(
                select(LogEvent.created_at)
                .where(LogEvent.service_name == name)
                .order_by(LogEvent.created_at.desc()) # type: ignore
                .limit(1)
            ).one_or_none()

            cmd = ["docker", "logs", "--timestamps"]
            if cursor_ts_str is not None:
                cmd.extend(["--since", cursor_ts_str])
            else:
                cmd.extend(["--tail", str(tail)])
            cmd.append(container)

            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
            except (subprocess.SubprocessError, OSError) as e:
                print(f"Error streaming docker logs for {name}: {e}")
                continue

            with tempfile.TemporaryFile(mode="w+t") as spool:
                # Stage to disk (tempfile) to avoid memory blowup while we wait for success
                for line in process.stdout:
                    line = line.rstrip()
                    if line:
                        spool.write(line + "\n")
                
                process.stdout.close()
                if process.wait() != 0:
                    session.rollback()
                    continue

                spool.seek(0)
                batch_count = 0
                last_source_ts = None
                
                for line in spool:
                    line = line.rstrip()
                    if not line:
                        continue
                        
                    message = line
                    if " " in line:
                        ts_str, msg = line.split(" ", 1)
                        if ts_str.startswith("20") and ts_str.endswith("Z"):
                            last_source_ts = ts_str
                            message = msg
                    
                    now = datetime.now(timezone.utc)
                    if latest_ts is not None:
                        if latest_ts.tzinfo is None:
                            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
                        if now <= latest_ts:
                            now = latest_ts + timedelta(microseconds=1)

                    event = LogEvent(
                        service_id=svc_id,
                        service_name=name,
                        created_at=now,
                        level=None,
                        message=message,
                        source="docker-logs-ingestor",
                    )
                    session.add(event)
                    latest_ts = now
                    
                    batch_count += 1
                    if batch_count >= 1000:
                        if last_source_ts:
                            if not cursor_row:
                                cursor_row = LogCursor(service_id=svc_id, last_timestamp=last_source_ts)
                                session.add(cursor_row)
                            else:
                                cursor_row.last_timestamp = last_source_ts
                        session.commit()
                        batch_count = 0

                if batch_count > 0:
                    if last_source_ts:
                        if not cursor_row:
                            cursor_row = LogCursor(service_id=svc_id, last_timestamp=last_source_ts)
                            session.add(cursor_row)
                        else:
                            cursor_row.last_timestamp = last_source_ts
                    session.commit()
