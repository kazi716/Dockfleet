import asyncio
import httpx
from httpx import ASGITransport

from dockfleet.dashboard.api import app


def test_get_services_schema():
    async def _run():
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.get("/services")

    response = asyncio.run(_run())
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    # don't assume data exists
    if data:
        service = data[0]
        assert "name" in service
        assert "status" in service
        assert "health_status" in service
        assert "restart_count" in service


def test_restart_endpoint():
    async def _run():
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.post("/services/api/restart")

    response = asyncio.run(_run())
    # allow both success and safe failure
    assert response.status_code in [200, 400, 404]


def test_stop_endpoint():
    async def _run():
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.post("/services/api/stop")

    response = asyncio.run(_run())
    # allow both success and safe failure
    assert response.status_code in [200, 400, 404]


def test_dashboard_home_renders_html_from_any_cwd(tmp_path, monkeypatch):
    """Verify that the dashboard home page renders HTML even when cwd is changed."""
    monkeypatch.chdir(tmp_path)

    async def _run():
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.get("/")

    response = asyncio.run(_run())
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "<!DOCTYPE html>" in response.text or "<html" in response.text
