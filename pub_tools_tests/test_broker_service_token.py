"""The mesh gateway can enforce its registration check only if pub-tools'
broker presents a service token, and the broker can guard /resolve only if
the installed dag-tools has the internal door. pub-tools was once pinned to a
dag-tools that did neither. These tests pin both behaviours so a downgrade of
the pin fails here rather than at the gateway.

Importing dag_tools.domain_broker.main builds the FastAPI app and reads
environment at module level, but opens no connection: registration and the
catalogue load happen in the lifespan, which these tests never start.
"""

import asyncio

import httpx
from fastapi.routing import APIRoute

from dag_tools.central_gateway import internal_door
from dag_tools.domain_broker import main


def test_registration_carries_the_service_token(monkeypatch):
    async def fake_headers():
        return {"Authorization": "Bearer test-token"}

    monkeypatch.setattr(main.internal_door, "service_headers", fake_headers)

    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            await main._register_once(client)

    asyncio.run(run())

    assert len(seen) == 1
    assert seen[0].url.path == "/api/v1/internal/register"
    assert seen[0].headers["Authorization"] == "Bearer test-token"


def test_resolve_route_is_guarded_by_the_internal_door():
    route = next(
        r
        for r in main.app.routes
        if isinstance(r, APIRoute) and r.path == "/api/v1/internal/resolve"
    )
    guards = [d.call for d in route.dependant.dependencies]
    assert guards, "the resolve route has no dependency"
    assert any(
        getattr(g, "__module__", None) == internal_door.__name__
        and g.__qualname__.startswith("door.")
        for g in guards
    ), "the resolve route is not guarded by internal_door.door"
