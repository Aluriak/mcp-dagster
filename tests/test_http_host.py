"""Posture réseau du mode conteneur (FastMCP 3.x).

Historique : sous l'ancien SDK ``mcp`` 1.x, FastMCP auto-activait une
protection anti-DNS-rebinding (``transport_security``) figée sur les
loopbacks. Derrière un réseau de conteneurs, LibreChat (client du réseau
compose) envoie ``Host: mcp-dagster-ro:8080`` (nom du conteneur, pas un
loopback) → le SDK répondait **421 Invalid Host header** avant même
l'``initialize``. Le correctif d'alors désactivait ``transport_security``
explicitement (commit 5e0d59f) avec un test A/B sur deux instances.

Le port sur FastMCP 3.x supprime cette classe de bug **à la racine** : le SDK
n'applique aucun contrôle du header Host par défaut, donc un ``Host`` non
loopback est accepté sans aucune configuration. L'A/B n'a plus d'objet — il
n'y a plus de « comportement d'origine » à reproduire. Ce qui reste à
garantir :

  1. le ``HeaderCaptureMiddleware`` pose les headers entrants dans le
     contextvar (unit, hors réseau) ;
  2. ``build_app()`` retourne l'app ASGI stateless de FastMCP ;
  3. de bout en bout : un client HTTP réel (uvicorn + httpx, header
     ``Host: mcp-dagster-ro:8080``) passe l'``initialize`` ET le header
     ``Authorization`` est relayé jusqu'au client Dagster (preuve que le
     contextvar se propage à travers le middleware natif, le lifespan et le
     session manager, jusqu'à l'exécution du tool).
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import threading
import time
from typing import Any

import httpx
import pytest

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, os.path.abspath(SRC))


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _init_body() -> dict:
    return {
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                   "clientInfo": {"name": "t", "version": "0"}},
    }


def _call_body(name: str, arguments: dict) -> dict:
    return {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": name, "arguments": arguments}}


def _base_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        # Le header tel quel que reçoit le conteneur sur le réseau compose :
        "Host": "mcp-dagster-ro:8080",
    }


def _rpc(resp: httpx.Response) -> dict[str, Any]:
    """Extrait la charge JSON-RPC d'une réponse SSE (``data:``) ou JSON pur."""
    body = resp.text
    if body.lstrip().startswith("{"):
        return resp.json()
    data_lines = [ln[5:].strip() for ln in body.splitlines()
                  if ln.startswith("data: ")]
    return json.loads(data_lines[-1]) if data_lines else {}


def _wait_ready(port: int) -> None:
    for _ in range(120):
        try:
            httpx.get(f"http://127.0.0.1:{port}/mcp", timeout=0.5)
            return
        except Exception:
            time.sleep(0.1)
    pytest.fail(f"serveur de test sur le port {port} pas prêt")


# ── Unit : le middleware capture les headers dans le contextvar ─────────────


def test_header_capture_middleware_sets_contextvar():
    from mcp_dagster import __main__ as entry
    from mcp_dagster.request_ctx import get_request_headers

    captured: dict[str, Any] = {}

    class FakeInner:
        async def __call__(self, scope, receive, send):
            rh = get_request_headers()
            for name in ("authorization", "x-foo", "host"):
                captured[name] = rh.get(name)

    app = entry.HeaderCaptureMiddleware(FakeInner())
    scope = {"type": "http", "headers": [
        (b"host", b"mcp-dagster-ro:8080"),
        (b"authorization", b"Bearer user-jeton"),
        (b"x-foo", b"bar"),
    ]}

    async def drive():
        await app(scope, None, None)

    asyncio.run(drive())
    assert captured.get("authorization") == "Bearer user-jeton"
    assert captured.get("x-foo") == "bar"
    assert captured.get("host") == "mcp-dagster-ro:8080"


# ── build_app() : app ASGI stateless de FastMCP ─────────────────────────────


def test_build_app_returns_starlette_with_lifespan():
    from mcp_dagster import __main__ as entry

    app = entry.build_app()
    # http_app() renvoie une Starlette (sous-classe avec lifespan).
    from starlette.applications import Starlette
    assert isinstance(app, Starlette)
    assert any(
        getattr(route, "path", None) == "/mcp" for route in app.routes
    ), "l'app doit exposer le point de montage /mcp"


# ── De bout en bout : Host compose + relay du jeton utilisateur ─────────────


def test_end_to_end_host_and_forward(monkeypatch):
    """Un client du réseau compose (Host non loopback) passe l'initialize, et
    le header Authorization entrant est relayé jusqu'au client Dagster —
    preuve que le contextvar traverse middleware + lifespan + session manager
    jusqu'au tool. Le client Dagster est mocké (FakeClient) pour rester
    hors-réseau : seul le chemin HTTP→contextvar→tool est réel."""
    from mcp_dagster import __main__ as entry
    from mcp_dagster.dagster_client import DagsterClient
    import mcp_dagster.server as server

    # Config en mode forward_header : le jeton entrant est relayé tel quel,
    # pas de token statique. _client() réel lit cette config ; on mocke le
    # client réseau (FakeClient) pour rester hors-réseau — seul le chemin
    # HTTP→contextvar→tool est réel.
    monkeypatch.setenv("DAGSTER_MCP_BASE_URL", "http://dagster.test:3000")
    monkeypatch.delenv("DAGSTER_MCP_API_TOKEN", raising=False)
    monkeypatch.setenv("DAGSTER_MCP_FORWARD_HEADER", "Authorization")
    monkeypatch.setenv("DAGSTER_MCP_READ_ONLY", "true")

    class FakeResponse:
        def __init__(self, data):
            self._data = data
        def json(self):
            return self._data

    class FakeClient:
        def __init__(self, **_kw):
            self.requests: list[dict] = []
        def post(self, url, json=None, headers=None):
            self.requests.append({"url": url, "json": json, "headers": headers})
            return FakeResponse({"run": {"id": "r1", "state": "SUCCESS"}})
        def close(self):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    from mcp_dagster.config import Config
    fake = FakeClient()
    # Config réelle depuis l'env (mode forward_header ci-dessus) — seul le
    # client réseau httpx est remplacé.
    monkeypatch.setattr(
        server, "_client",
        lambda: DagsterClient(Config.load(), client=fake),
    )

    port = _free_port()
    app = entry.build_app()

    def serve():
        import uvicorn
        uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                      lifespan="on", log_level="error")).run()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    try:
        _wait_ready(port)

        async def drive():
            async with httpx.AsyncClient(
                    base_url=f"http://127.0.0.1:{port}", timeout=15) as c:
                h = _base_headers()
                r1 = await c.post("/mcp", json=_init_body(), headers=h)
                assert r1.status_code == 200, (
                    f"l'initialize doit passer avec Host non loopback, "
                    f"reçu {r1.status_code} : {_rpc(r1)!r}")
                h2 = {**h, "Authorization": "Bearer user-jeton"}
                r2 = await c.post("/mcp",
                                  json=_call_body("dagster_run_detail",
                                                  {"run_id": "r1"}),
                                  headers=h2)
                assert r2.status_code == 200, (
                    f"le tools/call doit passer, reçu {r2.status_code}")
        asyncio.run(drive())

        # Le relay : le jeton entrant est passé TEL QUEL au client Dagster.
        assert fake.requests, "aucune requête GraphQL émise"
        assert fake.requests[0]["headers"]["Authorization"] == "Bearer user-jeton"
        assert "runOrError(runId: $runId)" in fake.requests[0]["json"]["query"]
    finally:
        t.join(timeout=5)