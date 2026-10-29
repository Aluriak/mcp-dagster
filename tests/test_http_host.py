"""Test de régression 421 « Invalid Host header » (conteneur derrière réseau).

Situation exacte de production : LibreChat (client du réseau compose) envoie
``Host: mcp-dagster-ro:8000`` (nom du conteneur + port), pas un loopback.
Avant le fix, FastMCP auto-activait la protection anti-DNS-rebinding (host de
construction = 127.0.0.1) et répondait 421 AVANT l'initialize → « échec de
l'initialisation du serveur mcp » dans LibreChat (commit 4889b83 du rôle
librechat, 2026-10-29).

A/B sur deux instances FastMCP fraîches servies par uvicorn (threads), client
httpx réel, header Host override (httpx le laisse passer, ce qui reproduit
fidèlement le header du réseau compose) :
  - « old » : settings du SDK d'origine (protection auto active) → 421.
  - « fix » : ``_configure_http`` (transport_security=None) → 200 + result.
"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import threading
import time

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


def _headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Authorization": "Bearer t",
        # Le header tel quel que reçoit le conteneur sur le réseau compose :
        "Host": "mcp-dagster-ro:8000",
    }


def _build_old_behavior_app():
    """App comme AVANT le fix : FastMCP construit par défaut (host=127.0.0.1
    → protection anti-rebinding auto-activée), jamais désactivée."""
    from mcp.server.fastmcp import FastMCP

    f = FastMCP("mcp-dagster-old")
    f.settings.stateless_http = True
    assert f.settings.transport_security is not None, (
        "précondition : le SDK doit auto-activer la protection (sinon ce "
        "test ne reprod plus la régression)"
    )
    return f.streamable_http_app()


def _build_fixed_app():
    """App comme APRÈS le fix : mêmes réglages que build_app(), sur une
    instance fraîche pour ne pas toucher le singleton de production."""
    from mcp.server.fastmcp import FastMCP

    f = FastMCP("mcp-dagster-fixed")
    f.settings.host = "0.0.0.0"
    f.settings.stateless_http = True
    f.settings.transport_security = None
    return f.streamable_http_app()


def _serve(app, port: int) -> threading.Thread:
    import uvicorn

    def run():
        uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port,
                                      log_level="error")).run()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


async def _initialize(port: int) -> tuple[int, dict | None]:
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}",
                                 timeout=10) as c:
        r = await c.post("/mcp", json=_init_body(), headers=_headers())
        data = None
        if "text/event-stream" in r.headers.get("content-type", ""):
            for line in r.text.splitlines():
                if line.startswith("data:"):
                    data = json.loads(line[5:].strip())
                    break
        else:
            try:
                data = r.json()
            except Exception:
                data = None
        return r.status_code, data


def _wait_ready(port: int) -> None:
    for _ in range(100):
        try:
            httpx.get(f"http://127.0.0.1:{port}/mcp", timeout=0.5)
            return
        except Exception:
            time.sleep(0.1)
    pytest.fail(f"serveur de test sur le port {port} pas prêt")


def test_host_container_421_regression(monkeypatch):
    """421 avant le fix / 200 après, A/B sur le même header Host compose."""
    monkeypatch.setenv("DAGSTER_MCP_BASE_URL", "http://127.0.0.1:9")
    monkeypatch.delenv("DAGSTER_MCP_API_TOKEN", raising=False)
    monkeypatch.setenv("DAGSTER_MCP_FORWARD_HEADER", "Authorization")

    port_old, port_new = _free_port(), _free_port()
    t_old = _serve(_build_old_behavior_app(), port_old)
    t_new = _serve(_build_fixed_app(), port_new)
    try:
        _wait_ready(port_old)
        _wait_ready(port_new)

        status, data = asyncio.run(_initialize(port_old))
        assert status == 421, (
            f"attendu 421 (comportement SDK d'origine, protection auto), "
            f"reçu {status} : {data!r}"
        )

        status, data = asyncio.run(_initialize(port_new))
        assert status == 200, (
            f"le fix doit faire passer l'initialize (plus de 421), "
            f"reçu {status} : {data!r}"
        )
        assert data is not None and "result" in data, (
            f"réponse initialize sans result : {data!r}"
        )
    finally:
        t_old.join(timeout=5)
        t_new.join(timeout=5)


def test_build_app_posture():
    """La posture par défaut du mode conteneur : _configure_http pose
    transport_security=None (le fix), host 0.0.0.0, stateless.

    ``build_app()`` applique ``_configure_http`` puis crée l'app ; on teste
    le réglage seul ici car l'app du singleton est one-shot (le test A/B
    ci-dessus l'ayant déjà exercée via ses instances fraîches)."""
    from mcp_dagster import __main__ as entry
    from mcp_dagster.server import mcp

    entry._configure_http(mcp)
    assert mcp.settings.transport_security is None
    assert mcp.settings.stateless_http is True
    assert mcp.settings.host == "0.0.0.0"