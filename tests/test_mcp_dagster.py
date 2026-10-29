"""Tests mcp-dagster : client GraphQL, garde-fous read_only, config, outils MCP.

Tous les tests sont hors-réseau : le client httpx est mocké.
"""

from __future__ import annotations

import asyncio
import json
import os

import pytest

from mcp_dagster.config import Config
from mcp_dagster.dagster_client import (
    DagsterClient,
    DagsterError,
    ForwardAuthError,
    looks_like_write,
)
from mcp_dagster.request_ctx import (
    RequestHeaders,
    get_request_headers,
    set_request_headers,
)
import mcp_dagster.server as server


# ── Faux client httpx ────────────────────────────────────────────────────────


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=""):
        self.status_code = status_code
        self._body = body or {}
        self.text = text or json.dumps(self._body)

    def json(self):
        return self._body


class FakeClient:
    def __init__(self, data=None, errors=None, status_code=200, **_kw):
        self.data = data
        self.errors = errors
        self.status_code = status_code
        self.requests: list[dict] = []

    def post(self, url, json=None, headers=None):
        self.requests.append({"url": url, "json": json, "headers": headers})
        body = {}
        if self.errors:
            body["errors"] = self.errors
        if self.data is not None:
            body["data"] = self.data
        return FakeResponse(self.status_code, body)

    def close(self):
        pass


def make_client(**fake_kwargs) -> DagsterClient:
    cfg = Config(base_url="http://dagster.local", api_token="tok")
    return DagsterClient(cfg, client=FakeClient(**fake_kwargs))


# Le contextvar de la requête courante est global au module ; on le remets à
# zéro entre chaque test pour éviter les fuites (une requête teste A ne doit
# pas polluer la requête teste B).
@pytest.fixture(autouse=True)
def _reset_request_ctx():
    set_request_headers(None)
    yield
    set_request_headers(None)


# ── request_ctx : vue headers insensible à la casse ─────────────────────────


def test_request_headers_case_insensitive_and_trims():
    rh = RequestHeaders({"Authorization": "  Bearer x  ", "X-Other": "a"})
    assert rh.get("authorization") == "Bearer x"
    assert rh.get("AUTHORIZATION") == "Bearer x"
    assert rh.get("x-other") == "a"
    assert rh.get("absent") is None
    assert bool(rh) is True
    assert RequestHeaders(None).get("x") is None
    assert bool(RequestHeaders({})) is False


def test_request_headers_contextvar_roundtrip():
    assert get_request_headers().get("Authorization") is None  # stdio par défaut
    set_request_headers({"Authorization": "Bearer u1"})
    assert get_request_headers().get("Authorization") == "Bearer u1"


# ── looks_like_write ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "q,expected",
    [
        ("query { repository { name } }", False),
        ("query ($x: Int) { runs(first: $x) { items { id } } }", False),
        ('mutation { startRun(jobName: "j") }', True),
        ('query { startRun(jobName: "j") { id } }', True),
        ('mutation { deleteRuns(runIds: ["a"]) }', True),
        ('query { applyWorkspaceChange { success } }', True),
    ],
)
def test_looks_like_write(q, expected):
    assert looks_like_write(q) is expected


# ── Client : requête OK, erreurs, auth ───────────────────────────────────────


def test_query_returns_data():
    c = make_client(data={"repository": {"name": "repo"}})
    out = c.query("query { repository { name } }")
    assert out == {"repository": {"name": "repo"}}
    req = c._client.requests[0]
    assert req["url"] == "/api"
    assert req["headers"]["Authorization"] == "Bearer tok"
    assert req["json"] == {"query": "query { repository { name } }", "variables": {}}


def test_query_passes_variables_and_drops_none():
    c = make_client(data={})
    c.query("query ($p: Int!, $j: String) { runs(first: $p, jobName: $j) { count } }", {"p": 5, "j": None})
    assert c._client.requests[0]["json"]["variables"] == {"p": 5}


def test_graphql_errors_raise():
    c = make_client(errors=[{"message": "field inconnu"}])
    with pytest.raises(DagsterError, match="field inconnu"):
        c.query("query { inconnu }")


def test_http_error_raises():
    c = make_client(status_code=404, text="<html>pas ça</html>")
    with pytest.raises(DagsterError, match="HTTP 404"):
        c.query("query { x }")


# ── forward_header : relay du jeton utilisateur (mode HTTP partagé) ──────────


def _forward_client(**fake_kwargs) -> DagsterClient:
    """Client en mode forward_header (sans api_token)."""
    cfg = Config(
        base_url="http://dagster.local",
        forward_header="Authorization",
    )
    return DagsterClient(cfg, client=FakeClient(**fake_kwargs))


def test_forward_header_relays_incoming_token_verbatim():
    """(spec 1) header présent → transmis TEL QUEL comme Authorization sortant."""
    c = _forward_client(data={"repository": {"name": "r"}})
    set_request_headers({"Authorization": "Bearer user-jeton"})
    out = c.query("query { repository { name } }")
    assert out["repository"]["name"] == "r"
    sent = c._client.requests[0]
    # « tel quel » : pas de préfixe Bearer ajouté par le serveur.
    assert sent["headers"]["Authorization"] == "Bearer user-jeton"


def test_forward_header_case_insensitive_lookup():
    """Le nom de header configuré est cherché insensible à la casse."""
    c = _forward_client(data={})
    set_request_headers({"authorization": "eyJraw.token"})
    c.query("query { repository { name } }")
    assert c._client.requests[0]["headers"]["Authorization"] == "eyJraw.token"


def test_forward_header_missing_fails_explicitly_no_fallback():
    """(spec 2) forward_header actif mais header absent → échec explicite,
    AUCUN réseau, aucun repli sur api_token."""
    c = _forward_client(data={})  # pas de set_request_headers → contexte vide
    with pytest.raises(ForwardAuthError, match="forward_header"):
        c.query("query { repository { name } }")
    assert not c._client.requests  # rien n'est envoyé sur le réseau


def test_forward_header_empty_value_fails_too():
    """Valeur du header présente mais vide → idem, échec (pas de jeton utile)."""
    c = _forward_client(data={})
    set_request_headers({"Authorization": "   "})
    with pytest.raises(ForwardAuthError):
        c.query("query { x }")
    assert not c._client.requests


def test_api_token_and_forward_header_mutually_exclusive():
    """(spec 3) les deux renseignés → erreur de configuration au démarrage."""
    cfg = Config(
        base_url="http://dagster.local",
        api_token="tok",
        forward_header="Authorization",
    )
    with pytest.raises(ValueError, match="mutuellement exclusifs"):
        cfg.validate()
    # La construction du client appelle validate() → même échec.
    with pytest.raises(ValueError, match="mutuellement exclusifs"):
        DagsterClient(cfg, client=FakeClient(data={}))


def test_forward_header_via_env_and_yaml(tmp_path, monkeypatch):
    """(spec 3) chargement du champ depuis YAML + env DAGSTER_MCP_FORWARD_HEADER."""
    y = tmp_path / "cfg.yaml"
    y.write_text("base_url: https://a\nforward_header: X-User-Token\n")
    monkeypatch.setenv("DAGSTER_MCP_CONFIG", str(y))
    cfg = Config.load()
    assert cfg.forward_header == "X-User-Token"
    assert cfg.uses_forward_header is True

    # L'env force la valeur du yaml.
    monkeypatch.setenv("DAGSTER_MCP_FORWARD_HEADER", "Authorization")
    assert Config.load().forward_header == "Authorization"

    # Par défaut (aucun yaml, aucun env), le mode statique reste le comportement.
    monkeypatch.delenv("DAGSTER_MCP_CONFIG")
    monkeypatch.delenv("DAGSTER_MCP_FORWARD_HEADER")
    assert Config(base_url="http://x").uses_forward_header is False


def test_forward_header_does_not_send_api_token_when_forwarding():
    """En mode forward, le Authorization sortant NE vient JAMAIS d'un token
    statique, même si extra_headers est vide : c'est le relay qui décide."""
    c = _forward_client(data={})
    set_request_headers({"Authorization": "Bearer only-this"})
    c.query("query { x }")
    assert c._client.requests[0]["headers"]["Authorization"] == "Bearer only-this"


# ── read_only : refus des écritures ──────────────────────────────────────────


def test_read_only_blocks_start_run():
    c = make_client(data={})
    with pytest.raises(PermissionError, match="read_only"):
        c.query('mutation { startRun(jobName: "j") }')
    assert not c._client.requests  # rien n'est envoyé sur le réseau


def test_read_only_override_allow_write():
    c = make_client(data={"startRun": {"success": True}})
    out = c.query('mutation { startRun(jobName: "j") }', allow_write=True)
    assert out["startRun"]["success"] is True


def test_read_only_disabled_in_config():
    cfg = Config(base_url="http://dagster.local", read_only=False)
    c = DagsterClient(cfg, client=FakeClient(data={}))
    c.query('mutation { startRun(jobName: "j") }')  # pas de PermissionError
    assert c._client.requests


# ── Config : YAML + env ──────────────────────────────────────────────────────


def test_config_yaml_and_env(tmp_path, monkeypatch):
    y = tmp_path / "cfg.yaml"
    y.write_text(
        "base_url: https://a\napi_token: yml\ntimeout: 5\nread_only: false\n"
    )
    monkeypatch.setenv("DAGSTER_MCP_CONFIG", str(y))
    cfg = Config.load()
    assert cfg.base_url == "https://a"
    assert cfg.api_token == "yml"
    assert cfg.timeout == 5.0
    assert cfg.read_only is False

    # L'env force la valeur du yaml.
    monkeypatch.setenv("DAGSTER_MCP_READ_ONLY", "1")
    monkeypatch.setenv("DAGSTER_MCP_API_TOKEN", "env")
    cfg = Config.load()
    assert cfg.read_only is True
    assert cfg.api_token == "env"
    assert cfg.graphql_url == "https://a/api"


def test_config_missing_base_url_raises():
    cfg = Config()
    with pytest.raises(ValueError, match="base_url"):
        cfg.validate()


# ── Outils MCP (serveur) ─────────────────────────────────────────────────────


def _tools() -> list[str]:
    return [t.name for t in asyncio.run(server.mcp.list_tools())]


def test_expected_tools_exposed():
    names = _tools()
    for t in (
        "dagster_overview",
        "dagster_list_assets",
        "dagster_asset_state",
        "dagster_list_runs",
        "dagster_run_detail",
        "dagster_sensors_schedules",
    ):
        assert t in names


def test_tool_overview_end_to_end(monkeypatch):
    fake = FakeClient(
        data={
            "repository": {"name": "cdc_eds"},
            "jobs": {"count": 2, "items": [{"name": "j1"}]},
            "runs": {"items": []},
            "backfills": {"items": []},
            "schedules": {"items": [{"name": "s1", "status": "ACTIVE"}]},
        }
    )
    monkeypatch.setattr(
        server, "_client", lambda: DagsterClient(
            Config(base_url="http://x", api_token="t"), client=fake
        )
    )
    result = asyncio.run(server.mcp.call_tool("dagster_overview", {}))
    content = result.content
    text = content[0].text
    data = json.loads(text)
    assert data["repository"]["name"] == "cdc_eds"
    assert fake.requests[0]["headers"]["Authorization"] == "Bearer t"


def test_tool_asset_state_not_found(monkeypatch):
    monkeypatch.setattr(
        server, "_client", lambda: make_client(data={})
    )
    result = asyncio.run(
        server.mcp.call_tool("dagster_asset_state", {"key": "ch.dataset"})
    )
    content = result.content
    assert "introuvable" in content[0].text


def test_tool_run_detail_filters_key(monkeypatch):
    fake = FakeClient(data={"run": {"id": "r1"}})
    monkeypatch.setattr(
        server, "_client", lambda: DagsterClient(
            Config(base_url="http://x"), client=fake
        )
    )
    asyncio.run(server.mcp.call_tool("dagster_run_detail", {"run_id": "r1"}))
    sent = fake.requests[0]["json"]
    assert sent["variables"] == {"runId": "r1"}
    assert "run(runId: $runId)" in sent["query"]