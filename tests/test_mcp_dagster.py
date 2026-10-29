"""Tests mcp-dagster : client GraphQL, découverte workspace, garde-fous
read_only, config, outils MCP, et validation des requêtes contre le vrai
schéma Dagster 1.12.1.

Tous les tests sont hors-réseau : le client httpx est mocké. Le schéma
(GraphQL SDL) est un snapshot de ``dagster_graphql.schema.create_schema()``
de la 1.12.1 (``tests/schema_1_12_1.graphql``).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from fastmcp.exceptions import ToolError

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
from mcp_dagster import queries as Q


# ── Faux client httpx ────────────────────────────────────────────────────────

WORKSPACE_DATA = {
    "workspaceOrError": {
        "locationEntries": [
            {
                "name": "eds",
                "loadStatus": "LOADED",
                "locationOrLoadError": {
                    "repositories": [{"name": "cdc_eds"}],
                    "name": "eds",
                },
            }
        ]
    }
}


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=""):
        self.status_code = status_code
        self._body = body or {}
        self.text = text or json.dumps(self._body)

    def json(self):
        return self._body


class FakeClient:
    """Répond par champ : si ``self.data`` porte la clé demandée par la
    query (top-level field sélectionné), on la renvoie ; sinon réponse
    vide. ``workspace`` répond à la query de découverte (``locationEntries``).

    Pour les requêtes combinées (plusieurs champs dans une seule query, ex.
    OVERVIEW = workspaceOrError + runsOrError + partitionBackfillsOrError),
    chaque champ est satisfait indépendamment à partir de ``self.data``."""

    def __init__(self, data=None, errors=None, status_code=200, workspace=None, **_kw):
        self.data = data or {}
        self.errors = errors
        self.status_code = status_code
        self.workspace = workspace if workspace is not None else WORKSPACE_DATA
        self.requests: list[dict] = []

    def post(self, url, json=None, headers=None):
        self.requests.append({"url": url, "json": json, "headers": headers})
        q = (json or {}).get("query", "")
        body = {}
        if self.errors:
            body["errors"] = self.errors
            return FakeResponse(self.status_code, body)
        data = {}
        for key in ("workspaceOrError", "runsOrError", "partitionBackfillsOrError",
                    "repositoryOrError", "assetNodeOrError", "assetRecordsOrError",
                    "schedulesOrError", "sensorsOrError", "runOrError",
                    "launchPartitionBackfill"):
            if key in q:
                if key == "workspaceOrError":
                    data[key] = self.workspace.get("workspaceOrError")
                elif key in self.data:
                    data[key] = self.data[key]
        body["data"] = data
        return FakeResponse(self.status_code, body)

    def close(self):
        pass


def make_client(**fake_kwargs) -> DagsterClient:
    cfg = Config(base_url="http://dagster.local", api_token="tok")
    return DagsterClient(cfg, client=FakeClient(**fake_kwargs))


# Le contextvar de la requête courante est global au module ; on le remets à
# zéro entre chaque test pour éviter les fuites.
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


# ── looks_like_write (motifs d'écriture de l'API 1.x) ────────────────────────


@pytest.mark.parametrize(
    "q,expected",
    [
        ("query { repository { name } }", False),
        ("query ($x: Int) { runsOrError(limit: $x) { ... on Runs { count } } }", False),
        ("query { workspaceOrError { ... on Workspace { locationEntries { name } } } }", False),
        ("mutation { launchPartitionBackfill(backfillParams: {assetSelection: []}) }", True),
        ('query { startRun(jobName: "j") { id } }', True),
        ("mutation { deletePipelineRun(runId: \"a\") }", True),
        ("mutation { terminateRun(runId: \"a\") }", True),
        ("query { wipeAssets(assetPartitionRanges: []) }", True),
        ("mutation { setAutoMaterializePaused(paused: true) }", True),
        ("mutation { reloadWorkspace }", True),
    ],
)
def test_looks_like_write(q, expected):
    assert looks_like_write(q) is expected


# ── Client : requête OK, erreurs, auth ───────────────────────────────────────


def test_query_returns_data():
    c = make_client(data={"runsOrError": {"count": 1}})
    out = c.query("query { runsOrError { ... on Runs { count } } }")
    assert out == {"runsOrError": {"count": 1}}
    req = c._client.requests[0]
    assert req["url"] == "http://dagster.local/graphql"
    assert req["headers"]["Authorization"] == "Bearer tok"
    assert req["json"] == {
        "query": "query { runsOrError { ... on Runs { count } } }",
        "variables": {},
    }


def test_query_passes_variables_and_drops_none():
    c = make_client(data={})
    c.query(
        "query ($p: Int!, $j: String) { runsOrError(limit: $p) { ... on Runs { count } } }",
        {"p": 5, "j": None},
    )
    assert c._client.requests[0]["json"]["variables"] == {"p": 5}


def test_graphql_errors_raise():
    c = make_client(errors=[{"message": "field inconnu"}])
    with pytest.raises(DagsterError, match="field inconnu"):
        c.query("query { inconnu }")


def test_http_error_raises():
    c = make_client(status_code=404, text="<html>pas ça</html>")
    with pytest.raises(DagsterError, match="HTTP 404"):
        c.query("query { x }")


# ── Découverte workspace / RepositorySelector ────────────────────────────────


def test_workspace_discovery_and_cache():
    c = make_client()
    repos = c.workspace()
    assert repos == [
        {"location": "eds", "repository": "cdc_eds", "loadStatus": "LOADED"}
    ]
    # Cache : une deuxième fois sans nouveau POST.
    c.workspace()
    assert len([r for r in c._client.requests if "locationEntries" in r["json"]["query"]]) == 1


def test_repository_selector_autodetects_single_repo():
    c = make_client()
    sel = c.repository_selector()
    assert sel == {"repositoryLocationName": "eds", "repositoryName": "cdc_eds"}


def test_repository_selector_explicit_names():
    c = make_client()
    sel = c.repository_selector(location="eds", repository="cdc_eds")
    assert sel == {"repositoryLocationName": "eds", "repositoryName": "cdc_eds"}


def test_repository_selector_unknown_location():
    c = make_client()
    with pytest.raises(DagsterError, match="Location introuvable"):
        c.repository_selector(location="nope")


def test_repository_selector_ambiguous_without_hint():
    ws = {
        "workspaceOrError": {
            "locationEntries": [
                {
                    "name": "eds",
                    "loadStatus": "LOADED",
                    "locationOrLoadError": {
                        "repositories": [{"name": "repo_a"}, {"name": "repo_b"}],
                        "name": "eds",
                    },
                }
            ]
        }
    }
    c = make_client(workspace=ws)
    with pytest.raises(DagsterError, match="Plusieurs repositories"):
        c.repository_selector()
    # Mais précisés, ça passe.
    assert c.repository_selector(location="eds", repository="repo_b")["repositoryName"] == "repo_b"


def test_repository_selector_no_repo_available():
    ws = {
        "workspaceOrError": {
            "locationEntries": [
                {
                    "name": "eds",
                    "loadStatus": "LOADING",
                    "locationOrLoadError": None,
                }
            ]
        }
    }
    c = make_client(workspace=ws)
    with pytest.raises(DagsterError, match="Aucun repository"):
        c.repository_selector()


# ── forward_header : relay du jeton utilisateur (mode HTTP partagé) ──────────


def _forward_client(**fake_kwargs) -> DagsterClient:
    cfg = Config(base_url="http://dagster.local", forward_header="Authorization")
    return DagsterClient(cfg, client=FakeClient(**fake_kwargs))


def test_forward_header_relays_incoming_token_verbatim():
    c = _forward_client(data={})
    set_request_headers({"Authorization": "Bearer user-jeton"})
    c.query("query { x }")
    sent = c._client.requests[0]
    assert sent["headers"]["Authorization"] == "Bearer user-jeton"


def test_forward_header_case_insensitive_lookup():
    c = _forward_client(data={})
    set_request_headers({"authorization": "eyJraw.token"})
    c.query("query { x }")
    assert c._client.requests[0]["headers"]["Authorization"] == "eyJraw.token"


def test_forward_header_missing_fails_explicitly_no_fallback():
    c = _forward_client(data={})  # pas de set_request_headers → contexte vide
    with pytest.raises(ForwardAuthError, match="forward_header"):
        c.query("query { x }")
    assert not c._client.requests  # rien n'est envoyé sur le réseau


def test_forward_header_empty_value_fails_too():
    c = _forward_client(data={})
    set_request_headers({"Authorization": "   "})
    with pytest.raises(ForwardAuthError):
        c.query("query { x }")
    assert not c._client.requests


def test_api_token_and_forward_header_mutually_exclusive():
    cfg = Config(
        base_url="http://dagster.local",
        api_token="tok",
        forward_header="Authorization",
    )
    with pytest.raises(ValueError, match="mutuellement exclusifs"):
        cfg.validate()
    with pytest.raises(ValueError, match="mutuellement exclusifs"):
        DagsterClient(cfg, client=FakeClient(data={}))


def test_forward_header_does_not_send_api_token_when_forwarding():
    c = _forward_client(data={})
    set_request_headers({"Authorization": "Bearer only-this"})
    c.query("query { x }")
    assert c._client.requests[0]["headers"]["Authorization"] == "Bearer only-this"


# ── read_only : refus des écritures (y compris via requête GraphQL) ──────────


def test_read_only_blocks_mutation():
    c = make_client(data={})
    with pytest.raises(PermissionError, match="read_only"):
        c.query("mutation { launchPartitionBackfill(backfillParams: {assetSelection: []}) }")
    assert not c._client.requests  # rien n'est envoyé sur le réseau


def test_read_only_override_allow_write():
    c = make_client(data={"launchPartitionBackfill": {"backfillId": "b1"}})
    out = c.query(
        "mutation { launchPartitionBackfill(backfillParams: {assetSelection: []}) "
        "{ ... on LaunchBackfillSuccess { backfillId } } }",
        allow_write=True,
    )
    assert out["launchPartitionBackfill"]["backfillId"] == "b1"


def test_read_only_disabled_in_config():
    cfg = Config(base_url="http://dagster.local", read_only=False)
    c = DagsterClient(cfg, client=FakeClient(data={}))
    c.query("mutation { reloadWorkspace }")  # pas de PermissionError
    assert c._client.requests


# ── Config : YAML + env ──────────────────────────────────────────────────────


def test_config_yaml_and_env(tmp_path, monkeypatch):
    y = tmp_path / "cfg.yaml"
    y.write_text("base_url: https://a\napi_token: yml\ntimeout: 5\nread_only: false\n")
    monkeypatch.setenv("DAGSTER_MCP_CONFIG", str(y))
    cfg = Config.load()
    assert cfg.base_url == "https://a"
    assert cfg.api_token == "yml"
    assert cfg.timeout == 5.0
    assert cfg.read_only is False

    monkeypatch.setenv("DAGSTER_MCP_READ_ONLY", "1")
    monkeypatch.setenv("DAGSTER_MCP_API_TOKEN", "env")
    cfg = Config.load()
    assert cfg.read_only is True
    assert cfg.api_token == "env"
    assert cfg.graphql_url == "https://a/graphql"


def test_config_graphql_url_override(tmp_path, monkeypatch):
    y = tmp_path / "cfg.yaml"
    y.write_text("base_url: https://a\ngraphql_url: https://a/api\n")
    monkeypatch.setenv("DAGSTER_MCP_CONFIG", str(y))
    assert Config.load().graphql_url == "https://a/api"

    monkeypatch.setenv("DAGSTER_MCP_GRAPHQL_URL", "https://b/graphql")
    assert Config.load().graphql_url == "https://b/graphql"

    monkeypatch.delenv("DAGSTER_MCP_GRAPHQL_URL")
    assert Config.load().graphql_url == "https://a/api"

    monkeypatch.delenv("DAGSTER_MCP_CONFIG")
    assert Config(base_url="https://a").graphql_url == "https://a/graphql"


def test_config_graphql_url_must_be_absolute(tmp_path, monkeypatch):
    y = tmp_path / "cfg.yaml"
    y.write_text("base_url: https://a\ngraphql_url: /custom\n")
    monkeypatch.setenv("DAGSTER_MCP_CONFIG", str(y))
    with pytest.raises(ValueError, match="URL absolue"):
        Config.load().validate()


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
        "dagster_workspace_status",
        "dagster_list_assets",
        "dagster_list_assets_paginated",
        "dagster_asset_status",
        "dagster_list_runs",
        "dagster_run_detail",
        "dagster_automations",
        "dagster_backfills",
        "dagster_jobs",
        "dagster_examples",
        "dagster_raw_query",
        "dagster_materialize",
    ):
        assert t in names


def _patch_client(monkeypatch, fake: FakeClient, **cfg_kwargs):
    cfg = Config(base_url="http://x", api_token="t", **cfg_kwargs)
    monkeypatch.setattr(
        server, "_client", lambda: DagsterClient(cfg, client=fake)
    )
    return fake


def _call(name: str, args: dict) -> str:
    result = asyncio.run(server.mcp.call_tool(name, args))
    return result.content[0].text


def test_tool_overview_end_to_end(monkeypatch):
    fake = FakeClient(
        data={
            "runsOrError": {
                "count": 1,
                "results": [{"id": "r1", "jobName": "j1", "status": "FAILURE"}],
            },
            "partitionBackfillsOrError": {"results": []},
        }
    )
    _patch_client(monkeypatch, fake)
    text = _call("dagster_overview", {})
    data = json.loads(text)
    # workspace détecté via la query de découverte
    assert data["workspace"]["locationEntries"][0]["name"] == "eds"
    assert data["dernieres_erreurs_runs"]["results"][0]["status"] == "FAILURE"
    assert fake.requests[0]["headers"]["Authorization"] == "Bearer t"


def test_tool_list_assets_sends_repository_selector(monkeypatch):
    fake = FakeClient(
        data={
            "repositoryOrError": {
                "name": "cdc_eds",
                "assetNodes": [
                    {"assetKey": {"path": ["ch", "orders"]}, "partitionStats": {}}
                ],
            }
        }
    )
    _patch_client(monkeypatch, fake)
    text = _call("dagster_list_assets", {})
    data = json.loads(text)
    assert data["repository"]["assetNodes"][0]["assetKey"]["path"] == ["ch", "orders"]
    # La query principale porte le selector auto-détecté.
    main = [
        r
        for r in fake.requests
        if "repositoryOrError" in r["json"]["query"] and "assetNodes" in r["json"]["query"]
    ]
    assert main
    assert main[0]["json"]["variables"] == {"loc": "eds", "repo": "cdc_eds"}


def test_tool_asset_status_normalizes_key(monkeypatch):
    fake = FakeClient(
        data={"assetNodeOrError": {"assetKey": {"path": ["ch", "dataset"]}}}
    )
    _patch_client(monkeypatch, fake)
    text = _call("dagster_asset_status", {"key": "ch.dataset"})
    data = json.loads(text)
    assert data["asset"]["assetKey"]["path"] == ["ch", "dataset"]
    req = fake.requests[0]
    assert req["json"]["variables"]["key"] == {"path": ["ch", "dataset"]}
    # assetNodeOrError est en racine : pas de discovery workspace requise.
    assert all("locationEntries" not in r["json"]["query"] for r in fake.requests)


def test_tool_asset_status_not_found(monkeypatch):
    _patch_client(monkeypatch, FakeClient(data={}))
    text = _call("dagster_asset_status", {"key": "ch.ghost"})
    assert "introuvable" in text


def test_tool_list_runs_status_list(monkeypatch):
    fake = FakeClient(data={"runsOrError": {"count": 0, "results": []}})
    _patch_client(monkeypatch, fake)
    _call("dagster_list_runs", {"statuses": "FAILURE, CANCELED"})
    req = fake.requests[0]
    assert req["json"]["variables"]["statuses"] == ["FAILURE", "CANCELED"]
    assert "pipelineName" not in req["json"]["variables"]  # None omis


def test_tool_run_detail(monkeypatch):
    fake = FakeClient(data={"runOrError": {"id": "r1", "status": "SUCCESS"}})
    _patch_client(monkeypatch, fake)
    text = _call("dagster_run_detail", {"run_id": "r1"})
    data = json.loads(text)
    assert data["run"]["id"] == "r1"
    sent = fake.requests[0]["json"]
    assert sent["variables"] == {"runId": "r1"}
    assert "runOrError(runId: $runId)" in sent["query"]


def test_tool_automations_sends_selector(monkeypatch):
    fake = FakeClient(
        data={
            "schedulesOrError": {"results": [{"name": "s1", "defaultStatus": "RUNNING"}]},
            "sensorsOrError": {"results": []},
        }
    )
    _patch_client(monkeypatch, fake)
    text = _call("dagster_automations", {"location": "eds", "repository": "cdc_eds"})
    data = json.loads(text)
    assert data["schedules"]["results"][0]["name"] == "s1"
    main = [r for r in fake.requests if "schedulesOrError" in r["json"]["query"]]
    assert main[0]["json"]["variables"] == {"loc": "eds", "repo": "cdc_eds"}


def test_tool_backfills(monkeypatch):
    fake = FakeClient(
        data={
            "partitionBackfillsOrError": {
                "results": [
                    {"id": "b1", "status": "REQUESTED", "numPartitions": 30}
                ]
            }
        }
    )
    _patch_client(monkeypatch, fake)
    text = _call("dagster_backfills", {"limit": 5})
    data = json.loads(text)
    assert data["backfills"]["results"][0]["numPartitions"] == 30


def test_tool_examples_lists_templates():
    text = _call("dagster_examples", {})
    assert "partitions_du_mois" in text
    assert "lineage_asset" in text


# ── dagster_raw_query : latitude lecture + verrou écriture ───────────────────


def test_raw_query_read_allowed(monkeypatch):
    fake = FakeClient(
        data={
            "workspaceOrError": {"locationEntries": []},
            "runsOrError": {"count": 0, "results": []},
            "partitionBackfillsOrError": {"results": []},
        }
    )
    _patch_client(monkeypatch, fake)
    text = _call("dagster_raw_query", {"query": Q.OVERVIEW})
    data = json.loads(text)
    # dagster_raw_query retourne le champ « data » BRUT (clés ...OrError).
    assert "workspaceOrError" in data
    assert "runsOrError" in data


def test_raw_query_mutation_blocked_in_read_only(monkeypatch):
    fake = FakeClient(data={})
    _patch_client(monkeypatch, fake, read_only=True)
    text = _call(
        "dagster_raw_query",
        {
            "query": "mutation { launchPartitionBackfill(backfillParams: {assetSelection: []}) }"
        },
    )
    assert "Refusé" in text or "read_only" in text
    assert not fake.requests  # rien n'est parti


def test_raw_query_mutation_allowed_in_read_write(monkeypatch):
    fake = FakeClient(data={"launchPartitionBackfill": {"backfillId": "b9"}})
    _patch_client(monkeypatch, fake, read_only=False)
    text = _call(
        "dagster_raw_query",
        {
            "query": "mutation { launchPartitionBackfill(backfillParams: {assetSelection: []}) "
            "{ ... on LaunchBackfillSuccess { backfillId } } }"
        },
    )
    data = json.loads(text)
    assert data["launchPartitionBackfill"]["backfillId"] == "b9"
    assert fake.requests


# ── dagster_materialize : garde-fou de confirmation + écriture ───────────────


def test_materialize_requires_confirmation(monkeypatch):
    fake = FakeClient(data={})
    _patch_client(monkeypatch, fake, read_only=False)
    with pytest.raises(ToolError, match="Confirmation requise"):
        _call("dagster_materialize", {"assets": "ch.orders"})
    assert not fake.requests


def test_materialize_read_only_flag_blocks(monkeypatch):
    fake = FakeClient(data={})
    _patch_client(monkeypatch, fake, read_only=False)
    with pytest.raises(ToolError, match="refusée"):
        _call("dagster_materialize", {"assets": "ch.orders", "read_only": True})
    assert not fake.requests


def test_materialize_launches_backfill(monkeypatch):
    fake = FakeClient(
        data={"launchPartitionBackfill": {"backfillId": "b42", "launchedRunIds": []}}
    )
    _patch_client(monkeypatch, fake, read_only=False)
    text = _call(
        "dagster_materialize",
        {"assets": "ch.orders, ch.payments", "partitions": "2026-09-01", "read_only": False},
    )
    data = json.loads(text)
    assert data["materialize"]["backfillId"] == "b42"
    sent = fake.requests[0]["json"]
    assert sent["variables"]["assets"] == [
        {"path": ["ch", "orders"]},
        {"path": ["ch", "payments"]},
    ]
    assert sent["variables"]["partitionNames"] == ["2026-09-01"]


def test_materialize_blocked_by_server_read_only(monkeypatch):
    """Même avec read_only=false au tool, le verrou serveur (config) s'applique."""
    fake = FakeClient(data={})
    _patch_client(monkeypatch, fake, read_only=True)
    with pytest.raises(ToolError, match="mode read_only"):
        _call("dagster_materialize", {"assets": "ch.orders", "read_only": False})
    assert not fake.requests


# ── Normalisation des asset keys ─────────────────────────────────────────────


def test_asset_key_variants():
    from mcp_dagster.server import _asset_key

    assert _asset_key("ch.dataset") == {"path": ["ch", "dataset"]}
    assert _asset_key("ch/dataset") == {"path": ["ch", "dataset"]}
    assert _asset_key("a/b/c") == {"path": ["a", "b", "c"]}
    assert _asset_key('["a","b"]') == {"path": ["a", "b"]}
    assert _asset_key("  ch.orders  ") == {"path": ["ch", "orders"]}
    # segments vides ignorés
    assert _asset_key("ch//dataset") == {"path": ["ch", "dataset"]}


# ── Validation des requêtes contre le vrai schéma Dagster 1.12.1 ─────────────

SCHEMA_PATH = Path(__file__).parent / "schema_1_12_1.graphql"


@pytest.fixture(scope="module")
def schema():
    from graphql import build_schema

    return build_schema(SCHEMA_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "query",
    [
        Q.OVERVIEW,
        Q.WORKSPACE_STATUS,
        Q.JOBS,
        Q.ASSETS,
        Q.ASSET_RECORDS,
        Q.ASSET_STATUS,
        Q.RUNS,
        Q.RUN_DETAIL,
        Q.AUTOMATIONS,
        Q.BACKFILLS,
        Q.MATERIALIZE,
    ],
    ids=[
        "overview",
        "workspace_status",
        "jobs",
        "assets",
        "asset_records",
        "asset_status",
        "runs",
        "run_detail",
        "automations",
        "backfills",
        "materialize",
    ],
)
def test_tool_query_validates_against_dagster_1_12_1_schema(schema, query):
    """Chaque requête outil doit être acceptée par le vrai schéma du
    webserver Dagster 1.12.1 (sinon Dagster renverrait une erreur de
    validation — c'est le bug qui a cassé la V1)."""
    from graphql import parse, validate

    errors = validate(schema, parse(query))
    assert not errors, "; ".join(str(e) for e in errors[:5])


@pytest.mark.parametrize("name", sorted(Q.EXEMPLES))
def test_example_validates_against_dagster_1_12_1_schema(schema, name):
    """Les exemples documentés (modèles pour l'agent) sont des requêtes
    valides, prêtes à être adaptées et relancées via dagster_raw_query."""
    from graphql import parse, validate

    errors = validate(schema, parse(Q.EXEMPLES[name]))
    assert not errors, f"{name}: " + "; ".join(str(e) for e in errors[:5])