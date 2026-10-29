"""Serveur MCP : outils de lecture sur l'API GraphQL Dagster.

Outils de la V1 (lecture seule par défaut) :
  - dagster_overview      : nom du dépôt, jobs, runs récents, backfills, schedules
  - dagster_list_assets   : assets + partitions
  - dagster_asset_state   : état et métadonnées d'un asset (par key)
  - dagster_asset_partitions : états de partitions d'un asset
  - dagster_run_detail    : détail d'un run (steps, logs, métadonnées)
  - dagster_list_runs     : runs filtrables (job, statut)
  - dagster_sensors_schedules : sensors + schedules
Le mode read_only (par défaut true) refuse toute opération GraphQL d'écriture.
"""

from __future__ import annotations

import json
import functools
import inspect
from typing import Any

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from .config import Config
from .dagster_client import (
    DagsterClient,
    DagsterError,
    ForwardAuthError,
    looks_like_write,
)

mcp = FastMCP("mcp-dagster")


def _client() -> DagsterClient:
    try:
        cfg = Config.load()
    except Exception as e:
        raise ToolError(f"Config invalide : {e}") from e
    return DagsterClient(cfg)


def _run_tool(fn):
    """Exécute fn(client, ...) dans un client ouvert, et convertit les erreurs
    en ToolError. functools.wraps préserve le nom + la signature pour que
    FastMCP enregistre chaque tool sous SON nom avec le bon schéma d'entrée."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with _client() as client:
            try:
                return fn(client, *args, **kwargs)
            except PermissionError as e:
                raise ToolError(str(e)) from e
            except ForwardAuthError as e:
                raise ToolError(str(e)) from e
            except DagsterError as e:
                raise ToolError(str(e)) from e
            except Exception as e:  # noqa: BLE001
                raise ToolError(f"Échec : {type(e).__name__}: {e}") from e

    # « client » est injecté par ce wrapper : il ne figure PAS dans le schéma
    # d'entrée MCP (sinon le client MCP devrait le fournir).
    sig = inspect.signature(fn)
    wrapper.__signature__ = sig.replace(  # type: ignore[attr-defined]
        parameters=[p for n, p in sig.parameters.items() if n != "client"]
    )
    return wrapper


def _fmt(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


# ── Requêtes GraphQL ─────────────────────────────────────────────────────────

_OVERVIEW = """
query ($pageSize: Int!) {
  repository { name }
  jobs(first: $pageSize) { count items { name description state } }
  runs(first: 10) { items { id status stateName startTime runMetadata { name value } } }
  backfills(first: 5) { items { id status } }
  schedules { items { name status stateDescription } }
}
"""

_ASSETS = """
query ($pageSize: Int!) {
  assets(first: $pageSize) {
    count
    items {
      key { path }
      description
      codeVersion
      freshKey
      lastMaterialized {
        materialization { assetKey { path } storageId }
        runId
      }
      partitions { count spec { name type } }
    }
  }
}
"""

_ASSET_STATE = """
query ($assetKey: AssetKey!) {
  asset(assetKey: $assetKey) {
    key { path }
    description
    codeVersion
    lastMaterialized {
      materialization { assetKey { path } storageId }
      runId
      event {
        ... on AssetMaterialization {
          eventMetadata { name value }
          metadataEntries { name label value }
        }
      }
    }
    partitions {
      count
      items { name lastMaterialization { runId } state { status } }
    }
  }
}
"""

_RUN_DETAIL = """
query ($runId: String!) {
  run(runId: $runId) {
    id status stateName startTime endTime jobName repositoryName
    tags { key value }
    runMetadata { name value }
    steps { name status stateName }
  }
}
"""

_RUNS = """
query ($pageSize: Int!, $jobName: String, $status: RunStatus) {
  runs(first: $pageSize, jobName: $jobName, status: $status) {
    count
    items { id status stateName startTime jobName }
  }
}
"""

_SENSORS_SCHEDULES = """
query {
  sensors { items { name description status stateDescription } }
  schedules { items { name description status stateDescription } }
}
"""


# ── Outils ───────────────────────────────────────────────────────────────────


@mcp.tool()
@_run_tool
def dagster_overview(client: DagsterClient, page_size: int = 50) -> str:
    """Vue d'ensemble de l'instance Dagster : nom du dépôt, liste des jobs,
    10 derniers runs, backfills et schedules actifs."""
    data = client.query(_OVERVIEW, {"pageSize": page_size})
    return _fmt(data)


@mcp.tool()
@_run_tool
def dagster_list_assets(client: DagsterClient, page_size: int = 100) -> str:
    """Liste des assets Dagster avec leur code version, dernière
    matérialisation et nombre de partitions."""
    data = client.query(_ASSETS, {"pageSize": page_size})
    return _fmt(data)


@mcp.tool()
@_run_tool
def dagster_asset_state(client: DagsterClient, key: str) -> str:
    """État et métadonnées d'un asset (key = chemin, ex. "ch.dataset" ou
    "ch/dataset" ; le « . » est normalisé en « / »)."""
    asset_key = {"path": key.strip().replace(".", "/").split("/")}
    data = client.query(_ASSET_STATE, {"assetKey": asset_key})
    if not data.get("asset"):
        return f"Asset introuvable : {key}"
    return _fmt(data)


@mcp.tool()
@_run_tool
def dagster_list_runs(
    client: DagsterClient,
    page_size: int = 20,
    job_name: str | None = None,
    status: str | None = None,
) -> str:
    """Liste des runs (optionnel : filtre par job et/ou statut, ex.
    SUCCESS, FAILURE, RUNNING, CANCELLED)."""
    data = client.query(
        _RUNS,
        {"pageSize": page_size, "jobName": job_name, "status": status},
    )
    return _fmt(data)


@mcp.tool()
@_run_tool
def dagster_run_detail(client: DagsterClient, run_id: str) -> str:
    """Détail d'un run Dagster (id du run) : statut, job, tags, métadonnées
    de run et état de chaque step."""
    data = client.query(_RUN_DETAIL, {"runId": run_id})
    if not data.get("run"):
        return f"Run introuvable : {run_id}"
    return _fmt(data)


@mcp.tool()
@_run_tool
def dagster_sensors_schedules(client: DagsterClient) -> str:
    """Sensors et schedules de l'instance avec leur état."""
    data = client.query(_SENSORS_SCHEDULES)
    return _fmt(data)


if __name__ == "__main__":
    mcp.run()