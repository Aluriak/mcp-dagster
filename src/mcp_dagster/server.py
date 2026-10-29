"""Serveur MCP : outils de lecture (et écriture optionnelle) sur l'API
GraphQL Dagster 1.x.

Conception (consigne Lucide/Lucas, 29/10/2026) : privilégier le maximum de
latitude pour l'agent. Les outils spécialisés couvrent les questions
courantes ; ``dagster_raw_query`` + ``dagster_examples`` donnent accès au
schéma complet via des requêtes adaptées/combinées par l'agent.

Outils de lecture (V2, API Dagster 1.x) :
  - dagster_overview          : locations, runs récents en erreur, backfills
  - dagster_workspace_status  : état détaillé des code locations / repositories
  - dagster_list_assets       : assets du repository (key, stockage, partitions)
  - dagster_asset_status      : état complet d'un asset (partitions, lineage,
                                stockage, ressources, dernières matérialisations)
  - dagster_list_runs         : runs filtrables (job, statuts)
  - dagster_run_detail        : détail d'un run (steps, events, métadonnées)
  - dagster_automations       : schedules + sensors (état, prochain tick, ticks récents)
  - dagster_backfills         : backfills et leur progression
  - dagster_examples          : requêtes modèles documentées (à adapter)
  - dagster_raw_query         : requête GraphQL arbitraire LECTURE seule
                                (en read_only ; cf. config)
Outil d'écriture (uniquement si read_only est désactivé dans la config) :
  - dagster_materialize       : matérialise des assets (backfill)

Le mode read_only (défaut true) refuse TOUTE modification — y compris une
mutation transmise via dagster_raw_query.
"""

from __future__ import annotations

import functools
import inspect
import json
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
from .queries import (
    ASSET_RECORDS,
    ASSET_STATUS,
    ASSETS,
    AUTOMATIONS,
    BACKFILLS,
    JOBS,
    MATERIALIZE,
    OVERVIEW,
    RUN_DETAIL,
    RUNS,
    WORKSPACE_STATUS,
    asset_key_variable,
    examples_doc,
    selector_variables,
)

mcp = FastMCP("mcp-dagster")


def _client() -> DagsterClient:
    try:
        cfg = Config.load()
    except Exception as e:
        raise ToolError(f"Config invalide : {e}") from e
    return DagsterClient(cfg)


def _run_tool(fn):
    """Exécute fn(client, ...) dans un client ouvert et convertit les erreurs
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


def _fmt(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def _union(payload: Any) -> Any:
    """Écrête le membre d'une union ``...OrError`` pour affichage :
    conserve le payload métier, transforme les erreurs en message lisible."""
    if payload is None:
        return {"warning": "réponse vide"}
    if isinstance(payload, dict) and "message" in payload and len(payload) <= 3:
        extra = {k: v for k, v in payload.items() if k != "message"}
        out: dict[str, Any] = {"error": payload["message"]}
        if extra:
            out["detail"] = extra
        return out
    return payload


def _with_repo(
    client: DagsterClient,
    location: str | None,
    repository: str | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Découvre le workspace et merge les variables de selector."""
    selector = client.repository_selector(location, repository)
    variables = selector_variables(selector["repositoryLocationName"], selector["repositoryName"])
    variables.update(extra or {})
    return variables


def _asset_key(key: str) -> dict[str, list[str]]:
    """Normalise un asset key en AssetKeyInput.path.

    Accepte "ch.dataset", "ch/dataset", ["a","b"] (JSON) ou "a/b". Le « . »
    est un séparateur comme le « / » (usage courant)."""
    k = key.strip()
    if k.startswith("["):
        try:
            path = json.loads(k)
            if isinstance(path, list) and all(isinstance(p, str) for p in path):
                return {"path": [p for p in path if p]}
        except json.JSONDecodeError:
            pass
    return asset_key_variable(k)["key"]


def _parse_assets(keys: str) -> list[dict[str, list[str]]]:
    """Parse une liste d'asset keys (séparées par des virgules) en
    AssetKeyInput."""
    out = []
    for part in keys.split(","):
        part = part.strip()
        if part:
            out.append(_asset_key(part))
    if not out:
        raise ToolError("Aucun asset key fourni.")
    return out


# ── Outils de lecture ────────────────────────────────────────────────────────


@mcp.tool()
@_run_tool
def dagster_overview(client: DagsterClient) -> str:
    """Vue d'ensemble de l'instance Dagster : code locations et leurs
    repositories, 10 derniers runs en erreur/annulés, backfills récents.

    Point de départ recommandé : donne les noms de location/repository à
    passer aux autres outils, et le goût de la santé générale de l'instance.
    """
    data = client.query(OVERVIEW)
    return _fmt(
        {
            "workspace": _union(data.get("workspaceOrError")),
            "dernieres_erreurs_runs": _union(data.get("runsOrError")),
            "backfills": _union(data.get("partitionBackfillsOrError")),
        }
    )


@mcp.tool()
@_run_tool
def dagster_workspace_status(client: DagsterClient) -> str:
    """État détaillé des code locations : statut de chargement, source de
    définitions (code server / connection), metadata de chaque repository."""
    data = client.query(WORKSPACE_STATUS)
    return _fmt({"workspace": _union(data.get("workspaceOrError"))})


@mcp.tool()
@_run_tool
def dagster_list_assets(
    client: DagsterClient,
    location: str | None = None,
    repository: str | None = None,
) -> str:
    """Liste les assets du repository : clé, groupe, kind de compute,
    jobs associés, adresse de stockage, métadonnées, stats et état des
    partitions (matérialisées / échouées / en cours / non matérialisées).

    Si l'instance expose plusieurs repositories, précisez location et/ou
    repository (les valeurs visibles via dagster_overview).
    """
    variables = _with_repo(client, location, repository)
    data = client.query(ASSETS, variables)
    return _fmt({"repository": _union(data.get("repositoryOrError"))})


@mcp.tool()
@_run_tool
def dagster_list_assets_paginated(
    client: DagsterClient,
    prefix: str | None = None,
    cursor: str | None = None,
    limit: int = 50,
) -> str:
    """Liste paginée des assets (tous repositories) : ``prefix`` filtre par
    préfixe de clé (ex. « ch »), ``cursor`` + ``limit`` pour la pagination
    (renvoyer le ``cursor`` de fin pour la page suivante). Préféré à
    dagster_list_assets quand le repository est volumineux."""
    variables: dict[str, Any] = {"limit": limit, "cursor": cursor}
    if prefix:
        variables["prefix"] = [p for p in prefix.strip().replace(".", "/").split("/") if p]
    data = client.query(ASSET_RECORDS, variables)
    return _fmt({"assets": _union(data.get("assetRecordsOrError"))})


@mcp.tool()
@_run_tool
def dagster_asset_status(
    client: DagsterClient,
    key: str,
    limit: int = 20,
) -> str:
    """État complet d'un asset par sa clé (ex. « ch.dataset » ou
    « ch/dataset ») : description, owners, jobs, op names, adresse de
    stockage, ressources requises, fraîcheur, métadonnées, état des
    partitions, dernières matérialisations (avec leurs métadonnées),
    lineage en amont (dependencies) et en aval (dependedBy)."""
    variables: dict[str, Any] = {"key": _asset_key(key), "limit": limit}
    data = client.query(ASSET_STATUS, variables)
    payload = data.get("assetNodeOrError")
    if not payload:
        return f"Asset introuvable : {key}"
    return _fmt({"asset": _union(payload)})


@mcp.tool()
@_run_tool
def dagster_list_runs(
    client: DagsterClient,
    limit: int = 20,
    job_name: str | None = None,
    status: str | None = None,
    statuses: str | None = None,
) -> str:
    """Liste des runs, filtrable par job et/ou statut.

    ``status`` : un statut unique (SUCCESS, FAILURE, STARTED, QUEUED, …).
    ``statuses`` : liste de statuts séparés par des virgules (ex.
    « FAILURE,CANCELED ») — pour les erreurs récentes. Les timestamps
    reviennent en epoch millis ; un run « in-progress » a un startTime
    non nul et un status STARTING/STARTED/QUEUED."""
    if statuses:
        status_values = [s.strip().upper() for s in statuses.split(",") if s.strip()]
    elif status:
        status_values = [status.strip().upper()]
    else:
        status_values = None
    variables: dict[str, Any] = {"limit": limit, "pipelineName": job_name}
    if status_values:
        variables["statuses"] = status_values
    data = client.query(RUNS, variables)
    return _fmt({"runs": _union(data.get("runsOrError"))})


@mcp.tool()
@_run_tool
def dagster_run_detail(client: DagsterClient, run_id: str) -> str:
    """Détail d'un run (son id) : statut, job, mode, timestamps, tags, config
    YAML, stats par step, et les 60 premiers événements (logs, succès/échecs
    de step, matérialisations avec leurs métadonnées, échecs de
    matérialisation). Pour la queue de runs : dagster_list_runs."""
    data = client.query(RUN_DETAIL, {"runId": run_id})
    payload = data.get("runOrError")
    if not payload:
        return f"Run introuvable : {run_id}"
    return _fmt({"run": _union(payload)})


@mcp.tool()
@_run_tool
def dagster_automations(
    client: DagsterClient,
    location: str | None = None,
    repository: str | None = None,
) -> str:
    """Schedules et sensors du repository : statut (RUNNING/STOPPED =
    activé/désactivé), cron, prochain tick prévu (et sa simulation :
    skipReason / runRequests), et les 5 derniers ticks avec leur résultat.
    Répond à « quelles automatisations vont se déclencher prochainement ?
    lesquelles sont désactivées ? »."""
    variables = _with_repo(client, location, repository)
    data = client.query(AUTOMATIONS, variables)
    return _fmt(
        {
            "schedules": _union(data.get("schedulesOrError")),
            "sensors": _union(data.get("sensorsOrError")),
        }
    )


@mcp.tool()
@_run_tool
def dagster_backfills(
    client: DagsterClient,
    limit: int = 20,
    status: str | None = None,
) -> str:
    """Backfills (matérialisations par lot) et leur progression : statut
    global (REQUESTED, COMPLETED, COMPLETED_FAILED, FAILED, CANCELED…),
    nombre de partitions, assets cibles, compteurs par statut de run et les
    runs récents du backfill. ``status`` filtre par statut."""
    variables: dict[str, Any] = {"limit": limit}
    if status:
        variables["status"] = status.strip().upper()
    data = client.query(BACKFILLS, variables)
    return _fmt({"backfills": _union(data.get("partitionBackfillsOrError"))})


@mcp.tool()
@_run_tool
def dagster_jobs(
    client: DagsterClient,
    location: str | None = None,
    repository: str | None = None,
) -> str:
    """Jobs/pipelines du repository (nom, description, isJob) et partition
    sets. Utile pour connaître les noms de jobs à passer à dagster_list_runs
    et la structure de partitionnement."""
    variables = _with_repo(client, location, repository)
    data = client.query(JOBS, variables)
    return _fmt({"repository": _union(data.get("repositoryOrError"))})


@mcp.tool()
def dagster_examples() -> str:
    """Requêtes GraphQL modèles documentées pour répondre aux questions
    courantes (erreurs récentes, partitions du mois, matérialisations en
    cours, automatisations, état des locations, lineage, métadonnées de
    run, ressources↔assets). Chaque exemple est adaptable : ajustez les
    champs, variables et filtres, puis lancez avec dagster_raw_query.

    Conventions du schéma Dagster 1.x :
    - workspaceOrError → locations → repositoryOrError(selector) : la plupart
      des requêtes « repo » exigent $loc/$repo (voir dagster_overview pour les
      valeurs).
    - Les champs terminés par OrError sont des unions : sélectionner
      « ... on <Type> { … } » et le cas d'erreur « ... on PythonError { message } ».
    - Timestamps : Float (epoch seconds). Statuts runs : RunStatus
      (QUEUED, NOT_STARTED, STARTING, STARTED, SUSPENDED, SUCCESS, FAILURE,
      CANCELING, CANCELED).
    """
    return (
        "Exemples disponibles (nom → description) :\n"
        + examples_doc()
        + "\n\nRécupérez le corps d'un exemple avec dagster_raw_query en "
        "collant le texte, ou adaptez-le directement. Les outils spécialisés "
        "(dagster_overview, dagster_asset_status, …) font déjà le travail "
        "pour les cas standards ; dagster_raw_query sert pour le sur-mesure."
    )


@mcp.tool()
@_run_tool
def dagster_raw_query(
    client: DagsterClient,
    query: str,
    variables: dict[str, Any] | None = None,
) -> str:
    """Exécute une requête GraphQL arbitraire sur l'API Dagster et retourne
    le champ « data » brut.

    DESTINÉ À L'AGENT : utilisez dagster_examples comme point de départ,
    adaptez les champs/variables, puis lancez ici. Le schéma est celui du
    webserver Dagster 1.x (workspaceOrError, runsOrError, assetNodeOrError,
    …).

    EN MODE READ_ONLY (défaut) : seules les opérations de lecture sont
    autorisées — toute mutation est refusée, y compris via cet outil. Les
    variables optionnelles (null) sont omises automatiquement.
    """
    if looks_like_write(query):
        if client._cfg.read_only:
            return (
                "Refusé : opération d'écriture en mode read_only — le mode "
                "lecture seule n'autorise AUCUNE modification, même via "
                "requête GraphQL. En mode read-write, utilisez plutôt un "
                "outil dédié (ex. dagster_materialize) ; une mutation "
                "brute via cet outil reste possible mais déconseillée."
            )
    return _fmt(client.query(query, variables))


# ── Outils d'écriture (read-write uniquement) ────────────────────────────────


@mcp.tool()
@_run_tool
def dagster_materialize(
    client: DagsterClient,
    assets: str,
    partitions: str | None = None,
    title: str | None = None,
    read_only: bool | None = None,
) -> str:
    """Matérialise un ou plusieurs assets via un backfill d'assets
    (mutation launchPartitionBackfill).

    ``assets`` : clés d'assets séparées par des virgules (ex.
    « ch.orders, ch.payments »). ``partitions`` : optionnel, noms de
    partitions séparés par des virgules (sinon : toutes les partitions si
    l'asset en a, sinon matérialisation simple). ``title`` : optionnel,
    titre du backfill.

    EXCLU EN MODE READ_ONLY (défaut) — c'est une écriture. Le serveur doit
    être configuré read_only: false (ou DAGSTER_MCP_READ_ONLY=0) ; le
    paramètre ``read_only`` sert de garde-fou explicite : passez
    read_only=false pour confirmer que vous savez ce que vous faites.
    Retourne le backfillId + les run ids lancés, ou l'erreur (conflict,
    permissions, partitions inconnues…).
    """
    # Verrou serveur (config) : s'il est en read_only, AUCUNE écriture, même
    # confirmée — on vérifie avant de lever allow_write.
    if client._cfg.read_only:
        raise ToolError(
            "Écriture refusée : le serveur est configuré en mode read_only "
            "(défaut). Le mode lecture seule ne doit autoriser AUCUNE "
            "modification. Configurez read_only: false "
            "(ou DAGSTER_MCP_READ_ONLY=0) pour activer les écritures."
        )
    if read_only is None:
        raise ToolError(
            "Confirmation requise : passez read_only=false pour exécuter "
            "cette écriture (et assurez-vous que le serveur est configuré "
            "read_only: false)."
        )
    if read_only:
        raise ToolError(
            "read_only=true : écriture refusée. Ce paramètre est un garde-fou ; "
            "passez read_only=false pour confirmer l'écriture."
        )
    asset_selection = _parse_assets(assets)
    variables: dict[str, Any] = {"assets": asset_selection, "title": title}
    if partitions:
        variables["partitionNames"] = [
            p.strip() for p in partitions.split(",") if p.strip()
        ]
    data = client.query(MATERIALIZE, variables, allow_write=True)
    return _fmt({"materialize": _union(data.get("launchPartitionBackfill"))})


if __name__ == "__main__":
    mcp.run()