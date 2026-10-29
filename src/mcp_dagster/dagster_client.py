"""Client GraphQL minimal pour l'API Dagster.

L'API Dagster est une API GraphQL unique, exposée par défaut sur
`<base_url>/graphql` (layout par défaut d'une instance Dagster 1.x, ex.
1.12). Le point d'entrée reste paramétrable (config `graphql_url` / env
`DAGSTER_MCP_GRAPHQL_URL`) pour les installations qui l'exposent ailleurs.
On POST des requêtes documentées avec variables ; les erreurs reviennent
dans le champ « errors » de la réponse (pas un code HTTP d'erreur).

L'API 1.x est organisée en deux niveaux : workspace → locations →
repositories. La plupart des requêtes exigent donc un
`RepositorySelector { repositoryName, repositoryLocationName }`. Le
méthode `workspace()` (avec cache par client) découvre automatiquement
les locations et les repositories disponibles.
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from .config import Config

# Motifs d'opérations d'écriture de l'API Dagster 1.x. Les mots-clés sont
# distingués (première lettre majuscule = mutation en GraphQL) ; on ne
# cherche pas les parenthèses pour rester robuste aux formats.
_WRITE_PATTERNS = [
    re.compile(r"\bmutation\b", re.IGNORECASE),
    re.compile(
        r"\b(launchRun|startRun|launchMultipleRuns|launchRunReexecution|startSchedule|"
        r"stopRunningSchedule|resetSchedule|startSensor|stopSensor|resetSensor|"
        r"setSensorCursor|sensorDryRun|scheduleDryRun|terminateRun|terminateRuns|"
        r"deletePipelineRun|deleteRun|deleteRuns|reloadRepositoryLocation|"
        r"reloadWorkspace|shutdownRepositoryLocation|wipeAssets|"
        r"reportRunlessAssetEvents|reportAssetCheckEvaluations|"
        r"launchPartitionBackfill|resumePartitionBackfill|"
        r"reexecutePartitionBackfill|cancelPartitionBackfill|"
        r"setAutoMaterializePaused|freeConcurrencySlotsForRun|setRunTag|"
        r"addRunTag|removeRunTag|setSensorState|deleteSensor|applyWorkspaceChange|"
        r"applyWorkspaceChanges|createRepositoryLocation|deleteRepositoryLocation|"
        r"scheduleRun|backfill|createBackfill|cancelBackfill|deleteBackfill|"
        r"materializeAssets|writeAsset|deleteAsset|recreateAsset|stopRun|"
        r"retryRun|cancelRun|updateJob|stepRetries)\b"
    ),
]

# Requête de découverte du workspace (API 1.x) : toutes les locations et
# les repositories de chacune. Aucune donnée de business, pas de secrets.
_WORKSPACE_QUERY = """
query {
  workspaceOrError {
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        locationOrLoadError {
          ... on RepositoryLocation {
            name
            repositories { name }
          }
          ... on PythonError {
            message
            className
          }
        }
      }
    }
    ... on PythonError {
      message
      className
    }
  }
}
"""


class DagsterError(RuntimeError):
    """Erreur GraphQL Dagster (champ errors de la réponse)."""

    def __init__(self, errors: list[dict[str, Any]]):
        self.errors = errors
        detail = "; ".join(str(e.get("message", e)) for e in errors[:5])
        super().__init__(f"Erreur GraphQL Dagster : {detail}")


class ForwardAuthError(RuntimeError):
    """Le mode ``forward_header`` est actif mais la requête MCP entrante ne
    porte pas le header attendu. Échec explicite — jamais de repli sur un
    token statique (qui élargirait les droits de l'utilisateur)."""


def looks_like_write(query: str) -> bool:
    """Vrai si la requête GraphQL contient un opérateur d'écriture connu."""
    return any(p.search(query) for p in _WRITE_PATTERNS)


def _union_payload(data: dict[str, Any]) -> Any:
    """Retourne le membre d'une union `...OrError` (premier membre
    non-erreur de la data) ou None si la réponse est vide.

    Les unions Dagster ont un seul membre dans la réponse ; le membre
    d'erreur (PythonError, NotFoundError…) est renvoyé tel quel afin que
    l'appelant puisse en extraire le message.
    """
    if not data:
        return None
    for key, value in data.items():
        if value is not None:
            return value
    return None


class DagsterClient:
    def __init__(self, cfg: Config, client: httpx.Client | None = None):
        cfg.validate()
        self._cfg = cfg
        self._client = client or httpx.Client(
            base_url=cfg.base_url, timeout=cfg.timeout
        )
        self._workspace_cache: list[dict[str, Any]] | None = None

    def _request_headers(self) -> dict[str, str]:
        """Headers de l'appel sortant vers Dagster.

        En mode ``forward_header``, échoue explicitement (``ForwardAuthError``)
        si la requête MCP entrante ne porte pas le header attendu : aucun repli
        silencieux sur ``api_token`` (exclu de toute façon par ``validate``).
        """
        headers: dict[str, str] = {"Content-Type": "application/json"}
        auth = self._cfg.outbound_auth_header()
        if self._cfg.uses_forward_header and not auth:
            raise ForwardAuthError(
                f"Requête MCP entrante sans le header {self._cfg.forward_header!r} "
                "en mode forward_header : impossible de relayer le jeton de "
                "l'utilisateur vers Dagster. Aucun repli sur un token statique "
                "n'est effectué (sécurité : le serveur ne doit jamais élargir "
                "les droits d'un utilisateur au-delà de ce que l'infrastructure "
                "lui accorde déjà)."
            )
        if auth:
            headers["Authorization"] = auth
        headers.update(self._cfg.extra_headers)
        return headers

    def close(self) -> None:
        self._workspace_cache = None
        self._client.close()

    def __enter__(self) -> "DagsterClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        *,
        allow_write: bool = False,
    ) -> dict[str, Any]:
        """Exécuter une requête GraphQL et retourner le champ « data ».

        Lève DagsterError si la réponse porte des erreurs GraphQL.
        Lève PermissionError si read_only est actif et que la requête est
        une opération d'écriture (sauf allow_write explicite).
        """
        if self._cfg.read_only and not allow_write and looks_like_write(query):
            raise PermissionError(
                "Refusé : opération d'écriture en mode read_only — le mode "
                "lecture seule ne doit autoriser AUCUNE modification, même "
                "via GraphQL (désactiver via DAGSTER_MCP_READ_ONLY=0 ou "
                "read_only: false dans la config)."
            )
        # GraphQL : une variable transmise explicitement en null NE PASSE
        # l'argument (l'argument devient null au lieu d'être omis). On ne
        # transmet donc que les valeurs non-None.
        variables = {k: v for k, v in (variables or {}).items() if v is not None}
        # Point d'entrée GraphQL : URL effective de la config (graphql_url),
        # soit l'URL absolue configurée, soit <base_url>/graphql par défaut.
        # httpx accepte une URL absolue : elle prime sur base_url.
        resp = self._client.post(
            self._cfg.graphql_url,
            json={"query": query, "variables": variables},
            headers=self._request_headers(),
        )
        if resp.status_code != 200:
            raise DagsterError(
                [
                    {
                        "message": (
                            f"HTTP {resp.status_code} sur l'API Dagster "
                            f"({self._cfg.graphql_url}) : "
                            f"{resp.text[:200]}"
                        )
                    }
                ]
            )
        body = resp.json()
        if body.get("errors"):
            raise DagsterError(body["errors"])
        return body.get("data") or {}

    # ── Découverte du workspace (API 1.x) ──────────────────────────────────

    def workspace(self) -> list[dict[str, Any]]:
        """Découvre le workspace et retourne la liste des repositories.

        Chaque entrée : {location, repository, loadStatus, error?}.
        Mis en cache par client (le workspace ne change pas d'une query
        à l'autre dans la durée de vie d'un tool).
        """
        if self._workspace_cache is not None:
            return self._workspace_cache
        data = self.query(_WORKSPACE_QUERY)
        result: list[dict[str, Any]] = []
        payload = _union_payload(data)
        if payload is None:
            self._workspace_cache = result
            return result
        if "message" in payload and "locationEntries" not in payload:
            raise DagsterError(
                [{"message": f"Workspace indisponible : {payload['message']}"}]
            )
        for entry in payload.get("locationEntries") or []:
            location = entry.get("name")
            loc_payload = _union_payload(
                {"x": entry.get("locationOrLoadError")}
            )
            if loc_payload is None or "repositories" not in loc_payload:
                result.append(
                    {
                        "location": location,
                        "repository": None,
                        "loadStatus": entry.get("loadStatus"),
                        "error": (
                            loc_payload.get("message")
                            if loc_payload
                            else "location non chargée"
                        ),
                    }
                )
                continue
            for repo in loc_payload.get("repositories") or []:
                result.append(
                    {
                        "location": location,
                        "repository": repo.get("name"),
                        "loadStatus": entry.get("loadStatus"),
                    }
                )
        self._workspace_cache = result
        return result

    def repository_selector(
        self,
        location: str | None = None,
        repository: str | None = None,
    ) -> dict[str, str]:
        """Retourne un RepositorySelector {repositoryLocationName,
        repositoryName}.

        Sans arguments : la première location chargée avec au moins un
        repository (les instances monodépôt — le cas courant du CDC —
        n'ont qu'une seule réponse possible). Si plusieurs repositories
        sont disponibles, lève une erreur listant les choix pour que
        l'appelant (ou l'agent) tranche explicitement.
        """
        repos = self.workspace()
        available = [
            (r["location"], r["repository"])
            for r in repos
            if r.get("repository")
        ]
        if not available:
            errors = [
                f"{r['location']}: {r.get('error')}"
                for r in repos
                if r.get("error")
            ]
            detail = (" ; ".join(errors) if errors else "aucune location")
            raise DagsterError(
                [{"message": f"Aucun repository accessible dans le workspace ({detail})."}]
            )
        candidates = available
        if location:
            candidates = [c for c in candidates if c[0] == location]
            if not candidates:
                raise DagsterError(
                    [{"message": f"Location introuvable : {location} "
                                 f"(disponibles : {sorted(set(c[0] for c in available))})"}]
                )
        if repository:
            candidates = [c for c in candidates if c[1] == repository]
            if not candidates:
                raise DagsterError(
                    [{"message": f"Repository introuvable : {repository} "
                                 f"(disponibles : {sorted(set(c[1] for c in available))})"}]
                )
        if len(candidates) > 1:
            listing = ", ".join(f"{loc} / {repo}" for loc, repo in candidates)
            raise DagsterError(
                [
                    {
                        "message": (
                            "Plusieurs repositories accessibles — précisez "
                            f"location/repository. Disponibles : {listing}"
                        )
                    }
                ]
            )
        loc, repo = candidates[0]
        return {
            "repositoryLocationName": loc,
            "repositoryName": repo,
        }

    def describe(self, data: dict[str, Any] | None) -> dict[str, Any]:
        """Alias utile : retourne data en json-serialisable (identité)."""
        return data or {}


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, default=str)