"""Client GraphQL minimal pour l'API Dagster.

L'API Dagster est une API GraphQL unique sur <base_url>/api. On POST des requêtes
documentées avec variables ; les erreurs reviennent dans le champ « errors » de la
réponse (pas un code HTTP d'erreur).
"""

from __future__ import annotations

import re
from typing import Any

import httpx

from .config import Config

# Motifs d'opérations d'écriture de l'API Dagster. Les mots-clés sont
# distingués (première lettre majuscule = mutation/tool call en GraphQL) ;
# on ne cherche pas les parenthèses pour rester robuste aux formats.
_WRITE_PATTERNS = [
    re.compile(r"\bmutation\b", re.IGNORECASE),
    re.compile(r"\b(startRun|startRuns|deleteRuns|retryRun|cancelRun|stopRun|stepRetries|applyWorkspaceChange|applyWorkspaceChanges|setSensorState|deleteSensor|createRepositoryLocation|deleteRepositoryLocation|updateJob|setRunTag|addRunTag|removeRunTag|pauseAutoResume|resumeAutoResume|applyAutoMaterialPolicy|scheduleRun|backfill|createBackfill|cancelBackfill|deleteBackfill|addJobBackfill|removeJobBackfill|writeAsset|deleteAsset|recreateAsset|materializeAssets)\b"),
]


class DagsterError(RuntimeError):
    """Erreur GraphQL Dagster (champ errors de la réponse)."""

    def __init__(self, errors: list[dict[str, Any]]):
        self.errors = errors
        detail = "; ".join(str(e.get("message", e)) for e in errors[:5])
        super().__init__(f"Erreur GraphQL Dagster : {detail}")


def looks_like_write(query: str) -> bool:
    """Vrai si la requête GraphQL contient un opérateur d'écriture connu."""
    return any(p.search(query) for p in _WRITE_PATTERNS)


class DagsterClient:
    def __init__(self, cfg: Config, client: httpx.Client | None = None):
        cfg.validate()
        self._cfg = cfg
        self._client = client or httpx.Client(
            base_url=cfg.base_url, timeout=cfg.timeout
        )

    def close(self) -> None:
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
                "Refusé : opération d'écriture en mode read_only "
                "(désactiver via DAGSTER_MCP_READ_ONLY=0 ou "
                "read_only: false dans la config)."
            )
        # GraphQL : une variable transmise explicitement en null NE PASSE
        # l'argument (l'argument devient null au lieu d'être omis). On ne
        # transmet donc que les valeurs non-None.
        variables = {k: v for k, v in (variables or {}).items() if v is not None}
        resp = self._client.post(
            "/api",
            json={"query": query, "variables": variables},
            headers=self._cfg.auth_headers(),
        )
        if resp.status_code != 200:
            raise DagsterError(
                [
                    {
                        "message": (
                            f"HTTP {resp.status_code} sur l'API Dagster "
                            f"({self._cfg.base_url}/api) : "
                            f"{resp.text[:200]}"
                        )
                    }
                ]
            )
        body = resp.json()
        if body.get("errors"):
            raise DagsterError(body["errors"])
        return body.get("data") or {}