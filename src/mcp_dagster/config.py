"""Chargement de configuration : fichier YAML + surcharge par variables d'env.

Ordre de priorité (le dernier gagne) :
  1. valeurs par défaut
  2. fichier YAML (chemin : env DAGSTER_MCP_CONFIG, sinon ./config.yaml,
     sinon ~/.config/mcp-dagster/config.yaml)
  3. variables d'env ciblées (DAGSTER_MCP_BASE_URL, DAGSTER_MCP_API_TOKEN,
     DAGSTER_MCP_TIMEOUT, DAGSTER_MCP_READ_ONLY, DAGSTER_MCP_FORWARD_HEADER,
     DAGSTER_MCP_GRAPHQL_URL)

``graphql_url`` (point d'entrée GraphQL) : URL absolue du point d'entrée
GraphQL, pour les installations qui ne l'exposent pas sur le chemin par
défaut ``<base_url>/graphql``. Absent (défaut) : le client POST sur le
chemin relatif ``/graphql`` résolu contre ``base_url``.

``forward_header`` (mode HTTP partagé) : quand il est défini, le serveur ne
doit **pas** utiliser ``api_token`` ; il relaie, tel quel vers Dagster, le
header de la requête MCP entrante de ce nom (le credential de l'utilisateur
courant). ``forward_header`` et ``api_token`` sont mutuellement exclusifs :
les deux renseignés = erreur de configuration au démarrage.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ENV_PREFIX = "DAGSTER_MCP_"


def _is_absolute_url(url: str) -> bool:
    """Vrai si ``url`` est une URL absolue (scheme + netloc)."""
    from urllib.parse import urlparse

    p = urlparse(url)
    return bool(p.scheme in ("http", "https") and p.netloc)


def _default_config_paths() -> list[Path]:
    env_path = os.environ.get(ENV_PREFIX + "CONFIG")
    if env_path:
        return [Path(env_path)]
    return [
        Path("config.yaml"),
        Path.home() / ".config" / "mcp-dagster" / "config.yaml",
    ]


@dataclass
class Config:
    base_url: str = ""
    api_token: str = ""
    forward_header: str = ""
    graphql_endpoint: str = ""
    extra_headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0
    read_only: bool = True

    @classmethod
    def load(cls) -> "Config":
        data: dict[str, Any] = {}
        for path in _default_config_paths():
            if path.is_file():
                loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    data = loaded
                break

        cfg = cls(
            base_url=str(data.get("base_url", "")),
            api_token=str(data.get("api_token", "")),
            forward_header=str(data.get("forward_header", "") or ""),
            graphql_endpoint=str(data.get("graphql_url", "") or ""),
            extra_headers=dict(data.get("extra_headers") or {}),
            timeout=float(data.get("timeout", 30.0)),
            read_only=bool(data.get("read_only", True)),
        )

        # Surcharges ciblées par env.
        cfg.base_url = os.environ.get(ENV_PREFIX + "BASE_URL", cfg.base_url)
        cfg.api_token = os.environ.get(ENV_PREFIX + "API_TOKEN", cfg.api_token)
        cfg.forward_header = (
            os.environ.get(ENV_PREFIX + "FORWARD_HEADER") or cfg.forward_header
        ).strip()
        cfg.graphql_endpoint = (
            os.environ.get(ENV_PREFIX + "GRAPHQL_URL") or cfg.graphql_endpoint
        ).strip()
        if v := os.environ.get(ENV_PREFIX + "TIMEOUT"):
            cfg.timeout = float(v)
        if v := os.environ.get(ENV_PREFIX + "READ_ONLY"):
            cfg.read_only = v.strip().lower() not in {"0", "false", "no", "off"}
        return cfg

    @property
    def uses_forward_header(self) -> bool:
        """Vrai si le mode relay du jeton utilisateur est actif."""
        return bool(self.forward_header)

    def validate(self) -> None:
        if not self.base_url:
            raise ValueError(
                "base_url manquant : définissez-le dans la config "
                f"(fichier YAML) ou via {ENV_PREFIX}BASE_URL."
            )
        if self.graphql_endpoint and not _is_absolute_url(self.graphql_endpoint):
            raise ValueError(
                "graphql_url doit être une URL absolue (ex. "
                "https://dagster.chu-brest.fr/graphql) : "
                f"reçu {self.graphql_endpoint!r}."
            )
        if self.api_token and self.forward_header:
            raise ValueError(
                "api_token et forward_header sont mutuellement exclusifs : "
                "ne définissez qu'un seul des deux (mode token statique "
                f"vs mode relay du jeton utilisateur). api_token={bool(self.api_token)}, "
                f"forward_header={self.forward_header!r}."
            )

    @property
    def graphql_url(self) -> str:
        """URL effective du point d'entrée GraphQL.

        Si ``graphql_endpoint`` est renseigné (config/env ``graphql_url``), il
        est utilisé tel quel (URL absolue). Sinon, chemin relatif ``/graphql``
        résolu contre ``base_url`` (layout par défaut d'une instance Dagster).
        """
        if self.graphql_endpoint:
            return self.graphql_endpoint
        return self.base_url.rstrip("/") + "/graphql"

    def outbound_auth_header(self) -> str | None:
        """Valeur du header ``Authorization`` à poser sur l'appel sortant vers
        Dagster, pour la requête courante.

        - Mode ``forward_header`` (HTTP partagé) : relaie tel quel la valeur du
          header de la requête MCP entrante de ce nom (le jeton de l'utilisateur
          courant). Retourne ``None`` s'il est absent/vide — c'est le client qui
          devra alors **échouer explicitement**, jamais replier sur ``api_token``.
        - Mode ``api_token`` (statique, stdio) : ``Bearer <token>``.
        - Aucun des deux : ``None`` (appel non authentifié, comportement V1).

        Note : ``forward_header`` et ``api_token`` étant exclusifs (``validate``),
        ce sont les deux branches d'un switch mutuellement exclusif.
        """
        if self.forward_header:
            from .request_ctx import get_request_headers

            return get_request_headers().get(self.forward_header)
        if self.api_token:
            return f"Bearer {self.api_token}"
        return None