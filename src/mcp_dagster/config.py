"""Chargement de configuration : fichier YAML + surcharge par variables d'env.

Ordre de priorité (le dernier gagne) :
  1. valeurs par défaut
  2. fichier YAML (chemin : env DAGSTER_MCP_CONFIG, sinon ./config.yaml,
     sinon ~/.config/mcp-dagster/config.yaml)
  3. variables d'env ciblées (DAGSTER_MCP_BASE_URL, DAGSTER_MCP_API_TOKEN,
     DAGSTER_MCP_TIMEOUT, DAGSTER_MCP_READ_ONLY)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ENV_PREFIX = "DAGSTER_MCP_"


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
            extra_headers=dict(data.get("extra_headers") or {}),
            timeout=float(data.get("timeout", 30.0)),
            read_only=bool(data.get("read_only", True)),
        )

        # Surcharges ciblées par env.
        cfg.base_url = os.environ.get(ENV_PREFIX + "BASE_URL", cfg.base_url)
        cfg.api_token = os.environ.get(ENV_PREFIX + "API_TOKEN", cfg.api_token)
        if v := os.environ.get(ENV_PREFIX + "TIMEOUT"):
            cfg.timeout = float(v)
        if v := os.environ.get(ENV_PREFIX + "READ_ONLY"):
            cfg.read_only = v.strip().lower() not in {"0", "false", "no", "off"}
        return cfg

    def validate(self) -> None:
        if not self.base_url:
            raise ValueError(
                "base_url manquant : définissez-le dans la config "
                f"(fichier YAML) ou via {ENV_PREFIX}BASE_URL."
            )

    @property
    def graphql_url(self) -> str:
        return self.base_url.rstrip("/") + "/api"

    def auth_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        headers.update(self.extra_headers)
        return headers