"""Contexte de la requête MCP entrante (mode HTTP stateless).

En mode stdio, il n'y a qu'un seul utilisateur local : un `api_token` statique
suffit. En mode conteneur (streamable-HTTP stateless), le serveur est **partagé**
par tous les utilisateurs de LibreChat via un seul process : le credential
Dagster doit donc être celui de **l'utilisateur de la requête courante**, relayé
par un header (ex. `Authorization`) posé par l'infra en amont.

Le protocole MCP ne transmet pas les headers HTTP aux tool calls : seul le
middleware ASGI les voit. Ce module fournit donc un canal propre — un
``contextvar`` — que le middleware remplit à l'entrée de la requête et que le
client Dagster lit au moment de l'appel sortant.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Mapping

__all__ = [
    "CURRENT_REQUEST_HEADERS",
    "RequestHeaders",
    "set_request_headers",
    "get_request_headers",
]

# None = transport sans contexte HTTP (stdio) ou requête non instrumentée.
# Stocke l'objet ``RequestHeaders`` (déclaré juste en dessous).
CURRENT_REQUEST_HEADERS: ContextVar["RequestHeaders | None"] = ContextVar(
    "mcp_dagster_request_headers", default=None
)


class RequestHeaders:
    """Vue sur les headers HTTP entrants, insensible à la casse.

    Stocke chaque nom en minuscules et normalise les valeurs (str, non vide).
    L'accès ``get(name)`` fonctionne quelle que soit la casse de ``name``.
    """

    __slots__ = ("_lower",)

    def __init__(self, headers: Mapping[str, str] | None):
        self._lower: dict[str, str] = {}
        if not headers:
            return
        for k, v in headers.items():
            if v is None:
                continue
            s = str(v).strip()
            if s:
                self._lower[str(k).lower()] = s

    def get(self, name: str | None) -> str | None:
        """Retourne la valeur du header ``name`` (insensible à la casse),
        ou ``None`` s'il est absent ou vide."""
        if not name:
            return None
        v = self._lower.get(str(name).lower())
        return v if v else None

    def __bool__(self) -> bool:
        return bool(self._lower)

    def __repr__(self) -> str:  # pragma: no cover - debug uniquement
        return f"RequestHeaders({sorted(self._lower)!r})"


def set_request_headers(headers: Mapping[str, str] | None) -> None:
    """Remplit le contexte de la requête courante (appelé par le middleware)."""
    CURRENT_REQUEST_HEADERS.set(RequestHeaders(headers))


def get_request_headers() -> RequestHeaders:
    """Retourne les headers de la requête courante (vide si stdio/non-instr.)."""
    v = CURRENT_REQUEST_HEADERS.get()
    return v if isinstance(v, RequestHeaders) else RequestHeaders(None)