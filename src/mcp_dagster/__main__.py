"""Point d'entrée conteneur : transport HTTP (stateless).

En local / stdio on passe par le script `mcp-dagster`
(``mcp_dagster.server:mcp.run`` en stdio). En conteneur, LibreChat (ou tout
autre client MCP HTTP) consomme le serveur sur son port, donc on force le
transport HTTP stateless :

    python -m mcp_dagster

Mode stateless : pas d'état de session persistant côté serveur, chaque
requête est autonome — c'est le mode adapté à un conteneur derrière un load
balancer, et c'est ce qu'attendent les clients HTTP MCP (LibreChat).

Configuration (variables d'env) :
  DAGSTER_MCP_BASE_URL  : URL de l'instance Dagster (obligatoire)
  DAGSTER_MCP_API_TOKEN : token d'API Dagster (optionnel)
  DAGSTER_MCP_TIMEOUT   : timeout httpx en secondes (défaut 30)
  DAGSTER_MCP_READ_ONLY : "true" (défaut) refuse toute écriture GraphQL
  DAGSTER_MCP_PORT      : port HTTP du serveur (défaut 8000)
"""

from __future__ import annotations

import os

from .server import mcp

_PORT_DEFAULT = 8000


def _port() -> int:
    raw = os.environ.get("DAGSTER_MCP_PORT", "").strip()
    if not raw:
        return _PORT_DEFAULT
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"DAGSTER_MCP_PORT invalide : {raw!r} (attendu : entier)")


def main() -> None:
    # host/port vivent dans les Settings de FastMCP ; on les fixe ici
    # explicitement plutôt que de compter sur les variables FASTMCP_*.
    # host=0.0.0.0 : en conteneur il faut écouter toutes les interfaces,
    # pas 127.0.0.1 (défaut FastMCP). stateless : voir docstring.
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = _port()
    mcp.settings.stateless_http = True
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()