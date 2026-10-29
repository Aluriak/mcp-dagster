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
  DAGSTER_MCP_BASE_URL      : URL de l'instance Dagster (obligatoire)
  DAGSTER_MCP_API_TOKEN     : token d'API Dagster (mode statique, exclusif
                               avec DAGSTER_MCP_FORWARD_HEADER)
  DAGSTER_MCP_FORWARD_HEADER: nom du header de la requête MCP entrante à
                               relayer tel quel comme Authorization vers
                               Dagster (mode relay du jeton utilisateur)
  DAGSTER_MCP_TIMEOUT       : timeout httpx en secondes (défaut 30)
  DAGSTER_MCP_READ_ONLY     : "true" (défaut) refuse toute écriture GraphQL
  DAGSTER_MCP_PORT          : port HTTP du serveur (défaut 8000)

Relay du jeton utilisateur
--------------------------
En mode partagé (un seul process pour tous les utilisateurs), le credential
Dagster ne doit pas être un token statique partagé : chaque requête MCP
entrante doit porter le jeton de **son** utilisateur (posé par l'infra, ex.
oauth2-proxy / LibreChat) dans le header nommé par ``forward_header``.

Le protocole MCP ne transmet pas les headers HTTP aux tool calls : seuls les
voit le middleware ASGI. Celui-ci capture donc les headers de la requête
entrante dans un contextvar (``request_ctx``), que le client Dagster lit au
moment de l'appel GraphQL sortant pour poser la valeur du header comme
``Authorization`` (relay tel quel). Si le header est absent, l'appel échoue
explicitement — jamais de repli sur un token statique.
"""

from __future__ import annotations

import os

from starlette.applications import Starlette
from starlette.requests import Request

from .request_ctx import set_request_headers
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


class HeaderCaptureMiddleware:
    """ASGI middleware qui capture les headers HTTP entrants dans le contextvar
    de la requête courante.

    Chacune des branches ASGI (http / websocket / lifecycle) est instrumentée
    pour appeler ``set_request_headers`` sur la requête, dans le contexte
    asyncio de celle-ci. Le contextvar est alors visible de toute la chaîne de
    tool calls servie par cette requête.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "websocket"):
            req = Request(scope, receive=receive, send=send)
            headers = dict(req.headers.items())
            # Le contextvar est posé dans LE contexte de cette requête :
            # chaque coroutine enfant (le traitement MCP) l'hérite.
            set_request_headers(headers)
        await self.app(scope, receive, send)


def main() -> None:
    # host/port vivent dans les Settings de FastMCP ; on les fixe ici
    # explicitement plutôt que de compter sur les variables FASTMCP_*.
    # host=0.0.0.0 : en conteneur il faut écouter toutes les interfaces,
    # pas 127.0.0.1 (défaut FastMCP). stateless : voir docstring.
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = _port()
    mcp.settings.stateless_http = True

    # On wrap l'app ASGI du transport pour y greffer la capture des headers.
    # ``streamable_http_app()`` renvoie l'ASGI app interne ; on l'installe
    # derrière notre middleware, puis on sert avec uvicorn (mêmes paramètres
    # que ``FastMCP.run(transport="streamable-http")``).
    import uvicorn

    inner: Starlette = mcp.streamable_http_app()
    app = HeaderCaptureMiddleware(inner)

    config = uvicorn.Config(
        app,
        host=mcp.settings.host,
        port=mcp.settings.port,
        log_level=mcp.settings.log_level.lower(),
    )
    uvicorn.Server(config).run()


if __name__ == "__main__":
    main()