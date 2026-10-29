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

DNS rebinding / contrôle du header Host
---------------------------------------
Le SDK MCP (FastMCP) active automatiquement la protection anti-DNS-rebinding
quand le serveur est construit avec ``host=127.0.0.1`` (sa valeur par
défaut) : ``allowed_hosts`` se fige sur les loopbacks ET n'est plus
recalculée quand ``main()`` passe ``host=0.0.0.0``. Derrière un réseau de
conteneurs, le client (LibreChat) envoie ``Host: <conteneur>:8000`` → le
SDK répond **421 Misdirected Request** avant même l'``initialize``.

Cette protection vise un serveur MCP exposé directement sur le web derrière
un domaine DNS ; ici le service est sur un réseau compose interne et l'auth
réelle est le relay du jeton utilisateur. ``main()`` désactive donc
explicitement le contrôle Host/Origin (``transport_security``) — c'est une
décision de posture, pas un oubli.
"""

from __future__ import annotations

import os

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.types import ASGIApp

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


def _configure_http(mcp) -> None:
    """Réglages FastMCP du mode conteneur, avant la création de l'app ASGI.

    ``transport_security = None`` est le cœur du correctif du 421 : FastMCP
    auto-active la protection anti-DNS-rebinding parce qu'il est construit
    avec ``host=127.0.0.1`` (défaut), et ``allowed_hosts`` reste figé sur les
    loopbacks quand on passe à ``0.0.0.0``. Derrière un réseau compose, le
    client (LibreChat) envoie ``Host: <conteneur>:8000`` → le SDK répond
    421 « Invalid Host header » avant même l'``initialize``. Service interne
    (non exposé publiquement), authentifié par relay du jeton utilisateur :
    contrôle Host/Origin désactivé explicitement (``None`` = la posture par
    défaut du SDK, protection éteinte).
    """
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = _port()
    mcp.settings.stateless_http = True
    mcp.settings.transport_security = None


def build_app() -> ASGIApp:
    """App ASGI du transport HTTP : middleware de capture des headers posé
    sur l'app streamable-http de FastMCP. Séparé de ``main()`` pour être
    testable sans lancer uvicorn."""
    _configure_http(mcp)
    inner: Starlette = mcp.streamable_http_app()
    return HeaderCaptureMiddleware(inner)


def main() -> None:
    # On wrap l'app ASGI du transport pour y greffer la capture des headers.
    # ``streamable_http_app()`` renvoie l'ASGI app interne ; on l'installe
    # derrière notre middleware, puis on sert avec uvicorn (mêmes paramètres
    # que ``FastMCP.run(transport="streamable-http")``).
    import uvicorn

    app = build_app()

    config = uvicorn.Config(
        app,
        host=mcp.settings.host,
        port=mcp.settings.port,
        log_level=mcp.settings.log_level.lower(),
    )
    uvicorn.Server(config).run()


if __name__ == "__main__":
    main()