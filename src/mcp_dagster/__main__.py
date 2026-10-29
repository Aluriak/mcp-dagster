"""Point d'entrée conteneur : transport HTTP (stateless).

En local / stdio on passe par le script `mcp-dagster`
(``mcp_dagster.server:mcp.run`` en stdio, transport par défaut de FastMCP).
En conteneur, LibreChat (ou tout autre client MCP HTTP) consomme le serveur
sur son port, donc on force le transport HTTP stateless :

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

Le middleware est greffé via l'API native de FastMCP
(``http_app(middleware=[Middleware(...)])`` / ``run(..., middleware=...)``) :
il s'exécute dans le contexte asyncio de la requête, donc le contextvar posé
dans ``HeaderCaptureMiddleware`` est visible par toute la chaîne de tool calls
servie par cette requête. (Validé de bout en bout, y compris le passage par
le lifespan et le session manager — voir tests/test_http_host.py.)

Header Host / réseau conteneur
------------------------------
Ce service tourne sur un réseau compose interne et n'est pas exposé
publiquement ; l'authentification réelle est le relay du jeton utilisateur.
FastMCP 3.x n'applique aucun contrôle du header Host par défaut, donc un
client qui envoie ``Host: <conteneur>:8000`` (nom du conteneur, pas un
loopback) est accepté sans aucune configuration. (L'ancien SDK ``mcp`` 1.x
auto-activait une protection anti-DNS-rebinding qui provoquait un
``421 Invalid Host header`` derrière un réseau de conteneurs ; le port sur
FastMCP 3.x supprime cette classe de bug à la racine.)
"""

from __future__ import annotations

import os

from starlette.middleware import Middleware
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

    Le middleware est instancié par FastMCP sous la forme
    ``Middleware(HeaderCaptureMiddleware)`` (Starlette l'appelle avec l'app
    interne en argument) ; il instrumente chaque requête HTTP pour appeler
    ``set_request_headers`` dans le contexte asyncio de la requête. Le
    contextvar est alors visible de toute la chaîne de tool calls servie par
    cette requête.
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


def _http_kwargs() -> dict:
    """Réglages HTTP du mode conteneur, communs à ``build_app()`` et
    ``main()`` : stateless + middleware de capture des headers.

    ``host``/``port`` ne figurent pas ici : ils ne s'appliquent qu'au bind
    réseau, donc seulement dans ``main()`` (``build_app()`` retourne l'ASGI
    app sans la servir).
    """
    return {
        "stateless_http": True,
        "middleware": [Middleware(HeaderCaptureMiddleware)],
    }


def build_app() -> ASGIApp:
    """App ASGI du transport HTTP : l'app streamable-http de FastMCP avec le
    middleware de capture des headers greffé. Séparée de ``main()`` pour être
    testable sans lancer uvicorn."""
    return mcp.http_app(**_http_kwargs())


def main() -> None:
    # Le SDK sert lui-même l'app (uvicorn + lifespan + session manager) ; on
    # passe juste les réglages de bind (host 0.0.0.0 = joignable sur le réseau
    # compose) et le port, plus les réglages HTTP communs.
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=_port(),
        **_http_kwargs(),
    )


if __name__ == "__main__":
    main()