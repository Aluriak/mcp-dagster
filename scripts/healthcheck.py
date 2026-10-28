#!/usr/bin/env python3
"""Healthcheck conteneur mcp-dagster.

L'endpoint /mcp (streamable-HTTP MCP) ne répond pas 200 à un GET nu :
  - 406 si le client n'envoie pas les bons Content-Type/Accept,
  - 400/405 sinon.
Toutes ces réponses HTTP prouvent que le serveur écoute → sain (exit 0).
Une connection refused / timeout / autre exception → non sain (exit 1).
"""
from __future__ import annotations

import os
import urllib.error
import urllib.request

PORT = int(os.environ.get("DAGSTER_MCP_PORT", "8000"))
URL = f"http://127.0.0.1:{PORT}/mcp"


def main() -> int:
    try:
        urllib.request.urlopen(URL, timeout=4)
        return 0  # 200 : sain
    except urllib.error.HTTPError:
        return 0  # réponse HTTP (4xx/5xx) : le serveur écoute → sain
    except Exception:
        return 1  # rien n'écoute / timeout → non sain


if __name__ == "__main__":
    raise SystemExit(main())