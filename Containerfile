# mcp-dagster — MCP server pour interroger l'API GraphQL d'une instance Dagster.
#
# Une seule image, deux transports selon l'entrée :
#   - stdio  : `mcp-dagster` (script) — usage local / orchestrateur MCP en stdio
#   - HTTP   : `python -m mcp_dagster` — défaut du conteneur (streamable-HTTP,
#              stateless, port DAGSTER_MCP_PORT, défaut 8080) pour LibreChat
# et les clients MCP HTTP.
#
# Lecture seule par défaut (DAGSTER_MCP_READ_ONLY, défaut "true") : les outils
# d'écriture GraphQL sont refusés, indépendamment du transport.

FROM python:3.11-slim AS build

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir --root-user-action=ignore build && \
    python -m build --wheel --outdir /dist

FROM python:3.11-slim

WORKDIR /app
COPY --from=build /dist/*.whl /tmp/
RUN pip install --no-cache-dir --root-user-action=ignore /tmp/*.whl && \
    rm -rf /tmp/*.whl

# Non-root : rien n'a besoin de root ici.
RUN useradd --system --no-create-home --uid 10001 mcp-dagster
USER mcp-dagster

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DAGSTER_MCP_READ_ONLY=true

EXPOSE 8080

# Santé : l'endpoint streamable-HTTP renvoie 400/405 sur un GET nu — c'est
# sain tant qu'on a une réponse HTTP (un 404/connexion refusée signifierait
# que le serveur ne tourne pas). On traite HTTPError comme « OK ».
COPY scripts/healthcheck.py /app/scripts/healthcheck.py
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "/app/scripts/healthcheck.py"]

CMD ["python", "-m", "mcp_dagster"]