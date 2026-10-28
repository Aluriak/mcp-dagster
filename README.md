# mcp-dagster

Un [MCP](https://modelcontextprotocol.io) pour interroger l'API GraphQL d'une
instance [Dagster](https://dagster.io) : vue d'ensemble, assets, états de
partitions, runs, sensors et schedules.

Lecture d'abord : toutes les opérations d'écriture sont **refusées par défaut**
(mode `read_only`). Le write reste possible, mais seulement en le débloquant
explicitement — la V1 ne fournit de toute façon que des outils de lecture.

## Installation

```bash
python -m venv .venv
.venv/bin/pip install .        # ou : pip install ".[dev]"
```

Le point d'entrée s'appelle `mcp-dagster` (stdio, compatible n'importe quel
client MCP : Claude, Hermes, …).

## Configuration

Deux façons, la dernière gagnant :

1. **Fichier YAML** — par défaut `./config.yaml`, puis
   `~/.config/mcp-dagster/config.yaml`, sinon le chemin de l'env
   `DAGSTER_MCP_CONFIG` :

   ```yaml
   base_url: "https://dagster.example.org"
   api_token: ""            # Bearer ; ne pas mettre en clair (voir env)
   timeout: 30
   read_only: true          # défaut : tout write GraphQL refusé
   extra_headers: {}
   ```

2. **Variables d'env** (recommandées pour le token) :

   | Variable | Champ |
   |---|---|
   | `DAGSTER_MCP_CONFIG` | chemin du fichier YAML |
   | `DAGSTER_MCP_BASE_URL` | `base_url` |
   | `DAGSTER_MCP_API_TOKEN` | `api_token` |
   | `DAGSTER_MCP_TIMEOUT` | `timeout` |
   | `DAGSTER_MCP_READ_ONLY` | `read_only` (`0`/`1`, `true`/`false`) |
   | `DAGSTER_MCP_PORT` | port HTTP du serveur conteneur (défaut `8000`) |

L'API est contactée sur `<base_url>/api` (l'UI Dagster est sur `/`).

> **Authentification derrière oauth2-proxy** : si l'instance passe par
> oauth2-proxy + un IdP (Keycloak, …), il faut un token que le proxy accepte.
> Si le client OIDC côté proxy est *confidential*, le password grant headless
> échoue sans son `client_secret` — dans ce cas un token provisionné (Vault,
> variable CI, ou un client OAuth dédié public) est la voie la plus simple.
> Vérifiez la nature du client OIDC de votre proxy avant de brancher un
> token.

## Outils (V1)

| Outil | Description |
|---|---|
| `dagster_overview` | nom du dépôt, jobs, 10 derniers runs, backfills, schedules |
| `dagster_list_assets` | assets : code version, dernière matérialisation, nb de partitions |
| `dagster_asset_state` | état + métadonnées d'un asset par clé (`ch.dataset` ou `ch/dataset`) |
| `dagster_list_runs` | runs, filtrables par `job_name` et/ou `status` |
| `dagster_run_detail` | détail d'un run : statut, job, tags, métadonnées, steps |
| `dagster_sensors_schedules` | sensors + schedules avec leur état |

## Exemple de déclaration (client MCP)

```json
{
  "mcpServers": {
    "dagster": {
      "command": "/chemin/.venv/bin/mcp-dagster",
      "env": {
        "DAGSTER_MCP_BASE_URL": "https://dagster.example.org",
        "DAGSTER_MCP_API_TOKEN": "<token>",
        "DAGSTER_MCP_READ_ONLY": "1"
      }
    }
  }
}
```

## Image conteneur

Le conteneur expose le serveur en **streamable-HTTP stateless** sur le port
`DAGSTER_MCP_PORT` (défaut `8000`) :

```bash
python -m mcp_dagster
```

Construire et lancer :

```bash
podman build -t mcp-dagster:local -f Containerfile .
podman run --rm -p 8000:8000 \
  -e DAGSTER_MCP_BASE_URL=https://dagster.example.org \
  -e DAGSTER_MCP_API_TOKEN=*** \
  -e DAGSTER_MCP_READ_ONLY=true \
  mcp-dagster:local
# endpoint MCP : http://localhost:8000/mcp
```

Extrait de compose pour LibreChat (serveur MCP HTTP) :

```yaml
services:
  mcp-dagster:
    image: ghcr.io/votre-org/mcp-dagster:latest
    environment:
      - DAGSTER_MCP_BASE_URL=https://dagster.example.org
      - DAGSTER_MCP_API_TOKEN=***          # via Vault, jamais en clair
      - DAGSTER_MCP_READ_ONLY=true
    restart: unless-stopped
```

L'image est publiée sur GHCR par la CI GitHub Actions (voir
`.github/workflows/ci.yml`) : tag immuable par commit, plus `latest` sur la
branche par défaut et par tag.

## Tests

```bash
.venv/bin/python -m pytest tests/ -q
```

Tous les tests sont hors-réseau (client httpx mocké).

## Limites connues / suite

- Les GraphQL sont calés sur l'API Dagster ~1.8 (à valider sur la version
  exacte de votre instance — le schema est légèrement différent selon les
  versions).
- Pas encore de `dagster_asset_logs` (logs de run) ni d'outils write
  (backfill, materialize, retry) : à ajouter une fois la lecture validée en
  production, derrière `read_only: false`.

## Licence

GPL-3.0-or-later — voir [LICENSE](LICENSE).