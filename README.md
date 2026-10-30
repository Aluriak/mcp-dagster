# mcp-dagster

Un [MCP](https://modelcontextprotocol.io) pour interroger l'API GraphQL
1.x d'une instance [Dagster](https://dagster.io) : workspace/code
locations, assets, états de partitions, lineage, runs, automatisations
(schedules/sensors), backfills.

**Design V2** : le serveur privilégie la **latitude de l'agent**. Les
outils spécialisés couvrent les questions courantes ; `dagster_examples`
fournit des requêtes GraphQL modèles documentées que l'agent peut
**adapter, combiner et lancer** via `dagster_raw_query` (accès au schéma
complet). Toutes les requêtes embarquées sont validées contre le vrai
schéma 1.12.1 (fixture `tests/schema_1_12_1.graphql`).

Lecture d'abord : en mode `read_only` (défaut), **aucune** opération
d'écriture n'est autorisée — y compris une mutation transmise via
`dagster_raw_query`. La seule écriture fournie est `dagster_materialize`,
derrière `read_only: false` ET une confirmation explicite
(`read_only=false` au niveau du tool).

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
   forward_header: ""       # mode relay du jeton utilisateur (voir plus bas)
   graphql_url: ""          # URL absolue du point d'entrée GraphQL (défaut : <base_url>/graphql)
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
   | `DAGSTER_MCP_FORWARD_HEADER` | `forward_header` |
   | `DAGSTER_MCP_GRAPHQL_URL` | `graphql_url` (URL absolue du point d'entrée GraphQL) |
   | `DAGSTER_MCP_TIMEOUT` | `timeout` |
   | `DAGSTER_MCP_READ_ONLY` | `read_only` (`0`/`1`, `true`/`false`) |
   | `DAGSTER_MCP_PORT` | port HTTP du serveur conteneur (défaut `8080`) |

L'API est contactée par défaut sur `<base_url>/graphql` (layout standard d'une
instance Dagster, l'UI est sur `/`). Si votre instance expose le point
d'entrée ailleurs, définissez `graphql_url` (YAML) ou `DAGSTER_MCP_GRAPHQL_URL`
(une URL absolue, ex. `https://dagster.example.org/api`).

> **Authentification derrière oauth2-proxy** : si l'instance passe par
> oauth2-proxy + un IdP (Keycloak, …), il faut un token que le proxy accepte.
> Si le client OIDC côté proxy est *confidential*, le password grant headless
> échoue sans son `client_secret` — dans ce cas un token provisionné (Vault,
> variable CI, ou un client OAuth dédié public) est la voie la plus simple.
> Vérifiez la nature du client OIDC de votre proxy avant de brancher un
> token.

### Relay du jeton utilisateur (`forward_header`) — mode HTTP partagé

En mode conteneur `streamable-HTTP stateless`, le serveur est **partagé** par
tous les utilisateurs via un seul process. Un `api_token` statique figé au
démarrage donnerait à **tout** le monde les mêmes droits Dagster, même pour
celui que l'oauth2-proxy a déjà filtré en amont. Le serveur MCP ne doit jamais
élargir les droits d'un utilisateur.

Solution : le serveur relaie, **tel quel**, le jeton de la requête MCP
entrante au lieu d'utiliser un token figé.

```yaml
# Mode relay (à la place de api_token)
base_url: "https://dagster.example.org"
forward_header: "Authorization"   # nom du header à relayer vers Dagster
# api_token doit rester vide / absent
```

Comportement :

- Chaque requête MCP entrante doit porter le header configuré (ex.
  `Authorization: Bearer <jeton de l'utilisateur>`).
- Sa valeur est relayée **telle quelle** comme `Authorization` sur l'appel
  GraphQL sortant (vers le point d'entrée configuré, par défaut
  `<base_url>/graphql`).
- Si le header est **absent ou vide** : l'appel échoue explicitement côté MCP
  (message d'erreur clair). **Aucun repli** sur `api_token`.
- Si `api_token` ET `forward_header` sont définis : **erreur de démarrage**
  (mutuellement exclusifs) — ne configurer qu'un des deux.
- Le mode `stdio` (usage local, un seul utilisateur) garde `api_token` tel
  quel ; `forward_header` est conçu pour le mode HTTP partagé.

Exemple consommateur côté client MCP HTTP (pattern générique, sans nom de
produit) :

```yaml
# Config du client MCP HTTP qui consomme le serveur en mode partagé
mcpServers:
  dagster:
    url: "http://localhost:8080/mcp"
    headers:
      # Le client (ou le reverse proxy en amont) doit poser le jeton
      # de l'utilisateur courant dans le header nommé par forward_header.
      Authorization: "Bearer {{ current_user_token }}"
```

> En pratique, c'est l'infrastructure en amont (oauth2-proxy, load balancer,
> client MCP) qui injecte le jeton de l'utilisateur courant dans ce header —
> pas le LLM. Le serveur n'y touche pas : il le relaye tel quel.

## Outils (V2 — API Dagster 1.x)

Lecture :

| Outil | Description |
|---|---|
| `dagster_overview` | locations + repositories, 10 derniers runs en erreur, backfills récents — point de départ |
| `dagster_workspace_status` | état détaillé des code locations (chargement, source de définitions, métadonnées) |
| `dagster_list_assets` | assets du repository : clé, groupe, jobs, stockage, partitions |
| `dagster_list_assets_paginated` | assets paginés (tous repos), filtrables par préfixe de clé |
| `dagster_asset_status` | état complet d'un asset : partitions, fraîcheur, ressources, lineage amont/aval, dernières matérialisations |
| `dagster_list_runs` | runs filtrables par job et/ou statut(s) |
| `dagster_run_detail` | détail d'un run : tags, config, stats par step, 60 premiers événements (logs, échecs, métadonnées de matérialisation) |
| `dagster_automations` | schedules + sensors : actif/désactivé, cron, prochain tick simulé, 5 derniers ticks |
| `dagster_backfills` | backfills en cours/terminés et leur progression par partition |
| `dagster_jobs` | jobs/pipelines du repository + partition sets |

Latitude agent :

| Outil | Description |
|---|---|
| `dagster_examples` | requêtes GraphQL modèles documentées (erreurs récentes, partitions du mois, queue, automatisations, locations, lineage, ressources↔assets, métadonnées de run) — à adapter puis lancer |
| `dagster_raw_query` | requête GraphQL arbitraire **lecture seule** en mode `read_only` (les mutations y sont refusées) ; accès au schéma complet en read-write |

Écriture (read-write uniquement) :

| Outil | Description |
|---|---|
| `dagster_materialize` | matérialise des assets (backfill) — verrou serveur `read_only: false` + confirmation explicite `read_only=false` |

Si l'instance expose plusieurs repositories, les outils qui ciblent un
repository acceptent `location`/`repository` (les valeurs sont visibles via
`dagster_overview`) ; sinon le premier repository accessible est utilisé.

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

**Réseau / header `Host`.** Le port du SDK `mcp` 1.x vers FastMCP 3.x a
supprimé à la racine l'ancienne protection anti-DNS-rebinding (contrôle du
header `Host`) qui provoquait des **421 Misdirected Request** derrière un
réseau de conteneurs (le client envoie `Host: <conteneur>:8000`, pas un
loopback). Le SDK 3.x n'applique aucun contrôle de `Host` par défaut — plus
aucune configuration nécessaire ; un test de bout en bout le garantit
(`tests/test_http_host.py`, `Host: mcp-dagster-ro:8000`). L'authentification
réelle reste le **relay du jeton utilisateur** (voir ci-dessus).

Le conteneur expose le serveur en **streamable-HTTP stateless** sur le port
`DAGSTER_MCP_PORT` (défaut `8080`) :

```bash
python -m mcp_dagster
```

Construire et lancer :

```bash
podman build -t mcp-dagster:local -f Containerfile .
podman run --rm -p 8080:8080 \
  -e DAGSTER_MCP_BASE_URL=https://dagster.example.org \
  -e DAGSTER_MCP_API_TOKEN=*** \
  -e DAGSTER_MCP_READ_ONLY=true \
  mcp-dagster:local
# endpoint MCP : http://localhost:8080/mcp
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

Tous les tests sont hors-réseau (client httpx mocké). Les 19 requêtes
embarquées (outils + exemples) sont validées dans la suite contre le vrai
schéma Dagster 1.12.1 (fixture `tests/schema_1_12_1.graphql`, généré via
`create_schema()` de `dagster-graphql`) : tout dérive de schéma échoue les
tests.

## Limites connues / suite

- Les requêtes sont calées sur le schéma Dagster **1.12.1** ; sur des
  versions très différentes du webserver, un champ peut avoir bougé —
  `dagster_raw_query` (et les erreurs GraphQL remontées) restent le plan B.
- `dagster_materialize` (backfill d'assets) est le seul outil d'écriture ;
  les autres écritures (terminer un run, recharger un repository, …)
  passent par `dagster_raw_query` en read-write, à l'initiative de
  l'opérateur.
- Pas encore de streaming des logs de run en continu (les 60 premiers
  événements sont retournés par `dagster_run_detail`).

## Licence

GPL-3.0-or-later — voir [LICENSE](LICENSE).