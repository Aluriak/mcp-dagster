"""Requêtes GraphQL pour l'API Dagster 1.x (workspace → locations → repos).

Chaque constante a été validée contre le schéma réel du webserver Dagster
1.12.1 (``dagster_graphql.schema.create_schema()`` + ``graphql-core``
``validate()``) — voir ``tests/test_queries_schema.py``.

Les requêtes qui ciblent un repository reçoivent les variables ``loc`` et
``repo`` (RepositorySelector) construites par la découverte automatique du
workspace (``DagsterClient.repository_selector``). Les requêtes racine
(runs, backfills, workspace) n'en ont pas besoin.

Les ``EXEMPLES_*`` sont des modèles documentés destinés à l'agent : il peut
les adapter, combiner et les lancer via ``dagster_raw_query``.
"""

from __future__ import annotations

# ── Workspace & locations (racine Query, pas de selector) ───────────────────

OVERVIEW = """
query {
  workspaceOrError {
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        locationOrLoadError {
          ... on RepositoryLocation {
            name
            repositories { name }
          }
          ... on PythonError {
            message
          }
        }
      }
    }
    ... on PythonError {
      message
    }
  }
  runsOrError(filter: {statuses: [FAILURE, CANCELING, CANCELED]}, limit: 10) {
    ... on Runs {
      # PAS de « count » : count(*) sur la table runs — sous charge (56k
      # runs en erreur sur vision) il timeout en Postgres et fait tomber
      # l'overview entier en 500. Les 10 derniers runs en erreur
      # suffisent pour la vue d'ensemble.
      results {
        id
        jobName
        status
        creationTime
        startTime
        endTime
        tags { key value }
        stepStats { stepKey status }
      }
    }
    ... on InvalidPipelineRunsFilterError {
      message
    }
    ... on PythonError {
      message
    }
  }
  partitionBackfillsOrError(limit: 10) {
    ... on PartitionBackfills {
      results {
        id
        title
        status
        numPartitions
        assetSelection { path }
        partitionStatusCounts { runStatus count }
      }
    }
    ... on PythonError {
      message
    }
  }
}
"""

WORKSPACE_STATUS = """
query {
  workspaceOrError {
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        updatedTimestamp
        definitionsSource
        locationOrLoadError {
          ... on RepositoryLocation {
            name
            repositories {
              name
              displayMetadata { key value }
            }
          }
          ... on PythonError {
            message
            className
          }
        }
      }
    }
    ... on PythonError {
      message
    }
  }
}
"""

# ── Requêtes à selector (variables $loc / $repo) ────────────────────────────

JOBS = """
query ($loc: String!, $repo: String!) {
  repositoryOrError(
    repositorySelector: {
      repositoryLocationName: $loc
      repositoryName: $repo
    }
  ) {
    ... on Repository {
      name
      pipelines {
        name
        description
        isJob
      }
      partitionSets {
        name
        pipelineName
      }
      displayMetadata { key value }
    }
    ... on RepositoryNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""

# Fragments d'état de partitions (les 3 branches de AssetPartitionStatuses ;
# materializedPartitions & co sont des [String!]! → pas de sous-sélection)
_PARTITION_FRAGMENTS = """
fragment defaultParts on DefaultPartitionStatuses {
  materializedPartitions
  failedPartitions
  unmaterializedPartitions
  materializingPartitions
}
fragment timeParts on TimePartitionStatuses {
  ranges { startKey endKey status }
}
fragment multiParts on MultiPartitionStatuses {
  primaryDimensionName
  ranges {
    primaryDimStartKey
    primaryDimEndKey
    secondaryDim {
      ... on TimePartitionStatuses {
        ranges { startKey endKey status }
      }
      ... on DefaultPartitionStatuses {
        materializedPartitions
        failedPartitions
        unmaterializedPartitions
        materializingPartitions
      }
    }
  }
}
"""

# Fragments d'état de partitions réutilisables dans les exemples aussi
PARTITION_FRAGMENTS = _PARTITION_FRAGMENTS

ASSETS = _PARTITION_FRAGMENTS + """
query ($loc: String!, $repo: String!) {
  repositoryOrError(
    repositorySelector: {
      repositoryLocationName: $loc
      repositoryName: $repo
    }
  ) {
    ... on Repository {
      name
      assetNodes {
        assetKey { path }
        description(characterLimit: 200)
        groupName
        computeKind
        kinds
        isPartitioned
        jobNames
        storageAddress { storageKind tableName }
        metadataEntries { label description }
        partitionStats {
          numMaterialized
          numPartitions
          numFailed
          numMaterializing
        }
        assetPartitionStatuses {
          ... on DefaultPartitionStatuses {
            ...defaultParts
          }
          ... on TimePartitionStatuses {
            ...timeParts
          }
          ... on MultiPartitionStatuses {
            ...multiParts
          }
        }
      }
    }
    ... on RepositoryNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""

ASSET_RECORDS = """
query ($prefix: [String!], $cursor: String, $limit: Int) {
  assetRecordsOrError(prefix: $prefix, cursor: $cursor, limit: $limit) {
    ... on AssetRecordConnection {
      assets { id key { path } }
      cursor
    }
    ... on PythonError {
      message
    }
  }
}
"""

ASSET_STATUS = _PARTITION_FRAGMENTS + """
query ($key: AssetKeyInput!, $limit: Int) {
  assetNodeOrError(assetKey: $key) {
    ... on AssetNode {
      assetKey { path }
      description(characterLimit: 400)
      groupName
      owners {
        ... on UserAssetOwner { email }
        ... on TeamAssetOwner { team }
      }
      computeKind
      kinds
      isPartitioned
      isMaterializable
      isVirtual
      jobNames
      opNames
      storageAddress { storageKind tableName }
      requiredResources { resourceKey }
      metadataEntries { label description }
      freshnessStatusInfo {
        freshnessStatus
        freshnessStatusMetadata { lastMaterializedTimestamp }
      }
      assetMaterializations(limit: $limit) {
        runId
        stepKey
        partition
        timestamp
        message
        metadataEntries { label description }
      }
      assetPartitionStatuses {
        ... on DefaultPartitionStatuses {
          ...defaultParts
        }
        ... on TimePartitionStatuses {
          ...timeParts
        }
        ... on MultiPartitionStatuses {
          ...multiParts
        }
      }
      partitionStats {
        numMaterialized
        numPartitions
        numFailed
        numMaterializing
      }
      dependencies {
        asset { assetKey { path } }
      }
      dependedBy {
        asset { assetKey { path } }
      }
    }
    ... on AssetNotFoundError {
      message
    }
  }
}
"""

RUNS = """
# Compat Dagster 1.12 → 1.13 :
#  - PAS de ``count`` ici : c'est un count(*) sur la table runs du metadata
#    (ex. 56k runs en erreur sur vision) — sous charge il dépasse le
#    statement_timeout de Postgres et fait tomber l'APPEL ENTIER en 500,
#    alors que les runs eux-mêmes s'obtiennent sans problème.
#  - PAS de ``assetSelection(limit: ...)`` : l'argument ``limit`` du champ
#    ``Run.assetSelection`` n'existe pas sur Dagster 1.13.x → 400
#    "Unknown argument 'limit' on field 'Run.assetSelection'" (le champ est
#    sélectionnable sans arg, mais son contenu n'est pas utile dans une
#    vue liste ; le détail par run passe par RUN_DETAIL).
query (
  $limit: Int
  $pipelineName: String
  $statuses: [RunStatus!]
  $tags: [ExecutionTag!]
) {
  runsOrError(
    filter: {
      pipelineName: $pipelineName
      statuses: $statuses
      tags: $tags
    }
    limit: $limit
  ) {
    ... on Runs {
      results {
        id
        jobName
        pipelineName
        status
        mode
        creationTime
        startTime
        endTime
        updateTime
        rootRunId
        tags { key value }
        stepStats {
          stepKey
          status
          startTime
          endTime
        }
      }
    }
    ... on InvalidPipelineRunsFilterError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""

RUN_DETAIL = """
query ($runId: ID!) {
  runOrError(runId: $runId) {
    ... on Run {
      id
      runId
      jobName
      pipelineName
      status
      mode
      rootRunId
      parentRunId
      creationTime
      startTime
      endTime
      updateTime
      tags { key value }
      runConfigYaml
      stepStats {
        stepKey
        status
        startTime
        endTime
      }
      stats {
        ... on RunStatsSnapshot {
          stepsSucceeded
          stepsFailed
          materializations
          expectations
          enqueuedTime
          launchTime
          startTime
          endTime
        }
        ... on PythonError {
          message
        }
      }
      eventConnection(limit: 60) {
        events {
          ... on MessageEvent { eventType }
          ... on LogMessageEvent {
            message
            timestamp
            stepKey
            level
          }
          ... on ExecutionStepSuccessEvent {
            message
            timestamp
            stepKey
          }
          ... on ExecutionStepFailureEvent {
            message
            timestamp
            stepKey
            error { message className }
          }
          ... on MaterializationEvent {
            assetKey { path }
            partition
            timestamp
            stepKey
            metadataEntries { label description }
          }
          ... on FailedToMaterializeEvent {
            message
            timestamp
          }
        }
        hasMore
      }
    }
    ... on RunNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""

AUTOMATIONS = """
query ($loc: String!, $repo: String!) {
  schedulesOrError(
    repositorySelector: {
      repositoryLocationName: $loc
      repositoryName: $repo
    }
  ) {
    ... on Schedules {
      results {
        name
        description
        cronSchedule
        executionTimezone
        defaultStatus
        scheduleState {
          status
          nextTick { timestamp evaluationResult { skipReason runRequests { runKey } } }
          runningCount
          ticks(limit: 5) {
            timestamp
            status
            skipReason
            runKeys
          }
        }
      }
    }
    ... on RepositoryNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
  sensorsOrError(
    repositorySelector: {
      repositoryLocationName: $loc
      repositoryName: $repo
    }
  ) {
    ... on Sensors {
      results {
        name
        description
        sensorType
        defaultStatus
        nextTick { timestamp evaluationResult { skipReason runRequests { runKey } } }
        sensorState {
          status
          runningCount
          ticks(limit: 5) {
            timestamp
            status
            skipReason
            runKeys
          }
        }
      }
    }
    ... on RepositoryNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""

BACKFILLS = """
query ($limit: Int, $status: BulkActionStatus) {
  partitionBackfillsOrError(limit: $limit, status: $status) {
    ... on PartitionBackfills {
      results {
        id
        title
        status
        numPartitions
        numCancelable
        fromFailure
        assetSelection { path }
        partitionSetName
        partitionStatusCounts { runStatus count }
        runs(limit: 5) { id status jobName creationTime }
      }
    }
    ... on PythonError {
      message
    }
  }
}
"""

# ── Mutation de matérialisation (uniquement en mode read-write) ─────────────
# launchPartitionBackfill accepte TROIS modes de sélection (mutuellement
# exclusifs côté resolver) :
#   1. selector (PartitionSetSelector) + partitionNames / allPartitions  → backfill de job
#   2. assetSelection (+ partitionNames)                                 → backfill d'assets
#   3. partitionsByAssets                                                 → partitions par asset
# Le résultat est une union : on sélectionne le succès + les erreurs courantes.
MATERIALIZE = """
mutation (
  $partitionSet: PartitionSetSelector
  $partitionNames: [String!]
  $assets: [AssetKeyInput!]
  $partitionsByAssets: [PartitionsByAssetSelector!]
  $allPartitions: Boolean
  $title: String
  $tags: [ExecutionTag!]
) {
  launchPartitionBackfill(
    backfillParams: {
      selector: $partitionSet
      partitionNames: $partitionNames
      assetSelection: $assets
      partitionsByAssets: $partitionsByAssets
      allPartitions: $allPartitions
      title: $title
      tags: $tags
    }
  ) {
    ... on LaunchBackfillSuccess {
      backfillId
      launchedRunIds
    }
    ... on PartitionSetNotFoundError { message }
    ... on PartitionKeysNotFoundError { message partitionKeys }
    ... on InvalidSubsetError { message }
    ... on RunConflict { message }
    ... on UnauthorizedError { message }
    ... on PythonError {
      message
      className
    }
  }
}
"""


def selector_variables(location: str, repository: str) -> dict[str, str]:
    """Variables standard $loc / $repo (RepositorySelector)."""
    return {"loc": location, "repo": repository}


def asset_key_variable(key: str) -> dict[str, dict[str, list[str]]]:
    """Normalise un asset key ("ch.dataset" ou "ch/dataset") en AssetKeyInput."""
    path = [p for p in key.strip().replace(".", "/").split("/") if p]
    return {"key": {"path": path}}


# ── Exemples documentés pour l'agent (adaptés via dagster_raw_query) ────────

EXEMPLES: dict[str, str] = {
    "dernieres_erreurs": """
# « Quelles sont les dernières erreurs rencontrées par Dagster ? »
# (variables : $limit, $statuses, $tags — cf. tool dagster_list_runs)
# Pas de « count » : count(*) sur la table runs → timeout Postgres sous
# charge (cf. requête RUNS).
query ($limit: Int, $statuses: [RunStatus!], $tags: [ExecutionTag!]) {
  runsOrError(filter: {statuses: $statuses, tags: $tags}, limit: $limit) {
    ... on Runs {
      results {
        id jobName status creationTime startTime endTime
        stepStats { stepKey status }
      }
    }
    ... on InvalidPipelineRunsFilterError { message }
    ... on PythonError { message }
  }
}
""",
    "partitions_du_mois": """
# « Les partitions de <asset> pour le mois dernier sont-elles toutes
# matérialisées ? » — filtrer les ranges par startKey/endKey côté client.
# (variable : $key — assetNodeOrError est en racine, pas besoin de selector)
query ($key: AssetKeyInput!) {
  assetNodeOrError(assetKey: $key) {
    ... on AssetNode {
      assetKey { path }
      assetMaterializations(limit: 200) {
        partition timestamp stepKey
      }
      assetPartitionStatuses {
        ... on TimePartitionStatuses {
          ranges { startKey endKey status }
        }
        ... on DefaultPartitionStatuses {
          failedPartitions
          unmaterializedPartitions
        }
        ... on MultiPartitionStatuses {
          primaryDimensionName
        }
      }
      partitionStats { numMaterialized numPartitions numFailed numMaterializing }
    }
    ... on AssetNotFoundError { message }
  }
}
""",
    "materialisations_en_cours": """
# « Quelles sont les matérialisations en cours ? Où en est la queue ? »
# Statuts in-progress compatibles Dagster 1.12/1.13 (SUSPENDED n'existe
# plus dans l'enum RunStatus de 1.13.x → ne pas l'utiliser). Pas de
# « count » (count(*) → timeout Postgres sous charge).
query {
  runsOrError(filter: {statuses: [STARTING, STARTED, QUEUED, CANCELING]}, limit: 50) {
    ... on Runs {
      results {
        id jobName status mode creationTime startTime
        rootRunId
        stepStats { stepKey status startTime }
        # sans argument limit : cet arg n'existe plus sur Dagster 1.13.x
        assetSelection { path }
      }
    }
    ... on InvalidPipelineRunsFilterError { message }
    ... on PythonError { message }
  }
  partitionBackfillsOrError(limit: 10) {
    ... on PartitionBackfills {
      results {
        id title status numPartitions
        assetSelection { path }
        partitionStatusCounts { runStatus count }
      }
    }
    ... on PythonError { message }
  }
}
""",
    "automatisations_prochaines": """
# « Quelles automatisations vont prochainement se déclencher ? Les désactivées ? »
# (variables : $loc, $repo — cf. tool dagster_automations)
query ($loc: String!, $repo: String!) {
  schedulesOrError(repositorySelector: {repositoryLocationName: $loc, repositoryName: $repo}) {
    ... on Schedules {
      results {
        name cronSchedule defaultStatus
        scheduleState {
          status
          nextTick { timestamp evaluationResult { skipReason runRequests { runKey } } }
        }
      }
    }
    ... on RepositoryNotFoundError { message }
    ... on PythonError { message }
  }
  sensorsOrError(repositorySelector: {repositoryLocationName: $loc, repositoryName: $repo}) {
    ... on Sensors {
      results {
        name sensorType defaultStatus
        nextTick { timestamp evaluationResult { skipReason runRequests { runKey } } }
        sensorState { status }
      }
    }
    ... on RepositoryNotFoundError { message }
    ... on PythonError { message }
  }
}
""",
    "etat_code_locations": """
# « Quel est l'état des différentes code locations ? »
query {
  workspaceOrError {
    ... on Workspace {
      locationEntries {
        name loadStatus updatedTimestamp definitionsSource
        locationOrLoadError {
          ... on RepositoryLocation { name repositories { name } }
          ... on PythonError { message className }
        }
      }
    }
    ... on PythonError { message }
  }
}
""",
    "lineage_asset": """
# « Comment est organisé le lineage de <asset> ? Où sont stockées ses données ? »
# (variable : $key — assetNodeOrError est en racine, pas besoin de selector)
query ($key: AssetKeyInput!) {
  assetNodeOrError(assetKey: $key) {
    ... on AssetNode {
      assetKey { path }
      storageAddress { storageKind tableName }
      dependencies { asset { assetKey { path } } }
      dependedBy { asset { assetKey { path } } }
      jobNames
    }
    ... on AssetNotFoundError { message }
  }
}
""",
    "metadata_run": """
# « Quelles sont les métadonnées liées à ce run ? » (variable : $runId)
query ($runId: ID!) {
  runOrError(runId: $runId) {
    ... on Run {
      id jobName status creationTime
      tags { key value }
      runConfigYaml
      eventConnection(limit: 60) {
        events {
          ... on MessageEvent { eventType }
          ... on LogMessageEvent { message timestamp stepKey level }
          ... on MaterializationEvent {
            assetKey { path }
            partition
            metadataEntries { label description }
          }
        }
      }
    }
    ... on RunNotFoundError { message }
    ... on PythonError { message }
  }
}
""",
    "ressource_assets": """
# « Quels assets travaillent avec la ressource <name> ? » (variables : $loc, $repo)
query ($loc: String!, $repo: String!) {
  repositoryOrError(repositorySelector: {repositoryLocationName: $loc, repositoryName: $repo}) {
    ... on Repository {
      allTopLevelResourceDetails {
        name
        resourceType
        assetKeysUsing { path }
      }
    }
    ... on RepositoryNotFoundError { message }
    ... on PythonError { message }
  }
}
""",
}


def examples_doc() -> str:
    """Liste des exemples documentés (nom → description de la 1ʳᵉ ligne)."""
    out = []
    for name, doc in EXEMPLES.items():
        first = next(
            (l.strip("# ").strip() for l in doc.splitlines() if l.strip().startswith("#")),
            name,
        )
        out.append(f"- {name} : {first}")
    return "\n".join(out)