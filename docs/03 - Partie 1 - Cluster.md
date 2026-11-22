---
tags: [pedali, partie1, cluster]
date: 2026-11-22
---
# Partie 1 — Cluster et positionnement

Retour : [[Pedali - Accueil]] · Plan : [[00 - Plan Jalon 1]] · Journal : [[01 - Journal]]

## Déploiement

`docker compose up -d` → 8 conteneurs (2 one-shot) :

| Service | Rôle | Port |
|---|---|---|
| `pedali-spark-master` | Master standalone + driver | 7077, UI 8080 |
| `pedali-spark-worker-1/2` | Executors (2 cœurs, 1 Go chacun) | UI 8081 / 8082 |
| `pedali-kafka` | Broker KRaft, topic `trips` (3 partitions) | 29092 (hôte) |
| `pedali-minio` | Stockage S3, bucket `lake` | 9000, console 9001 |
| `pedali-jupyter` | PySpark notebook | 8888 (`token=pedali`) |
| `kafka-init` / `minio-init` | One-shot : topic + bucket/couches | — |

## Résultat mesuré

```
Spark version      : 3.5.6
master             : spark://spark-master:7077
appName            : Pedali Part1 Cluster
defaultParallelism : 2
```

Master UI `http://localhost:8080` : **2 workers ALIVE, 4 cœurs, 2 Go**.

## Corrections nécessaires (journal)

- Images `bitnami/*` retirées de Docker Hub → `bitnamilegacy/*`.
- Kafka : `CONTROLLER_QUORUM_VOTERS` doit contenir le `NODE_ID` (`0@kafka:9093`).
- MinIO : volume nommé non inscriptible par l'uid 1001 → `user: root`.
- Spark : uid 1001 sans entrée passwd → Ivy `?/.ivy2` + UGI Hadoop `LoginException`
  → `user: root` sur master et workers.

## Capture à faire

`http://localhost:8080` avec les deux workers `ALIVE` → [[06 - Captures à faire]].
