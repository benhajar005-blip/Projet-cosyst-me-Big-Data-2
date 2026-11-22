---
tags: [pedali, plan, jalon1]
date: 2026-11-22
---
# Plan — Jalon 1 (Parties 1–3)

> Suivi d'exécution : [[01 - Journal]] · Retour : [[Pedali - Accueil]]

## Objectif

Rendre le **Jalon 1** : Parties 1, 2 et 3 du projet, avec dépôt Git, scripts PySpark, `docker-compose.yml`, README et rapport.

## Arborescence cible

```
pedali/
  docker-compose.yml     # Kafka KRaft (topic trips, 3 partitions), Spark master + 2 workers,
                         # MinIO + one-shot minio-init, one-shot kafka-init, Jupyter PySpark
  producer.py            # fourni
  stations.json          # fourni (JSONL, 400 stations)
  src/part1_cluster.py
  src/part2_rdd.py
  src/part3_sql.py
  README.md
  REPORT.md              # réponses théoriques P1–P3
  docs/                  # ce vault Obsidian
  data/                  # sorties locales (ignoré par git)
```

## Phase 0 — Docker Desktop

- [x] `winget install -e --id Docker.DockerDesktop` (4.100.0)
- [x] `wsl --update` (déjà à jour)
- [x] Vérifier `docker version` et `docker compose version` — CLI OK (29.9.3 / v5.7.1), moteur OK

## Phase 1 — Squelette

- [x] Copier `producer.py` + `stations.json` depuis le ZIP (sans `.DS_Store`)
- [x] `git init` + `.gitignore`
- [x] Créer le vault Obsidian et le journal

## Phase 2 — Code + validation locale

- [x] `python producer.py --mode file --n 50000` → `data/trips_history.jsonl`
- [x] Partie 2 : RDD (`textFile`, `filter ENDED`, CA/ville, durée moyenne ville×type en un passage, top 5 départs, `toDebugString`)
- [x] Partie 3 : DataFrames (`StructType` explicite), SQL, jointure stations, UDF `trip_band` vs `when`, Parquet partitionné `city`, `explain(True)`
- [x] Exécution `local[*]` avec `assert`s : nb ENDED > 0, CA DataFrame == CA RDD, top 5 non vide, jointure sans perte
- [x] S3A MinIO : `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262`, `path.style.access=true`

## Phase 3 — Cluster + preuves

- [x] `docker compose up -d` (master + 2 workers + kafka + minio)
- [x] Bucket `lake` + préfixes, topic `trips` (3 partitions)
- [ ] Partie 1 : capture **Master UI :8080 avec 2 workers ALIVE** (mesure OK, capture à faire)
- [x] `mc cp` historique vers `lake/bronze/history/`
- [ ] Partie 2 : capture **DAG + stages** (exécution OK 13,2 s, capture à faire)
- [x] Partie 3 : `explain(True)` obtenu, `mc ls --recursive local/lake/` vérifié, Parquet Silver écrit
- [x] `REPORT.md` : Driver/Executors/Cluster Manager/SparkContext, 2 limites MapReduce, lineage + shuffle, Catalyst, UDF vs `when`, RDD vs DataFrame

## Hors périmètre (Jalon 2)

Delta Lake, Structured Streaming, GraphFrames/GraphX, fenêtrages, exactly-once. → 20/12/2026.

## Critères de fin

- `docker compose ps` : master + 2 workers + kafka + minio `Up`.
- Les 3 scripts tournent sans exception, `assert`s OK.
- Checklist captures §4.1 du PDF (P1–P3) cochée dans `README.md`.
