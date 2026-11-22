---
tags: [pedali, partie3, sql, dataframe]
date: 2026-11-22
---
# Partie 3 — Spark SQL et DataFrames

Retour : [[Pedali - Accueil]] · [[04 - Partie 2 - RDD]] · [[06 - Captures à faire]]

## Commande cluster (écriture MinIO)

```powershell
docker compose exec -T -e TRIPS_INPUT=/opt/pedali/data/trips_history.jsonl `
  -e STATIONS=/opt/pedali/stations.json -e SILVER_OUT=s3a://lake/silver/trips_history `
  spark-master spark-submit --master spark://spark-master:7077 `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/pedali/src/part3_sql.py
```

## Résultats

- CA par ville identique en DataFrame et en SQL (assertion) : 114 023,63 / 59 185,71 / 57 259,36 / 46 185,24.
- Jointure `ENDED ⋈ stations` sur `start_station` : 15 918 lignes, **aucune perte**.
- `trip_band` : court 4 805 · moyen 7 312 · long 3 801 ; UDF == `when/otherwise` (assertion).
- **UDF 8,04 s vs `when` 1,08 s** en local → les expressions natives passent par Catalyst/Tungsten.
- Plan physique : **BroadcastHashJoin** (stations = 400 lignes, BuildRight), pas de shuffle du côté trajets.
- Parquet Silver : 4 partitions `city=Casablanca|Marrakech|Rabat|Tanger` + `_SUCCESS` dans
  `s3a://lake/silver/trips_history/` (72 Ko max par fichier).
- Temps cluster : 59,3 s (dont ~60 s de résolution Ivy au premier lancement).

## Vérification MinIO

```powershell
docker compose run --rm --entrypoint sh minio-init -c `
  "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls --recursive local/lake"
```

Attendu : `bronze/history/trips_history.jsonl` (15 Mo) + `silver/trips_history/city=.../*.parquet`.

## Captures à faire

- `explain(True)` (sortie console, plan logique/optimisé/physique).
- `mc ls --recursive local/lake` (Bronze + Silver).
- Parquet partitionné dans la console MinIO (`http://localhost:9001`).
