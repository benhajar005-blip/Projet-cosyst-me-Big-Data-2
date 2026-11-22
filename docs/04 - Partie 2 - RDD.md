---
tags: [pedali, partie2, rdd]
date: 2026-11-22
---
# Partie 2 — SparkCore (RDD)

Retour : [[Pedali - Accueil]] · [[03 - Partie 1 - Cluster]] · [[04 - Partie 2 - RDD]]

## Commande cluster

```powershell
docker compose exec -T -e TRIPS_INPUT=/opt/pedali/data/trips_history.jsonl spark-master `
  spark-submit --master spark://spark-master:7077 /opt/pedali/src/part2_rdd.py
```

## Résultats (15 918 ENDED)

| Ville | CA (MAD) |
|---|---|
| Casablanca | 114 023,63 |
| Marrakech | 59 185,71 |
| Rabat | 57 259,36 |
| Tanger | 46 185,24 |
| **Total** | **276 653,94** |

Top 5 départs : `S-MAR-002` 168 · `S-MAR-004` 160 · `S-MAR-005` 152 · `S-CAS-001` 147 · `S-MAR-001` 137
→ boucle touristique de Marrakech retrouvée.

Durées moyennes (s) : BIKE 1 400–1 674 · EBIKE 1 007–1 273 · SCOOTER 1 073–1 372.

**Temps** : 13,2 s sur le cluster (2 workers), 54,7 s en local `local[*]`.

## Lineage

```
PythonRDD[2] at RDD [Memory Serialized 1x Replicated]
 | CachedPartitions: 2
 | /opt/pedali/data/trips_history.jsonl MapPartitionsRDD[1] at textFile
 | /opt/pedali/data/trips_history.jsonl HadoopRDD[0] at textFile
```

Transformations lazy : `textFile`, `map`, `filter`, `reduceByKey`, `sortBy`, `takeOrdered`.
Actions : `count`, `collect`, `sum`, `toDebugString`.

`reduceByKey` = frontière de shuffle (CA/ville, durée ville×type) → stages visibles dans la Spark UI.

## Capture à faire

Spark UI (port 4040 pendant l'exécution, lien depuis l'app dans Master UI) : DAG + stages.
