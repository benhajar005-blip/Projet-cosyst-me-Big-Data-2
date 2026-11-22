---
tags: [pedali, captures, jalon1]
date: 2026-11-22
---
# Captures à faire (Jalon 1)

Retour : [[Pedali - Accueil]] · [[00 - Plan Jalon 1]]

État : environnement **opérationnel** et résultats produits le 22/11/2026.
Il ne reste que les captures d'écran (à faire par l'utilisateur).

| # | Preuve | Où / commande | État |
|---|---|---|---|
| 1 | Master UI, 2 workers ALIVE | http://localhost:8080 | à capturer |
| 2 | Console MinIO, bucket `lake` | http://localhost:9001 (`minioadmin`/`minioadmin`) | à capturer |
| 3 | DAG + stages Partie 2 | Spark UI (4040) pendant `part2_rdd.py` | à capturer |
| 4 | DAG + stages Partie 3 | Spark UI (4040) pendant `part3_sql.py` | à capturer |
| 5 | `explain(True)` | sortie console de `part3_sql.py` | à capturer |
| 6 | `mc ls --recursive local/lake` | voir [[05 - Partie 3 - SQL]] | à capturer |
| 7 | Parquet partitionné `city` | MinIO : `lake/silver/trips_history/` | à capturer |
| 8 | Kafka topic 3 partitions | `docker compose logs kafka-init` | à capturer |

## Rappels

- Jupyter : http://localhost:8888 (`token=pedali`).
- Si la machine manque de RAM : `docker compose stop jupyter kafka` avant la Partie 3.
- Ne pas oublier le commit Git et le README dans le rendu.
