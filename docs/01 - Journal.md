---
tags: [pedali, journal]
date: 2026-11-22
---
# Journal d'exécution

Retour : [[Pedali - Accueil]] · Plan : [[00 - Plan Jalon 1]]

Chaque étape réalisée est ajoutée ici, dans l'ordre chronologique.

## 2026-11-22

### Étape 1 — Vault Obsidian + plan

- Création du dossier projet `pedali/` : `docs/`, `src/`, `data/`.
- Vault Obsidian initialisé dans `docs/` (`.obsidian/app.json`).
- Notes créées : Accueil, Plan Jalon 1, Environnement, Journal.
- **Décision** : le vault vit dans `docs/` du dépôt Git → notes versionnées avec le code, pas de duplication du plan.

### Étape 2 — Docker Desktop

- `winget install -e --id Docker.DockerDesktop` → **Docker Desktop 4.100.0** installé.
- `wsl --update` : déjà à jour.
- CLI : `docker 29.9.3`, `docker compose v5.7.1`.
- **Blocage** : le moteur n'écoute pas encore (`npipe:////./pipe/docker_engine` absent). À relancer via Docker Desktop puis re-tester `docker info`.

### Étape 3 — Squelette du dépôt

- `producer.py` et `stations.json` extraits du ZIP (sans `.DS_Store` ni `__MACOSX`).
- `git init` dans `pedali/`, `.gitignore` (`data/`, `*.jsonl`, `.venv/`, workspace Obsidian).
- Dossiers : `src/`, `data/`, `docs/`.

### Étape 4 — Génération de l'historique

```
python producer.py --mode file --n 50000 --output data/trips_history.jsonl
```

- 50 000 événements, 2,1 Mo, du 2026-11-09 au 2026-11-15.
- Statuts : RESERVED 16 847 · UNLOCKED 15 960 · ENDED 15 918 · CANCELLED 891 · FAULT_REPORTED 384.
- 16 531 trajets, 2 541 en retard, 961 doublons.
- **Incident** : premier lancement interrompu (coupure réseau) → 6 885 lignes ; relancé avec `-u`, 50 000 OK.

### Étape 5 — Scripts Parties 1–3

- `src/part1_cluster.py` : version, master, appName, `defaultParallelism`, nb executors.
- `src/part2_rdd.py` : `textFile` + `json.loads`, `filter ENDED`, CA/ville (`reduceByKey`), durée ville×type (`reduceByKey` somme+compte), top 5 (`takeOrdered`), `toDebugString`.
- `src/part3_sql.py` : `StructType` explicites, CA DataFrame vs SQL, jointure stations, UDF `trip_band` vs `when`, Parquet `partitionBy("city")`, `explain(True)`, config S3A MinIO.

### Étape 6 — Validation locale `local[*]`

- Partie 1 : Spark 3.5.9, `defaultParallelism = 4`, executors = 1 (local). OK.
- Partie 2 : 15 918 ENDED, CA total **276 653,94 MAD**, top 5 dominé par `S-MAR-002/004/005` (boucle touristique détectée), 54,7 s.
- Partie 3 : DataFrame == SQL, jointure 15 918/15 918 lignes, UDF == `when`, **BroadcastHashJoin** (stations = 400 lignes), UDF 8,04 s vs `when` 1,08 s, Parquet écrit (4 partitions `city=`), 35,7 s.
- Assertions passées, aucun warning bloquant.

### Étape 7 — Documentation

- `README.md` : lancement Docker, commandes cluster, checklist captures §4.1.
- `REPORT.md` : réponses théoriques P1–P3 + résultats mesurés.
- `docker-compose.yml` : Kafka KRaft + topic 3 partitions, master + 2 workers, MinIO + init bucket, Jupyter.

### Prochaine étape

Démarrer le moteur Docker, `docker compose up -d`, vérifier Master UI (2 workers) et MinIO, puis exécuter P1–P3 sur le cluster et capturer les preuves.

### Étape 8 — Cluster opérationnel + Parties 1–3 exécutées

**Mise en route (3 bugs corrigés)**
- `bitnami/*` retiré de Docker Hub → bascule sur `bitnamilegacy/*` (spark 3.5, kafka 3.7, minio 2025.4.22, minio-client).
- Kafka refusait de démarrer : `controller.quorum.voters=1` alors que `node.id=0` → `0@kafka:9093`. Topic `trips` créé avec **3 partitions**.
- MinIO : volume nommé non inscriptible par l'uid 1001 → `user: root`. Bucket `lake` + `bronze/silver/gold/checkpoints` créés.
- Spark : uid 1001 sans entrée passwd → Ivy `?/.ivy2/local` et UGI Hadoop `LoginException` → `user: root` sur master + workers.

**Exécution cluster**
- Partie 1 : `FINISHED`, Spark **3.5.6**, master `spark://spark-master:7077`, 2 workers ALIVE (4 cœurs), `defaultParallelism=2`.
- Historique copié dans MinIO : `lake/bronze/history/trips_history.jsonl` (15 Mo).
- Partie 2 : mêmes résultats qu'en local (CA total 276 653,94 MAD, top 5 boucle Marrakech), **13,2 s** (vs 54,7 s local).
- Partie 3 : DataFrame == SQL, jointure 15 918/15 918, **BroadcastHashJoin**, UDF 8,04 s vs `when` 1,08 s, Parquet 4 partitions `city=` écrit dans `s3a://lake/silver/trips_history/`, **59,3 s**.
- Pression mémoire (8 Go host) : `docker compose stop jupyter kafka` + `--driver-memory 512m --executor-memory 512m` pour la Partie 3 ; services redémarrés ensuite.
- Vérification : `mc ls --recursive local/lake` montre Bronze (15 Mo) + Silver (`_SUCCESS` + 16 fichiers Parquet).

**Documentation**
- Notes `03` à `06` créées (Partie 1, Partie 2, Partie 3, Captures).
- `README.md` : commandes corrigées, résultats, notes d'exploitation.
- `REPORT.md` : temps cluster ajoutés.

**Reste à faire** : captures d'écran (Master UI, MinIO, DAG, `explain`, `mc ls`), puis commit Git.

<!-- prochaine entrée ici -->
