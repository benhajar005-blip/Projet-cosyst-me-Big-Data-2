# Pedali — Plateforme Big Data (Jalon 1)

Projet EMSI 5A — Écosystème Big Data 2. Traitement batch de trajets Pedali.ma avec Spark 3.5, Kafka, MinIO.
**Jalon 1 = Parties 1, 2 et 3.**

## Contenu

```
docker-compose.yml     Kafka KRaft (topic trips, 3 partitions), Spark master + 2 workers,
                       MinIO + init du bucket lake, Jupyter PySpark
producer.py            générateur d'événements (fourni)
stations.json          référentiel 400 stations (JSONL, fourni)
src/part1_cluster.py   SparkSession sur le cluster + métriques
src/part2_rdd.py       SparkCore / RDD
src/part3_sql.py       Spark SQL / DataFrames + Parquet Silver
docs/                  vault Obsidian (plan, journal, notes)
REPORT.md              réponses théoriques et résultats
```

## Prérequis

- Docker Desktop (WSL2) avec `docker compose`
- 8 Go de RAM libres conseillés (images Spark + Jupyter)

## 1. Démarrer l'environnement

```powershell
docker compose up -d
docker compose ps
```

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Worker 1 / 2 | http://localhost:8081 / http://localhost:8082 |
| MinIO console | http://localhost:9001 (`minioadmin` / `minioadmin`) |
| Jupyter | http://localhost:8888 (`token=pedali`) |
| Kafka (depuis l'hôte) | `localhost:29092` |

`kafka-init` crée le topic `trips` (3 partitions), `minio-init` crée le bucket `lake`
et les préfixes `bronze/ silver/ gold/ checkpoints/`.

## 2. Générer l'historique (Partie 2)

```powershell
python producer.py --mode file --n 50000 --output data/trips_history.jsonl
```

Copie dans la couche Bronze :

```powershell
docker compose run --rm -v "${PWD}/data:/data" --entrypoint sh minio-init -c `
  "mc alias set local http://minio:9000 minioadmin minioadmin && mc cp /data/trips_history.jsonl local/lake/bronze/history/"
```

## 3. Lancer les parties sur le cluster

Partie 1 (capture Master UI : 2 workers ALIVE) :

```powershell
docker compose exec spark-master spark-submit --master spark://spark-master:7077 `
  /opt/pedali/src/part1_cluster.py
```

Partie 2 (capture DAG / stages dans la Spark UI) :

```powershell
docker compose exec -e PEDALI_PAUSE=120 spark-master `
  spark-submit --master spark://spark-master:7077 /opt/pedali/src/part2_rdd.py
```

Partie 3 (écriture Parquet dans MinIO + `explain(True)`) :

```powershell
docker compose exec -e PEDALI_PAUSE=120 `
  -e SILVER_OUT=s3a://lake/silver/trips_history `
  spark-master spark-submit --master spark://spark-master:7077 `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/pedali/src/part3_sql.py
```

Pendant la pause (`PEDALI_PAUSE`), ouvrir **http://localhost:4040** (UI du driver) :
onglet *Jobs* → clic sur un job → **DAG Visualization** ; onglet *Stages* pour le nombre
de stages (Partie 2 : 4 jobs / 5 stages, Partie 3 : 13 jobs / 23 stages).

## 4. Validation locale (sans Docker)

```powershell
$env:TRIPS_INPUT="$PWD\data\trips_history.jsonl"
$env:STATIONS="$PWD\stations.json"
$env:SPARK_MASTER="local[*]"
python src/part1_cluster.py
python src/part2_rdd.py
python src/part3_sql.py
```

Les scripts contiennent des `assert` : CA DataFrame == CA SQL, UDF == `when/otherwise`,
jointure sans perte, top 5 non vide.

## 5. Captures obligatoires (Jalon 1)

- [ ] Spark Master UI avec **2 workers ALIVE**
- [ ] Console MinIO : bucket `lake` + couches
- [ ] DAG + stages Spark UI (Parties 2 et 3)
- [ ] Sortie `explain(True)` (Partie 3)
- [ ] `docker compose run --rm --entrypoint sh minio-init -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls --recursive local/lake"`

## Résultats mesurés (cluster, 22/11/2026)

| Partie | Résultat | Temps |
|---|---|---|
| 1 | Spark 3.5.6, master `spark://spark-master:7077`, 2 workers ALIVE, 4 cœurs | — |
| 2 | 15 918 ENDED, CA total 276 653,94 MAD, top 5 dominé par la boucle de Marrakech | 13,2 s |
| 3 | DataFrame == SQL, UDF == `when`, BroadcastHashJoin, Parquet 4 partitions `city=` dans MinIO | 59,3 s |

Détail : `docs/03` à `docs/06`.

## Notes d'exploitation

- Les images Bitnami ne sont plus publiées sur `docker.io/bitnami` : le compose utilise
  `bitnamilegacy/*` (archive officielle).
- Les conteneurs Spark tournent en `user: root` : l'image Bitnami n'a pas d'entrée passwd pour
  l'uid 1001, ce qui casse Ivy (`?/.ivy2`) et l'UGI Hadoop (`LoginException`).
- MinIO tourne aussi en root pour écrire dans le volume nommé.
- Sur une machine à 8 Go, `jupyter` et `kafka` peuvent être arrêtés pendant la Partie 3
  (`docker compose stop jupyter kafka`) ; sinon `--driver-memory 512m --executor-memory 512m`.

## Versions

| Composant | Version |
|---|---|
| Spark | 3.5.6 (image `bitnamilegacy/spark:3.5`) |
| Kafka | 3.7 KRaft (`bitnamilegacy/kafka:3.7`) |
| MinIO | `bitnamilegacy/minio:2025.4.22` + `bitnamilegacy/minio-client` |
| Hadoop AWS | 3.3.4 |
| aws-java-sdk-bundle | 1.12.262 |
| Java (hôte, validation locale) | 17 |
| PySpark (hôte) | 3.5.9 |

## Notes

- `stations.json` est du **JSONL** (un objet par ligne), lu avec schéma explicite.
- Les écritures MinIO passent par `s3a://` (`path.style.access=true`, endpoint `http://minio:9000`).
- Delta Lake, Structured Streaming et GraphX/GraphFrames sont hors Jalon 1 (voir Jalon 2).
