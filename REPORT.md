# Rapport — Jalon 1 (Parties 1 à 3)

Projet Pedali.ma — Écosystème Big Data 2, EMSI 5A.
Dépôt : `pedali/`. Environnement : Docker Compose (Kafka KRaft, Spark 3.5 master + 2 workers, MinIO, Jupyter).

> Les chiffres ci-dessous proviennent de la validation locale (`local[*]`, 4 cœurs, 50 000 événements,
> 15 918 trajets ENDED). À compléter avec les captures du cluster (Master UI, DAG, `explain`, MinIO).

## Partie 1 — Cluster et positionnement

### Rôles dans notre déploiement

- **Driver** : exécute `main()`, crée la `SparkSession`/`SparkContext`, construit le DAG et planifie
  les tâches. Ici le driver tourne dans le conteneur `spark-master` via `spark-submit`
  (`--master spark://spark-master:7077`).
- **Executors** : JVM lancées sur `spark-worker-1` et `spark-worker-2` ; elles exécutent les tâches
  et stockent les partitions (`ended.persist()` en mémoire).
- **Cluster Manager** : le **Master Spark standalone** (port 7077, UI 8080) alloue les ressources
  aux applications et supervise les 2 workers.
- **SparkContext** : point d'entrée unique côté driver ; c'est lui qui négocie les executors avec
  le Master et expose `defaultParallelism`, `master`, `appName`.

### Deux limites de MapReduce/Hadoop corrigées par Spark

1. **Écritures disque entre chaque job** : MapReduce matérialise chaque sortie de map/reduce sur HDFS.
   Pour la Partie 2 (CA par ville puis durée moyenne, plusieurs passes sur le même historique),
   Spark garde `ended` en mémoire (`persist`) et enchaîne les stages dans un seul DAG.
2. **Pas de pipeline généraliste / latence élevée** : MapReduce impose le modèle map→reduce et
   relance un job par étape. Spark enchaîne transformations lazy et actions, ce qui permet
   d'écrire la Partie 3 comme une suite de `join`/`groupBy`/`write` sans job intermédiaire sur disque.

## Partie 2 — SparkCore (RDD)

### Transformations (lazy) vs actions

| Opération | Type |
|---|---|
| `textFile`, `map(json.loads)`, `filter(ENDED)`, `map`, `reduceByKey`, `sortBy`, `takeOrdered`, `persist` | transformations (lazy) |
| `count`, `collect`, `sum`, `takeOrdered`, `toDebugString` | actions (déclenchent un job) |

`toDebugString()` montre le lineage :
`PythonRDD[2] ← MapPartitionsRDD[1] (textFile) ← HadoopRDD[0]` après `persist`.

### Shuffle et stages

`reduceByKey` (CA par ville, durée par ville×type) et `sortBy` provoquent un **shuffle** :
les paires sont repartitionnées par clé (hash) pour que toutes les valeurs d'une même ville
arrivent sur le même reducer. Un stage est coupé à chaque frontière de shuffle ; la Spark UI
montre ces stages et le volume `Shuffle Read/Write`.

Mesuré via l'API de la Spark UI (`localhost:4040`) : **Partie 2 = 4 jobs, 5 stages**
(1 stage sauté par réutilisation du cache) ; **Partie 3 = 13 jobs, 23 stages**
(dont 9 sautés par réutilisation des mêmes agrégats).

### Lineage et tolérance aux pannes

Chaque RDD connaît la suite de transformations qui l'a produit. Si une partition est perdue
(executor tué), Spark la **recalcule** depuis la source ou depuis le dernier checkpoint, sans
rejouer tout le job. C'est ce qui évite la réplication HDFS systématique.

### Résultats (local, 50 000 événements)

| Ville | CA (MAD) |
|---|---|
| Casablanca | 114 023,63 |
| Marrakech | 59 185,71 |
| Rabat | 57 259,36 |
| Tanger | 46 185,24 |
| **Total** | **276 653,94** |

Top 5 stations de départ : `S-MAR-002` (168), `S-MAR-004` (160), `S-MAR-005` (152),
`S-CAS-001` (147), `S-MAR-001` (137) — la boucle touristique de Marrakech est bien visible.

Durées moyennes (s) : BIKE ≈ 1 400–1 674, EBIKE ≈ 1 007–1 273, SCOOTER ≈ 1 073–1 372.

Sur le cluster (2 workers, 4 cœurs), la Partie 2 s'exécute en **13,2 s** (contre 54,7 s en local,
démarrage JVM compris).

## Partie 3 — Spark SQL et DataFrames

### Schéma explicite

Deux `StructType` : événement (13 champs, `duration_s` long, `price_mad` double, `battery_pct` int)
et station (8 champs). Aucune inférence (`spark.read.schema(...).json(...)`), donc pas de scan
supplémentaire et types stables (`price_mad` reste `double`, pas de string).

### DataFrame vs SQL vs RDD

- API DataFrame et `spark.sql` donnent **le même résultat** (assertion d'égalité dans le script).
- RDD : le parsing JSON et le typage sont manuels, aucune optimisation possible.
- DataFrame : Catalyst réordonne les filtres, pousse les prédicats et choisit la jointure ;
  Tungsten gère le format binaire en mémoire.

### Jointure et plan physique

`explain(True)` sur la jointure `trips ENDED ⋈ stations` montre :

- **Parsed** → **Analyzed** (résolution des colonnes) → **Optimized** (filtre `isnotnull`,
  cache `InMemoryRelation`) → **Physical** ;
- stratégie retenue : **BroadcastHashJoin** (BuildRight), car `stations` fait 400 lignes :
  Spark diffuse le petit côté sur chaque executor et évite un shuffle de la table de trajets.
  `SortMergeJoin` apparaîtrait si le référentiel était volumineux.

### UDF vs `when/otherwise`

`trip_band` : court < 10 min, moyen 10–30 min, long > 30 min.

| Implémentation | Temps (local) | Remarque |
|---|---|---|
| UDF Python | 8,04 s | sérialisation Python ↔ JVM par ligne, Catalyst ne voit pas la logique |
| `when/otherwise` | 1,08 s | expression native, optimisée par Catalyst, codegen Tungsten |

Répartition identique (assertion) : court 4 805, moyen 7 312, long 3 801.
Conclusion : préférer les fonctions natives ; réserver les UDF aux logiques impossibles à exprimer en SQL.

### Parquet Silver

`ended` enrichi (`trip_band`, `zone_type`, `district`) écrit en Parquet
`partitionBy("city")` → dossiers `city=Casablanca/…`, `city=Marrakech/…`, etc.
Dans le cluster la cible est `s3a://lake/silver/trips_history/`.

### RDD vs DataFrame vs Dataset

| | RDD | DataFrame | Dataset |
|---|---|---|---|
| Typage | objets Python, non typé | non typé (Row) | typé (JVM/Scala) |
| Optimisation | aucune | Catalyst + Tungsten | Catalyst + Tungsten |
| Notre usage | Partie 2 (contrôle bas niveau) | Parties 3–5 | non utilisé (PySpark) |

Sur ce jeu, la Partie 3 est plus rapide que la Partie 2 en local (35,7 s vs 54,7 s) malgré la
jointure et l'écriture Parquet, grâce au schéma explicite et au plan optimisé. Sur le cluster,
la Partie 3 prend 59,3 s, dont ~60 s de résolution Ivy des jars S3A au premier lancement ;
hors téléchargement, le job est du même ordre que la Partie 2 (13,2 s).

## Limites

- Mesures locales bruitées par le démarrage JVM (Windows) ; refaire les temps sur le cluster.
- `price_mad` est arrondi à 2 décimales dans le générateur ; les agrégats sont donc exacts au centime.
