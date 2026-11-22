---
tags: [pedali, env]
date: 2026-11-22
---
# Environnement

Retour : [[Pedali - Accueil]] · Plan : [[00 - Plan Jalon 1]]

## Machine

| Élément | Valeur |
|---|---|
| OS | Windows (win32) |
| Shell | PowerShell 5.1 |
| Java | OpenJDK 17.0.21 (Microsoft) |
| Python | 3.13 (`C:\Program Files\Python313`) |
| PySpark | 3.5.9 (local) |
| WSL | WSL2, distro par défaut Kali |
| Git | 2.55.0 |
| Docker | **absent au départ** — installation en Phase 0 |

## Données fournies

- `producer.py` : générateur stdlib, modes `file` (JSONL) et `kafka` (topic `trips`, clé `trip_id`).
- `stations.json` : **JSONL de 400 stations** (90 par ville : Casablanca, Rabat, Marrakech, Tanger).
  Champs : `station_id, name, city, district, zone_type, capacity, latitude, longitude`.
  `zone_type` ∈ {gare, centre_affaires, commercial, universite, tourisme, residentiel, plage}.
- Scénarios injectés : station qui se vide `S-CAS-074`, boucle touristique `S-MAR-002 → 004 → 005 → 002`, véhicule défectueux `V-SC-0137`, flux pendulaires.
- Qualité : ~5 % retards (≤ 15 min), ~2 % doublons (`trip_id`, `status`).

## Versions cibles du cluster

- Spark **3.5.6** (`bitnamilegacy/spark:3.5` — Bitnami n'est plus publié sur Docker Hub)
- Kafka 3.7 en KRaft (`bitnamilegacy/kafka:3.7`)
- MinIO `bitnamilegacy/minio:2025.4.22` + client `bitnamilegacy/minio-client`
- Jupyter PySpark (`jupyter/pyspark-notebook:spark-3.5.0`)
- Hadoop AWS 3.3.4 / aws-java-sdk-bundle 1.12.262 (alignés sur le Hadoop de Spark 3.5)

## Particularités de l'image Bitnami

- Pas d'entrée passwd pour l'uid 1001 → `user: root` sur Spark et MinIO
  (sinon Ivy `?/.ivy2` et UGI Hadoop `LoginException`).
- Kafka KRaft : `KAFKA_CFG_CONTROLLER_QUORUM_VOTERS` doit contenir le `NODE_ID` (`0@kafka:9093`).
- Mémoire hôte 8 Go → arrêter `jupyter`/`kafka` pendant la Partie 3 ou limiter driver/executor à 512m.
