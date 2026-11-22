"""Partie 2 - Traitement batch avec SparkCore (RDD)."""
import json
import os
import time
from operator import add

from pyspark.sql import SparkSession

INPUT = os.getenv("TRIPS_INPUT", "/opt/pedali/data/trips_history.jsonl")
MASTER = os.getenv("SPARK_MASTER", "local[*]")


def main():
    spark = SparkSession.builder.appName("Pedali Part2 RDD").master(MASTER).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    t0 = time.time()

    events = sc.textFile(INPUT).map(json.loads)
    ended = events.filter(lambda e: e["status"] == "ENDED")
    ended.persist()

    n = ended.count()
    assert n > 0, "aucun trajet ENDED"

    revenue = ended.map(lambda e: (e["city"], e["price_mad"])) \
                   .reduceByKey(add) \
                   .sortBy(lambda kv: -kv[1]) \
                   .collect()

    duration = ended.map(lambda e: ((e["city"], e["vehicle_type"]), (e["duration_s"], 1))) \
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                    .mapValues(lambda s: s[0] / s[1]) \
                    .sortBy(lambda kv: kv[0]) \
                    .collect()

    top5 = ended.map(lambda e: (e["start_station"], 1)) \
                .reduceByKey(add) \
                .takeOrdered(5, key=lambda kv: -kv[1])

    total = ended.map(lambda e: e["price_mad"]).sum()
    assert abs(sum(v for _, v in revenue) - total) < 0.01, "CA par ville != CA total"

    print(f"trajets ENDED : {n}")
    print("\n=== Chiffre d'affaires par ville (MAD) ===")
    for city, ca in revenue:
        print(f"{city:<12} {ca:10.2f}")
    print(f"{'TOTAL':<12} {total:10.2f}")

    print("\n=== Duree moyenne par ville et type (s) ===")
    for (city, vt), d in duration:
        print(f"{city:<12} {vt:<8} {d:8.1f}")

    print("\n=== Top 5 stations de depart ===")
    for station, c in top5:
        print(f"{station:<12} {c:6d}")
    assert len(top5) == 5

    print("\n=== Lineage ===")
    print(ended.toDebugString().decode())
    print(f"\ntemps d'execution : {time.time() - t0:.1f} s")

    pause = int(os.getenv("PEDALI_PAUSE", "0"))
    if pause:
        print(f"UI Spark sur http://localhost:4040 — pause {pause}s pour les captures")
        time.sleep(pause)

    ended.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
