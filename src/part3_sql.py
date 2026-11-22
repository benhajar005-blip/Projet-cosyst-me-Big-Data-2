"""Partie 3 - Spark SQL et DataFrames."""
import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, when
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

INPUT = os.getenv("TRIPS_INPUT", "/opt/pedali/data/trips_history.jsonl")
STATIONS = os.getenv("STATIONS", "/opt/pedali/stations.json")
OUTPUT = os.getenv("SILVER_OUT", "data/silver/trips_history")
MASTER = os.getenv("SPARK_MASTER", "local[*]")

EVENT_SCHEMA = StructType([
    StructField("trip_id", StringType()),
    StructField("user_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("start_station", StringType()),
    StructField("end_station", StringType()),
    StructField("city", StringType()),
    StructField("status", StringType()),
    StructField("duration_s", LongType()),
    StructField("distance_km", DoubleType()),
    StructField("battery_pct", IntegerType()),
    StructField("price_mad", DoubleType()),
    StructField("event_time", StringType()),
])

STATION_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("district", StringType()),
    StructField("zone_type", StringType()),
    StructField("capacity", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])


@udf(StringType())
def trip_band(duration_s):
    if duration_s is None:
        return None
    if duration_s < 600:
        return "court"
    if duration_s <= 1800:
        return "moyen"
    return "long"


def s3a_conf(builder):
    return (builder
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.hadoop.fs.s3a.endpoint",
                    os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USER", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASSWORD", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))


def main():
    builder = SparkSession.builder.appName("Pedali Part3 SQL").master(MASTER)
    if OUTPUT.startswith("s3a://"):
        builder = s3a_conf(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    events = spark.read.schema(EVENT_SCHEMA).json(INPUT)
    stations = spark.read.schema(STATION_SCHEMA).json(STATIONS)
    ended = events.filter(F.col("status") == "ENDED").cache()
    n = ended.count()
    assert n > 0, "aucun trajet ENDED"

    # 2. CA par ville : API DataFrame puis SQL
    ca_df = ended.groupBy("city").agg(F.round(F.sum("price_mad"), 2).alias("ca_mad")) \
                 .orderBy(F.desc("ca_mad"))
    ended.createOrReplaceTempView("ended")
    ca_sql = spark.sql("""
        SELECT city, ROUND(SUM(price_mad), 2) AS ca_mad
        FROM ended GROUP BY city ORDER BY ca_mad DESC
    """)
    assert ca_df.collect() == ca_sql.collect(), "DataFrame != SQL"
    print("=== CA par ville (DataFrame / SQL) ===")
    ca_df.show(truncate=False)

    # 3. Jointure referentiel stations sur la station de depart
    joined = ended.alias("t").join(
        stations.alias("s"), F.col("t.start_station") == F.col("s.station_id"), "inner")
    by_zone = joined.groupBy(F.col("s.city").alias("city"), "zone_type") \
                    .agg(F.count("*").alias("trips"),
                         F.round(F.sum("price_mad"), 2).alias("ca_mad")) \
                    .orderBy("city", F.desc("trips"))
    print("=== Trajets et CA par type de zone et par ville ===")
    by_zone.show(50, truncate=False)
    assert joined.count() == n, "jointure: lignes perdues (station de depart inconnue)"

    # 4. UDF trip_band vs when/otherwise
    band_when = (when(F.col("duration_s") < 600, "court")
                 .when(F.col("duration_s") <= 1800, "moyen")
                 .otherwise("long"))
    t_udf = time.time()
    dist_udf = ended.withColumn("band", trip_band("duration_s")) \
                    .groupBy("band").count().orderBy("band").collect()
    udf_s = time.time() - t_udf
    t_when = time.time()
    dist_when = ended.withColumn("band", band_when) \
                     .groupBy("band").count().orderBy("band").collect()
    when_s = time.time() - t_when
    assert dist_udf == dist_when, "UDF != when/otherwise"
    print("=== Repartition trip_band ===")
    for row in dist_when:
        print(f"{row['band']:<6} {row['count']:6d}")
    print(f"temps UDF : {udf_s:.2f}s | temps when : {when_s:.2f}s")

    # 5. Ecriture Parquet nettoyee et enrichie, partitionnee par city
    clean = ended.withColumn("trip_band", band_when) \
                 .join(stations.select("station_id", "zone_type", "district"),
                       ended.start_station == stations.station_id, "left") \
                 .drop("station_id")
    clean.write.mode("overwrite").partitionBy("city").parquet(OUTPUT)
    print(f"Parquet ecrit dans {OUTPUT}")

    # 6. Plan de la jointure
    print("=== explain(True) de la jointure ===")
    joined.explain(True)

    print(f"\ntemps d'execution : {time.time() - t0:.1f} s")

    pause = int(os.getenv("PEDALI_PAUSE", "0"))
    if pause:
        print(f"UI Spark sur http://localhost:4040 — pause {pause}s pour les captures")
        time.sleep(pause)

    spark.stop()


if __name__ == "__main__":
    main()
