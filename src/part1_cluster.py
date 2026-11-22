"""Partie 1 - SparkSession connectee au cluster et positionnement."""
import os

from pyspark.sql import SparkSession

MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")


def main():
    spark = SparkSession.builder.appName("Pedali Part1 Cluster").master(MASTER).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print(f"Spark version      : {spark.version}")
    print(f"master             : {sc.master}")
    print(f"appName            : {sc.appName}")
    print(f"defaultParallelism : {sc.defaultParallelism}")
    print(f"executors          : {sc._jsc.sc().getExecutorMemoryStatus().size()}")

    assert sc.defaultParallelism > 0
    assert sc.parallelize(range(1000)).sum() == 499500

    spark.stop()


if __name__ == "__main__":
    main()
