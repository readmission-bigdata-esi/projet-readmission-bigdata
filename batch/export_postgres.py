"""Export des indicateurs HDFS vers PostgreSQL.

Lancement depuis la racine du projet :
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/export_postgres.py
"""

import os

from pyspark.sql import SparkSession


INDICATORS_PATH = os.getenv(
    "INDICATORS_PATH",
    "hdfs://namenode:8020/data/indicators",
)

POSTGRES_DB = os.getenv("POSTGRES_DB", "readmission")
POSTGRES_USER = os.getenv("POSTGRES_USER", "readmission")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")

JDBC_URL = f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}"
TABLE_NAME = "indicateurs"


def main():
    spark = (
        SparkSession.builder
        .appName("export-indicateurs-postgres")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Lit tous les dossiers créés par batch/indicateurs.py.
    indicateurs = spark.read.parquet(f"{INDICATORS_PATH}/*")

    # Garde seulement les colonnes attendues par la table PostgreSQL.
    indicateurs = indicateurs.select(
        "indicateur",
        "dimension",
        "nb_sejours",
        "nb_readmis",
        "taux_readmission",
    )

    nombre_lignes = indicateurs.count()
    print(f"{nombre_lignes} indicateurs à exporter vers PostgreSQL.")

    (
        indicateurs.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TABLE_NAME)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    print("Export PostgreSQL terminé.")
    spark.stop()


if __name__ == "__main__":
    main()