# batch/export_postgres.py
# Exporte tous les indicateurs HDFS vers la table PostgreSQL `indicateurs`.
# Prérequis : indicateurs.py doit avoir tourné (hdfs://namenode:8020/data/indicators/ non vide).
# Lancement : docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/export_postgres.py

import os
from pyspark.sql import SparkSession

# Lecture des credentials — KeyError intentionnel si variable absente
pg_db       = os.environ["POSTGRES_DB"]
pg_user     = os.environ["POSTGRES_USER"]
pg_password = os.environ["POSTGRES_PASSWORD"]

JDBC_URL = f"jdbc:postgresql://postgres:5432/{pg_db}"
IND_BASE = "hdfs://namenode:8020/data/indicators"

spark = SparkSession.builder.appName("export_postgres").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Lecture de tous les indicateurs en un seul DataFrame
df = spark.read.parquet(f"{IND_BASE}/*")
nb_lignes = df.count()
print(f"Lignes à exporter : {nb_lignes}")

# Export JDBC — mode overwrite avec truncate=true pour conserver le schéma créé par init.sql
try:
    df.write \
        .format("jdbc") \
        .option("url",      JDBC_URL) \
        .option("dbtable",  "indicateurs") \
        .option("user",     pg_user) \
        .option("password", pg_password) \
        .option("driver",   "org.postgresql.Driver") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()
except Exception as e:
    print(
        f"Echec de l'export JDBC vers '{JDBC_URL}'.\n"
        f"La table 'indicateurs' existe-t-elle ? Vérifiez que init.sql d'Ines a été exécuté "
        f"et que le service postgres tourne (docker compose --profile restitution up -d postgres).\n"
        f"Erreur originale : {e}"
    )
    raise

print(f"Export terminé : {nb_lignes} lignes dans la table 'indicateurs'.")
print(f"Vérification : docker exec -it postgres psql -U {pg_user} -d {pg_db} "
      f"-c \"SELECT indicateur, dimension, taux_readmission FROM indicateurs "
      f"ORDER BY taux_readmission DESC LIMIT 15;\"")

spark.stop()
