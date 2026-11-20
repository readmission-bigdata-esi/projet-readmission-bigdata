"""Livrable 2 - préparation des données (voie froide, batch).

Lit l'historique brut archivé par streaming/archive_to_hdfs.py, le nettoie et
écrit un jeu de données propre, prêt pour les indicateurs (L2) et le ML (L3).

  entrée : hdfs://namenode:8020/data/raw/hospital_discharges   (Parquet brut)
  sortie : hdfs://namenode:8020/data/clean/readmission           (Parquet propre)

Lancement sur le cluster (depuis la racine du projet) :
  docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/preparation.py
"""
import os

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StringType

RAW_PATH = os.getenv("RAW_PATH", "hdfs://namenode:8020/data/raw/hospital_discharges")
CLEAN_PATH = os.getenv("CLEAN_PATH", "hdfs://namenode:8020/data/clean/readmission")

# Sorties d'hôpital vers un décès ou un hospice (codes UCI de discharge_disposition_id) :
# le patient ne peut pas être réadmis, ces séjours fausseraient la cible.
DISCHARGE_DEATH_OR_HOSPICE = [11, 13, 14, 19, 20, 21]

# Colonnes trop vides pour être exploitées (weight : ~97 % de valeurs manquantes)
DROPPED_COLUMNS = ["weight"]

# Colonnes techniques ajoutées par Kafka et le producteur : inutiles pour l'analyse
TECHNICAL_COLUMNS = ["kafka_partition", "kafka_offset", "kafka_timestamp",
                     "seq", "source", "event_type"]


def main():
    spark = SparkSession.builder.appName("preparation-donnees").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.parquet(RAW_PATH)
    nb_brut = raw.count()

    # 1. Doublons : un même séjour (encounter_id) peut avoir été rejoué plusieurs fois
    #    par le producteur. On garde la première réception (kafka_timestamp le plus ancien).
    premier = F.row_number().over(
        Window.partitionBy("encounter_id").orderBy(F.col("kafka_timestamp").asc()))
    dedup = raw.withColumn("_rang", premier).filter(F.col("_rang") == 1).drop("_rang")
    nb_dedup = dedup.count()

    # 2. On retire les séjours terminés par un décès ou un transfert en hospice
    vivants = dedup.filter(
        F.col("discharge_disposition_id").isNull()
        | ~F.col("discharge_disposition_id").isin(DISCHARGE_DEATH_OR_HOSPICE))
    nb_vivants = vivants.count()

    # 3. Colonnes inutiles : trop vides ou purement techniques
    a_retirer = [c for c in DROPPED_COLUMNS + TECHNICAL_COLUMNS + ["ingest_date"]
                 if c in vivants.columns]
    propre = vivants.drop(*a_retirer)

    # 4. Valeurs manquantes des colonnes texte -> "Unknown" (sauf la cible)
    texte = [f.name for f in propre.schema.fields
             if isinstance(f.dataType, StringType) and f.name not in ("readmitted", "event_id")]
    propre = propre.fillna("Unknown", subset=texte)

    # 5. Cible binaire : 1 = réadmis dans les 30 jours, 0 sinon (">30" ou "NO")
    #    readmitted est conservée pour l'analyse, mais ne sera jamais une variable explicative.
    propre = propre.withColumn("label", (F.col("readmitted") == "<30").cast("int"))

    propre.coalesce(4).write.mode("overwrite").parquet(CLEAN_PATH)

    # Résumé
    final = spark.read.parquet(CLEAN_PATH)
    nb_final = final.count()
    print("\n===== PRÉPARATION DES DONNÉES =====")
    print(f"Lignes brutes lues           : {nb_brut}")
    print(f"Après suppression doublons   : {nb_dedup}  (-{nb_brut - nb_dedup})")
    print(f"Après exclusion décès/hospice: {nb_vivants}  (-{nb_dedup - nb_vivants})")
    print(f"Lignes écrites               : {nb_final}  -> {CLEAN_PATH}")
    print(f"Colonnes retirées            : {', '.join(a_retirer)}")
    print("\nRépartition de la cible (label = 1 : réadmis < 30 jours) :")
    (final.groupBy("label").count()
     .withColumn("pourcentage", F.round(F.col("count") * 100 / nb_final, 2))
     .orderBy("label").show())

    spark.stop()


if __name__ == "__main__":
    main()
