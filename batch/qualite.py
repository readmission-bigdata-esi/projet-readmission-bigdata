# -*- coding: utf-8 -*-
"""
Rapport qualité des données brutes (livrable 2).

Lit l'archive Parquet brute dans HDFS et produit 4 tables de qualité :
  1. nulls    : % de valeurs manquantes par colonne
  2. doublons : nombre de lignes, de séjours distincts et de doublons sur encounter_id
  3. patients : patients ayant plusieurs séjours
  4. cible    : répartition de la cible "readmitted"

Lancement (depuis le dossier du projet) :
  docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/qualite.py

Résultats écrits dans hdfs://namenode:8020/data/quality/<nom>/ (Parquet, overwrite).
"""
from pyspark.sql import SparkSession, functions as F

CHEMIN_BRUT = "hdfs://namenode:8020/data/raw/hospital_discharges"
CHEMIN_QUALITE = "hdfs://namenode:8020/data/quality"

# Colonnes ajoutées par le pipeline (pas des données médicales) : exclues du calcul des null.
COLONNES_TECHNIQUES = [
    "event_id", "event_type", "event_time", "seq", "source",
    "kafka_partition", "kafka_offset", "kafka_timestamp", "ingest_date",
]

spark = SparkSession.builder.appName("qualite-donnees").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

brut = spark.read.parquet(CHEMIN_BRUT)
nb_lignes = brut.count()
if nb_lignes == 0:
    print("Aucune ligne dans l'archive brute : rien à analyser.")
    spark.stop()
    raise SystemExit(1)
print(f"Lignes dans l'archive brute : {nb_lignes}")

# 1. Pourcentage de null par colonne (du plus incomplet au moins incomplet)
colonnes = [c for c in brut.columns if c not in COLONNES_TECHNIQUES]
comptes = brut.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in colonnes]
).first().asDict()
lignes_nulls = [
    (c, int(comptes[c] or 0), round(100.0 * (comptes[c] or 0) / nb_lignes, 2))
    for c in colonnes
]
nulls = spark.createDataFrame(
    lignes_nulls, ["colonne", "nb_null", "pct_null"]
).orderBy(F.desc("pct_null"))

# 2. Doublons sur encounter_id
nb_distincts = brut.select("encounter_id").distinct().count()
doublons = spark.createDataFrame(
    [(nb_lignes, nb_distincts, nb_lignes - nb_distincts)],
    ["nb_lignes", "nb_sejours_distincts", "nb_doublons"],
)

# Pour les points 3 et 4 on travaille sur les séjours dédoublonnés
sejours = brut.dropDuplicates(["encounter_id"])
nb_sejours = sejours.count()

# 3. Patients ayant plusieurs séjours
par_patient = sejours.groupBy("patient_nbr").agg(F.count("*").alias("nb_sejours"))
patients = par_patient.agg(
    F.count("*").alias("nb_patients"),
    F.sum((F.col("nb_sejours") > 1).cast("int")).alias("nb_patients_multi_sejours"),
    F.max("nb_sejours").alias("max_sejours_par_patient"),
).withColumn(
    "pct_multi_sejours",
    F.round(100.0 * F.col("nb_patients_multi_sejours") / F.col("nb_patients"), 2),
)

# 4. Répartition de la cible readmitted (<30, >30, NO)
cible = (
    sejours.groupBy("readmitted")
    .agg(F.count("*").alias("nb"))
    .withColumn("pct", F.round(100.0 * F.col("nb") / nb_sejours, 2))
    .orderBy(F.desc("nb"))
)

# Écriture dans HDFS et affichage
for nom, tableau in [("nulls", nulls), ("doublons", doublons),
                     ("patients", patients), ("cible", cible)]:
    tableau.write.mode("overwrite").parquet(f"{CHEMIN_QUALITE}/{nom}")
    print(f"\n=== {nom} ===")
    tableau.show(60, truncate=False)

spark.stop()
# Interprétation :
# 1. Colonnes très incomplètes : weight (96,86 % de null), medical_specialty (49,08 %) et
#    payer_code (39,56 %) sont inutilisables ou à écarter ; race (2,23 %) et diag_1/2/3 (0,02 à 1,4 %)
#    sont peu touchées et peuvent être imputées ou traitées par une catégorie "inconnu".
# 2. Doublons et patients à plusieurs séjours : aucun doublon sur encounter_id (101 766 séjours distincts),
#    mais 16 773 des 71 518 patients (23,45 %) ont plusieurs séjours (jusqu'à 40). Il faudra séparer
#    train/test par patient_nbr pour éviter qu'un même patient apparaisse des deux côtés.
# 3. Déséquilibre de la cible : NO 53,91 %, >30 34,93 %, <30 11,16 %. La classe "<30" est minoritaire :
#    prévoir une stratification et des métriques adaptées (rappel, F1, AUC) plutôt que la seule accuracy.