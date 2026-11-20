# batch/indicateurs.py
# Calcule les taux de réadmission par dimension clinique (Spark SQL).
# Lancement : docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/indicateurs.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("indicateurs").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

CLEAN_PATH = "hdfs://namenode:8020/data/clean/readmission"
IND_BASE   = "hdfs://namenode:8020/data/indicators"

df = spark.read.parquet(CLEAN_PATH)
df.createOrReplaceTempView("readmission")
print(f"Données chargées : {df.count()} séjours.")


def save_indicator(query: str, name: str) -> None:
    """Exécute la requête SQL, écrit en Parquet et affiche trié par taux décroissant."""
    result = spark.sql(query)
    result.write.mode("overwrite").parquet(f"{IND_BASE}/{name}")
    print(f"\n=== {name} ===")
    result.orderBy("taux_readmission", ascending=False).show(truncate=False)


# 1. Taux global
save_indicator("""
    SELECT
        'global'       AS indicateur,
        'global'       AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
""", "global")

# 2. Par tranche d'âge
save_indicator("""
    SELECT
        'age'          AS indicateur,
        age            AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY age
""", "age")

# 3. Par genre
save_indicator("""
    SELECT
        'gender'       AS indicateur,
        gender         AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY gender
""", "gender")

# 4. Par number_inpatient (regroupé 0 / 1 / 2 / 3+)
save_indicator("""
    SELECT
        'number_inpatient' AS indicateur,
        CASE
            WHEN number_inpatient = 0 THEN '0'
            WHEN number_inpatient = 1 THEN '1'
            WHEN number_inpatient = 2 THEN '2'
            ELSE '3+'
        END AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY 2
""", "number_inpatient")

# 5. Par durée de séjour (1-3 / 4-7 / 8-14 jours)
save_indicator("""
    SELECT
        'time_in_hospital' AS indicateur,
        CASE
            WHEN time_in_hospital BETWEEN 1 AND 3 THEN '1-3 jours'
            WHEN time_in_hospital BETWEEN 4 AND 7 THEN '4-7 jours'
            WHEN time_in_hospital BETWEEN 8 AND 14 THEN '8-14 jours'
            ELSE '>14 jours (hors plage)'
        END AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY 2
""", "time_in_hospital")

# 6. Par résultat HbA1c
save_indicator("""
    SELECT
        'A1Cresult'    AS indicateur,
        A1Cresult      AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY A1Cresult
""", "A1Cresult")

# 7. Par insuline
save_indicator("""
    SELECT
        'insulin'      AS indicateur,
        insulin        AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY insulin
""", "insulin")

# 8. Par changement de médication (change)
save_indicator("""
    SELECT
        'change'       AS indicateur,
        change         AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY change
""", "change")

# 9. Par type d'admission
save_indicator("""
    SELECT
        'admission_type_id'          AS indicateur,
        CAST(admission_type_id AS STRING) AS dimension,
        COUNT(*)                     AS nb_sejours,
        SUM(label)                   AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    GROUP BY admission_type_id
""", "admission_type_id")

# 10. Par famille de diagnostic principal (ICD-9)
#     Codes V-codes et E-codes → 'Autre'
save_indicator("""
    SELECT
        'diag_1'       AS indicateur,
        CASE
            WHEN diag_1 RLIKE '^250'                            THEN 'Diabete'
            WHEN diag_1 RLIKE '^(39[0-9]|4[0-5][0-9]|785)'    THEN 'Circulatoire'
            WHEN diag_1 RLIKE '^(4[6-9][0-9]|5[01][0-9]|786)' THEN 'Respiratoire'
            WHEN diag_1 RLIKE '^(5[2-7][0-9]|787)'             THEN 'Digestif'
            WHEN diag_1 RLIKE '^[89][0-9][0-9]'                THEN 'Blessure'
            WHEN diag_1 RLIKE '^7[1-3][0-9]'                   THEN 'Musculo-squelettique'
            WHEN diag_1 RLIKE '^(5[89][0-9]|6[0-2][0-9]|788)' THEN 'Genito-urinaire'
            WHEN diag_1 RLIKE '^(1[4-9][0-9]|2[0-3][0-9])'    THEN 'Neoplasme'
            ELSE 'Autre'
        END AS dimension,
        COUNT(*)       AS nb_sejours,
        SUM(label)     AS nb_readmis,
        ROUND(SUM(label) * 100.0 / COUNT(*), 2) AS taux_readmission
    FROM readmission
    WHERE diag_1 IS NOT NULL
    GROUP BY 2
""", "diag_1")

spark.stop()

# Interprétations (exécution du 20/11/2026 sur 99 343 séjours, taux global 11,39 %) :
# 1. number_inpatient est l'indicateur le plus discriminant : 8,59 % sans hospitalisation antérieure,
#    13,25 % (1), 17,93 % (2) et 26,40 % pour 3+ — le risque est multiplié par 3.
# 2. Le diabète comme diagnostic principal (13,10 %) et un ajustement de l'insuline pendant le séjour
#    (Down 14,22 %, Up 13,33 % contre 10,21 % sans insuline) signalent un diabète instable.
#    Les séjours longs sont aussi plus à risque (8-14 jours : 13,85 % contre 9,86 % pour 1-3 jours).
# 3. Quand l'HbA1c n'est pas mesurée (A1Cresult = None), le taux est plus élevé (11,68 %) que lorsqu'elle
#    l'est (9,77 à 10,15 %). Le sexe et le type d'admission sont peu discriminants (écarts < 1,5 point).
# Action de prévention : suivi dans les 30 jours (appel ou consultation) pour les patients ayant au moins
# 2 hospitalisations dans l'année ou dont l'insuline a été ajustée, et mesure systématique de l'HbA1c.
