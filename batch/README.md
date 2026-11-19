# Analyse batch (voie froide) — livrable 2

Jobs PySpark lancés **sur le cluster** (`spark://spark-master:7077`). Ils lisent l'historique archivé dans HDFS par `streaming/archive_to_hdfs.py`.

| Ordre | Script | Entrée | Sortie |
|---|---|---|---|
| 1 | `qualite.py` | `/data/raw/hospital_discharges` | `/data/quality/` |
| 2 | `preparation.py` | `/data/raw/hospital_discharges` | `/data/clean/readmission` |
| 3 | `indicateurs.py` | `/data/clean/readmission` | `/data/indicators/<indicateur>/` |
| 4 | `export_postgres.py` | `/data/indicators/` | table `indicateurs` (PostgreSQL) |

Lancer un job :

```
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/preparation.py
```

Toute la chaîne (producteur → archivage → jobs batch) : `scripts\demo_livrable2.ps1`.

## preparation.py

- Supprime les doublons sur `encounter_id` (un séjour rejoué plusieurs fois par le producteur), en gardant la première réception.
- Exclut les sorties pour décès ou hospice (`discharge_disposition_id` 11, 13, 14, 19, 20, 21) : ces patients ne peuvent pas être réadmis.
- Retire `weight` (≈ 97 % de valeurs manquantes) et les colonnes techniques Kafka.
- Remplace les valeurs manquantes des colonnes texte par `Unknown`.
- Crée la cible binaire `label` (1 = réadmis dans les 30 jours, 0 sinon).

Résultat sur l'historique du 19/11/2026 : 101 766 séjours lus → 99 343 conservés (2 423 décès/hospice), dont **11,39 %** de `label = 1` (classes déséquilibrées).
