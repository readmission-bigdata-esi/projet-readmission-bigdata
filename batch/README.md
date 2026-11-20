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

Prérequis : image Spark reconstruite après le livrable 2 (driver PostgreSQL) et PostgreSQL démarré : `docker compose --profile restitution up -d --build`. `export_postgres.py` lit `POSTGRES_DB`, `POSTGRES_USER` et `POSTGRES_PASSWORD` (définies pour `spark-master` dans `docker-compose.yml`).

## qualite.py

Rapport sur les données brutes, écrit dans `/data/quality/{nulls,doublons,patients,cible}` : 101 766 lignes, 0 doublon sur `encounter_id` ; `weight` vide à 96,86 %, `medical_specialty` à 49,08 %, `payer_code` à 39,56 % ; 16 773 des 71 518 patients (23,45 %) ont plusieurs séjours → découpage train/test **par patient** au livrable 3 ; cible : NO 53,91 %, >30 34,93 %, <30 11,16 %.

## preparation.py

- Supprime les doublons sur `encounter_id` (un séjour rejoué plusieurs fois par le producteur), en gardant la première réception.
- Exclut les sorties pour décès ou hospice (`discharge_disposition_id` 11, 13, 14, 19, 20, 21) : ces patients ne peuvent pas être réadmis.
- Retire `weight` (≈ 97 % de valeurs manquantes) et les colonnes techniques Kafka.
- Remplace les valeurs manquantes des colonnes texte par `Unknown`.
- Crée la cible binaire `label` (1 = réadmis dans les 30 jours, 0 sinon).

Résultat sur l'historique du 19/11/2026 : 101 766 séjours lus → 99 343 conservés (2 423 décès/hospice), dont **11,39 %** de `label = 1` (classes déséquilibrées).

## indicateurs.py

Dix requêtes Spark SQL sur `/data/clean/readmission` (taux global, âge, sexe, hospitalisations antérieures, durée de séjour, HbA1c, insuline, changement de traitement, type d'admission, famille du diagnostic principal). Chaque indicateur a le même schéma (`indicateur`, `dimension`, `nb_sejours`, `nb_readmis`, `taux_readmission`) et est écrit dans `/data/indicators/<indicateur>/` — 48 lignes au total. Résultats et interprétation : section 6.3 du [README](../README.md#63-résultats).

## export_postgres.py

Lit tous les sous-dossiers de `/data/indicators/` et les écrit par JDBC dans la table `indicateurs` (mode `overwrite` + `truncate` : la structure de `infra/postgres/init.sql` est conservée, la colonne `calcule_le` est remplie automatiquement). Requêtes de contrôle : `infra/postgres/queries.sql`.
