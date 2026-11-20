# Prédiction des réadmissions hospitalières des patients diabétiques à l'aide d'une architecture Big Data

Projet **Big Data Plateformes / Big Data Analytics** — ESI Rabat, 3A ISITD, 2026-2027
Encadrante : **Pr. El Ghali Btihal** · Équipe : BENRBIB Aymen, AMIRACHE Zaid, ZIANE Marya, KARAM Ines

---

## Sommaire

1. [Le projet en bref](#1-le-projet-en-bref)
2. [État des livrables](#2-état-des-livrables)
3. [Architecture et services du cluster](#3-architecture-et-services-du-cluster)
4. [Démarrage rapide](#4-démarrage-rapide)
5. [Livrable 1 — Cluster et couche d'ingestion](#5-livrable-1--cluster-et-couche-dingestion)
6. [Livrable 2 — Analyse batch](#6-livrable-2--analyse-batch)
7. [Livrables 3 et 4 — à venir](#7-livrables-3-et-4--à-venir)
8. [Documentation](#8-documentation)
9. [Organisation du dépôt](#9-organisation-du-dépôt)
10. [Équipe et contributions](#10-équipe-et-contributions)

---

## 1. Le projet en bref

| | |
|---|---|
| **Problème** | À la sortie d'hôpital d'un patient diabétique, prédire s'il sera **réhospitalisé dans les 30 jours**. |
| **Tâche ML** | Classification binaire — Random Forest (Spark MLlib), comparée à deux autres techniques. |
| **Données** | *Diabetes 130-US Hospitals for Years 1999–2008* (UCI, **101 766 séjours**, licence CC BY 4.0). Détails : [docs/donnees.md](docs/donnees.md). |
| **Pipeline** | Kafka (ingestion) → Spark Structured Streaming → HDFS (stockage) → Spark SQL / MLlib (analyse) → PostgreSQL (restitution). |

---

## 2. État des livrables

| Livrable | Contenu | État | Section |
|---|---|---|---|
| **1** | Cluster Docker, figure d'architecture, couche d'ingestion Kafka → HDFS | ✅ terminé | [§ 5](#5-livrable-1--cluster-et-couche-dingestion) |
| **2** | Kafka → Spark → HDFS → analyse batch (préparation des données, indicateurs Spark SQL, export PostgreSQL) | 🔄 en cours | [§ 6](#6-livrable-2--analyse-batch) |
| **3** | Pipeline ML : 3 modèles, tuning du Random Forest, modèle versionné | à venir | [§ 7](#7-livrables-3-et-4--à-venir) |
| **4** | Inférence en streaming → PostgreSQL → tableau de bord | à venir | [§ 7](#7-livrables-3-et-4--à-venir) |

---

## 3. Architecture et services du cluster

![Architecture](docs/architecture.png)

*Trait plein : réalisé · trait pointillé : livrables suivants.*

| Conteneur | Image (version fixée) | Rôle | Accès hôte |
|---|---|---|---|
| `namenode` | `apache/hadoop:3.4.2` | NameNode HDFS (métadonnées) | http://localhost:9870 |
| `datanode1`, `datanode2` | `apache/hadoop:3.4.2` | DataNodes, **réplication 2**, volumes distincts | — |
| `spark-master` | `apache/spark:4.1.3` (+ numpy) | Master Spark standalone, soumission des jobs | http://localhost:8080 · :4040 |
| `spark-worker-1`, `spark-worker-2` | idem | Workers (executors) | http://localhost:18081 · :18082 |
| `kafka` | `apache/kafka:4.2.2` | Broker Kafka en mode KRaft (sans ZooKeeper) | `localhost:29092` |
| `kafka-init` | `apache/kafka:4.2.2` | Crée les topics `hospital-discharges` (3 partitions) et `hospital-discharges-dlq`, puis s'arrête | — |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | Interface d'observation des topics et messages | http://localhost:8090 |
| `producer` | `python:3.11.11-slim` + `confluent-kafka 2.6.1` | Rejoue le CSV dans Kafka (profil `ingestion`) | — |
| `postgres` | `postgres:16.6-alpine` | Base de restitution (profil `restitution`) | :5432 |
| `kafka-exporter` | `danielqsj/kafka-exporter:latest` | Métriques Kafka pour Prometheus (profil `monitoring`) | :9308 |
| `prometheus` | `prom/prometheus:v2.52.0` | Collecte des métriques (profil `monitoring`) | http://localhost:9090 |
| `grafana` | `grafana/grafana:10.4.3` | Tableau de bord de supervision (profil `monitoring`) | http://localhost:3000 |

---

## 4. Démarrage rapide

**Prérequis**
- Docker Desktop (Windows / macOS) ou Docker Engine + Compose v2.24 ou plus récent.
- **8 Go de RAM** alloués à Docker, 4 cœurs, environ 6 Go de disque.
- Ports libres : 9870, 8080, 18081, 18082, 8090, 4040, 29092 (modifiables dans `.env`). Arrêter d'abord les clusters des TP précédents.

**Commandes** (depuis la racine du dépôt, PowerShell ; les scripts `.sh` sont les équivalents Linux) :

```powershell
Copy-Item .env.example .env                                              # configuration (aucun secret commité)
powershell -ExecutionPolicy Bypass -File scripts\download_dataset.ps1    # dataset UCI dans ./data
docker compose build spark-master                                        # image Spark du projet

powershell -ExecutionPolicy Bypass -File scripts\demo_livrable1.ps1      # démo complète du livrable 1
powershell -ExecutionPolicy Bypass -File scripts\demo_livrable2.ps1      # démo complète du livrable 2
```

**Arrêt :** `docker compose --profile ingestion --profile monitoring --profile restitution down` arrête tout et **conserve les données** (volumes HDFS, Kafka, PostgreSQL). Ajouter `-v` pour repartir de zéro.

---

## 5. Livrable 1 — Cluster et couche d'ingestion

**Attendu :** cluster créé, figure d'architecture, couche d'ingestion avec Kafka, le tout sur GitHub. ✅

### 5.1 Ce qui a été réalisé

1. **Cluster Docker** : HDFS (1 NameNode + 2 DataNodes, réplication 2), Spark standalone (1 master + 2 workers), Kafka KRaft — versions fixées (§ 3).
2. **Producteur** (`ingestion/producer.py`) : lit `diabetic_data.csv` et publie **un événement JSON par sortie d'hôpital** dans le topic `hospital-discharges`.
   - **Clé :** `patient_nbr` → les séjours d'un même patient vont dans la même partition et restent ordonnés.
   - **Livraison fiable :** `acks=all` + producteur idempotent → pas de perte ni de doublon.
   - **Contenu :** `event_id`, `event_time` (UTC), `seq` et les 50 colonnes du séjour, typées, avec les `?` remplacés par `null`.
   - **Dead Letter Queue :** une ligne invalide part dans `hospital-discharges-dlq` (lisible avec `ingestion/dlq_consumer.py`) au lieu d'arrêter le flux.
3. **Topic Kafka** `hospital-discharges` : 3 partitions, rétention 7 jours, créé par `kafka-init`.
4. **Archivage (voie froide)** (`streaming/archive_to_hdfs.py`) : job **Spark Structured Streaming** sur le cluster.
   - **Source :** Kafka · **Sortie :** Parquet dans `hdfs://namenode:8020/data/raw/hospital_discharges`, partitionné par `ingest_date`.
   - **Reprise :** checkpoint dans HDFS → redémarrage exactement là où le job s'était arrêté.
5. **Supervision** (profil `monitoring`) : Prometheus + Grafana + kafka-exporter.

La cible `readmitted` voyage dans l'événement **uniquement** pour l'entraînement et l'évaluation : elle n'est connue que 30 jours après la sortie et **n'est jamais une variable explicative**.

### 5.2 Lancement pas à pas

> Raccourci : `scripts\demo_livrable1.ps1` enchaîne toutes les étapes et termine par la vérification.

```powershell
docker compose up -d                                                     # cluster : HDFS, Spark, Kafka, Kafka UI
powershell -ExecutionPolicy Bypass -File scripts\start_archive.ps1       # archivage Kafka -> HDFS (Structured Streaming)
docker compose --profile ingestion up -d producer                        # producteur : rejoue le CSV (20 évts/s)
docker compose logs -f producer                                          # suivi (Ctrl+C pour quitter)
powershell -ExecutionPolicy Bypass -File scripts\verify_livrable1.ps1    # vérification de toute la chaîne
python scripts\healthcheck.py                                            # état HDFS / Kafka / Spark (code 0 = OK)
docker compose --profile monitoring up -d                                # supervision : http://localhost:3000 (admin / admin)
```

Réglages du producteur dans `.env` : `EVENTS_PER_SECOND=0` (vitesse maximale), `MAX_EVENTS=5000` (5 000 premiers séjours), `LOOP=true` (flux continu).

### 5.3 Vérification

| Contrôle | Résultat | Détail |
|---|---|---|
| Lignes archivées dans HDFS | **101 766** / 101 766 | [docs/verification_output_livrable1.md](docs/verification_output_livrable1.md) |
| Doublons sur `encounter_id` | **0** | idem |
| Schéma (59 colonnes) | ✅ conforme | idem |
| Valeurs `?` résiduelles | **0** | idem |
| Réplication HDFS / santé du système de fichiers | 2,0 / HEALTHY | [docs/verification_livrable1.txt](docs/verification_livrable1.txt) |

### 5.4 Captures — livrable 1

| | |
|---|---|
| ![](docs/captures/01_hdfs_overview.png) | ![](docs/captures/02_hdfs_datanodes.png) |
| **L1 · HDFS – NameNode** (localhost:9870) : cluster actif, 2 DataNodes vivants. | **L1 · HDFS – DataNodes** : `datanode1` et `datanode2` *In Service*. |
| ![](docs/captures/03_hdfs_archive_parquet.png) | ![](docs/captures/04_spark_master_workers.png) |
| **L1 · HDFS – archive brute** : fichiers Parquet dans `/data/raw/hospital_discharges`, réplication 2. | **L1 · Spark master** (localhost:8080) : 2 workers ALIVE, application `archive-kafka-to-hdfs` RUNNING. |
| ![](docs/captures/05_spark_streaming_query.png) | ![](docs/captures/06_spark_streaming_stats.png) |
| **L1 · Structured Streaming** (localhost:4040) : requête `archive_raw` active. | **L1 · Statistiques de la requête** : débit d'entrée et durée des micro-batchs. |
| ![](docs/captures/07_kafka_topic.png) | ![](docs/captures/08_kafka_messages.png) |
| **L1 · Kafka UI** (localhost:8090) : topic `hospital-discharges`, 3 partitions, 101 766 messages. | **L1 · Kafka UI – messages** : événements JSON, clé `patient_nbr`. |
| ![](docs/captures/09_grafana_dashboard.png) | ![](docs/captures/10_grafana_dashboard_live.png) |
| **L1 · Grafana** : tableau de bord de supervision du pipeline. | **L1 · Grafana en direct** : débit d'ingestion et panneaux DLQ pendant un envoi. |

**Panneaux Grafana :** messages ingérés par seconde (par partition), taille des topics `hospital-discharges` et `hospital-discharges-dlq`, brokers actifs, messages rejetés vers la DLQ (débit et total). Le retard de lecture de Spark se lit dans l'onglet Structured Streaming (localhost:4040), car Spark garde ses offsets dans son checkpoint et non dans un *consumer group* Kafka.

---

## 6. Livrable 2 — Analyse batch

**Attendu :** démonstration **Kafka → Spark → HDFS → analyse batch**. 🔄 en cours

Les jobs batch lisent l'historique archivé dans HDFS et s'exécutent **sur le cluster** (`spark://spark-master:7077`). Détails : [batch/README.md](batch/README.md).

### 6.1 Chaîne de traitement

| Ordre | Étape | Script | Entrée (HDFS) | Sortie | État |
|---|---|---|---|---|---|
| 1 | Qualité des données | `batch/qualite.py` | `/data/raw/hospital_discharges` | `/data/quality/` | ✅ |
| 2 | Préparation | `batch/preparation.py` | `/data/raw/hospital_discharges` | `/data/clean/readmission` | ✅ |
| 3 | Indicateurs Spark SQL | `batch/indicateurs.py` | `/data/clean/readmission` | `/data/indicators/<indicateur>/` | ✅ |
| 4 | Restitution | `batch/export_postgres.py` | `/data/indicators/` | table `indicateurs` (PostgreSQL) | à venir |

### 6.2 Lancement

Toute la démonstration en une commande (cluster → archivage → producteur → jobs batch dans l'ordre) :

```powershell
powershell -ExecutionPolicy Bypass -File scripts\demo_livrable2.ps1                 # 5 000 événements à 200/s
powershell -ExecutionPolicy Bypass -File scripts\demo_livrable2.ps1 -Events 20000 -Rate 500
```

> **Après un `git pull` :** l'image Spark embarque `infra/spark/spark-defaults.conf` (driver PostgreSQL ajouté au livrable 2). Le script de démo la reconstruit (`--build`) ; à la main : `docker compose --profile restitution up -d --build`.

Ou job par job, une fois le cluster et l'archivage démarrés :

```powershell
docker compose --profile restitution up -d --build        # cluster + PostgreSQL, image Spark à jour
docker exec postgres psql -U readmission -d readmission -f /docker-entrypoint-initdb.d/init.sql   # table indicateurs (idempotent)
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/qualite.py
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/preparation.py
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/indicateurs.py
docker exec spark-master /opt/spark/bin/spark-submit /opt/app/batch/export_postgres.py
```

**Vérification dans PostgreSQL** (table `indicateurs` définie dans `infra/postgres/init.sql` ; ce script ne s'exécute automatiquement qu'à la première création du volume, d'où la commande ci-dessus) :

```powershell
docker exec postgres psql -U readmission -d readmission -c "SELECT indicateur, dimension, taux_readmission FROM indicateurs ORDER BY taux_readmission DESC LIMIT 15;"
```

Requêtes complémentaires (top 15, résumé par indicateur, nombre de lignes exportées) : [`infra/postgres/queries.sql`](infra/postgres/queries.sql).

**Emplacement des résultats :** données nettoyées `/data/clean/readmission` (HDFS) · indicateurs `/data/indicators/` (HDFS) · table `indicateurs` (PostgreSQL).

### 6.3 Résultats

**Préparation des données** (`preparation.py`, exécutée sur le cluster le 19/11/2026) :

| Étape | Séjours |
|---|---|
| Séjours lus dans HDFS | 101 766 |
| Après suppression des doublons (`encounter_id`) | 101 766 |
| Après exclusion des décès / hospices | **99 343** (−2 423) |
| Réadmis < 30 jours (`label = 1`) | 11 314 (**11,39 %**) |

- Les patients décédés ou partis en hospice (`discharge_disposition_id` 11, 13, 14, 19, 20, 21) ne peuvent pas être réadmis : ils sont exclus pour ne pas fausser la cible.
- La classe positive ne représente que **11,39 %** des séjours : au livrable 3, on évaluera avec la précision, le rappel et le F1, pas l'exactitude seule.
- `weight` (≈ 97 % de valeurs manquantes) et les colonnes techniques Kafka sont retirées ; les valeurs texte manquantes deviennent `Unknown`.

**Qualité des données** (`qualite.py`, détail et interprétation dans [batch/README.md](batch/README.md)) : 101 766 lignes, **0 doublon** ; `weight` vide à 96,9 %, `medical_specialty` à 49,1 %, `payer_code` à 39,6 % ; 71 518 patients dont **23,45 %** ont plusieurs séjours (jusqu'à 40) → au livrable 3, le découpage train/test se fera **par patient** (`patient_nbr`) pour éviter les fuites.

**Indicateurs Spark SQL** (`indicateurs.py`, 99 343 séjours, 48 lignes écrites dans `/data/indicators/`) :

| Indicateur | Taux de réadmission < 30 jours | Lecture |
|---|---|---|
| Global | **11,39 %** | référence |
| Hospitalisations l'année précédente (`number_inpatient`) | 0 : 8,59 % · 1 : 13,25 % · 2 : 17,93 % · **3+ : 26,40 %** | facteur le plus discriminant (×3 entre 0 et 3+) |
| Durée du séjour | 1–3 j : 9,86 % · 4–7 j : 12,42 % · 8–14 j : **13,85 %** | plus le séjour est long, plus le risque monte |
| Diagnostic principal (`diag_1`) | **Diabète : 13,10 %** · Blessure : 12,40 % · Respiratoire : 10,06 % · Musculo-squelettique : 9,54 % | diabète déséquilibré = séjour le plus à risque |
| Insuline | **Down : 14,22 %** · Up : 13,33 % · Steady : 11,37 % · No : 10,21 % | un ajustement de dose signale un diabète instable |
| HbA1c (`A1Cresult`) | non mesurée : 11,68 % · mesurée : 9,77 – 10,15 % | mesurer l'HbA1c est associé à moins de réadmissions |
| Âge | [20-30) : 14,31 % · [80-90) : 12,57 % · [70-80) : 12,06 % · [40-50) : 10,66 % | sujets âgés plus à risque (le groupe 20-30 ans est petit : 1 649 séjours) |
| Changement de traitement (`change`) | Ch : 12,02 % · No : 10,84 % | effet modéré |
| Sexe / type d'admission | F 11,46 % · H 11,30 % ; types 1 à 6 entre 10,5 et 11,8 % | peu discriminants |

**Action de prévention proposée :** cibler en priorité les patients ayant **au moins 2 hospitalisations dans l'année** ou dont l'insuline a été ajustée pendant le séjour (appel ou consultation de suivi dans les 30 jours), et mesurer systématiquement l'HbA1c avant la sortie.

### 6.4 Fiabilité du démarrage

Au démarrage du cluster, le NameNode HDFS reste quelques secondes en *safe mode* (lecture seule) : un job lancé trop tôt échouait avec `SafeModeException`. `scripts/start_archive.ps1` attend désormais la sortie du safe mode (`hdfs dfsadmin -safemode wait`) avant de soumettre le job d'archivage.

### 6.5 Captures — livrable 2

| | |
|---|---|
| ![](docs/captures/11_l2_preparation_resultat.png) | ![](docs/captures/12_l2_spark_jobs.png) |
| **L2 · Préparation des données** : `preparation.py` sur le cluster, 101 766 → 99 343 séjours, 11,39 % de `label = 1`. | **L2 · Spark master** : `preparation-donnees` FINISHED (36 s) pendant que l'archivage `archive-kafka-to-hdfs` tourne. |
| ![](docs/captures/13_l2_hdfs_clean.png) | ![](docs/captures/14_l2_safemode_erreur.png) |
| **L2 · HDFS – données propres** : `/data/clean/readmission`, Parquet, réplication 2. | **L2 · Avant correction** : l'archivage échouait au démarrage (`SafeModeException`, NameNode en safe mode). |
| ![](docs/captures/15_l2_archive_apres_correction.png) | |
| **L2 · Après correction** : l'archivage redémarre normalement (« Archivage démarré »). | |

| ![](docs/captures/16_l2_qualite_rapport.png) | ![](docs/captures/17_l2_indicateurs_global_age.png) |
|---|---|
| **L2 · Rapport qualité** (`qualite.py`) : 0 doublon, 23,45 % de patients multi-séjours, répartition de la cible. | **L2 · Indicateurs** : taux global (11,39 %) et par tranche d'âge. |
| ![](docs/captures/18_l2_indicateurs_cliniques.png) | ![](docs/captures/19_l2_indicateurs_traitement_diagnostic.png) |
| **L2 · Indicateurs cliniques** : sexe, hospitalisations antérieures, durée de séjour, HbA1c. | **L2 · Indicateurs traitement et diagnostic** : insuline, changement de traitement, type d'admission, diagnostic principal. |

---
### 6.6 Base PostgreSQL de restitution

Les indicateurs calculés par Spark sont stockés dans PostgreSQL pour être interrogés en SQL. Le service `postgres` fait partie du profil `restitution`, qui n'est pas lancé par défaut (commandes de démarrage et de chargement en § 6.2).

**Table `indicateurs`** (créée par `infra/postgres/init.sql`, monté dans `/docker-entrypoint-initdb.d/`) :

| Colonne | Type | Contenu |
|---|---|---|
| `indicateur` | TEXT | nom de l'indicateur (`age`, `number_inpatient`, `insulin`...) |
| `dimension` | TEXT | modalité (`[80-90)`, `3+`, `Steady`...) |
| `nb_sejours` | INT | nombre de séjours |
| `nb_readmis` | INT | nombre de réadmis à moins de 30 jours |
| `taux_readmission` | DOUBLE PRECISION | taux en % |
| `calcule_le` | TIMESTAMP | date de calcul (défaut : `now()`) |

> `init.sql` ne s'exécute automatiquement qu'à la première création du volume `postgres-data`. Pour recréer la table sans perdre les données, rejouer le fichier avec `psql -f` (commande en § 6.2). Pour repartir de zéro : `docker compose --profile restitution down -v`.

**Vérifier la table** :

```powershell
docker exec postgres psql -U readmission -d readmission -c "\d indicateurs"
docker exec -i postgres psql -U readmission -d readmission < infra/postgres/queries.sql
```

`infra/postgres/queries.sql` regroupe trois requêtes de vérification : les 15 dimensions au taux de réadmission le plus élevé, un résumé par indicateur (nombre de dimensions, total de séjours et de réadmis, taux moyen) et le nombre total de lignes exportées avec la date du dernier calcul.

**Capture :**

| |
|---|
| [![](docs/captures/20_l2_postgres_schema.png)](docs/captures/20_l2_postgres_schema.png) |
| **L2 · PostgreSQL – schéma** : structure de la table `indicateurs` (`\d indicateurs`). |
 réadmission, résumé par indicateur et nombre de lignes exportées.
## 7. Livrables 3 et 4 — à venir

| Livrable | Contenu prévu |
|---|---|
| **3** | Lecture de `/data/clean/readmission` ; découpage entraînement / test ; 3 modèles Spark MLlib comparés (dont Random Forest) ; tuning du Random Forest ; matrice de confusion, précision, rappel, F1 ; pipeline sauvegardé et versionné. |
| **4** | Job Structured Streaming qui charge le modèle au démarrage et prédit sur les nouveaux événements Kafka ; écriture des prédictions dans PostgreSQL ; tableau de bord (bonus). |

---

## 8. Documentation

| Document | Contenu |
|---|---|
| [docs/donnees.md](docs/donnees.md) | Provenance, schéma, cible et qualité du dataset |
| [docs/choix_techniques.md](docs/choix_techniques.md) | Justification des choix (Kafka, HDFS, Spark, Parquet, Docker) |
| [docs/runbook.md](docs/runbook.md) | Pannes probables : symptôme, diagnostic, résolution |
| [docs/verification_output_livrable1.md](docs/verification_output_livrable1.md) | Rapport de vérification PySpark des données archivées (L1) |
| [docs/verification_livrable1.txt](docs/verification_livrable1.txt) | Vérification en ligne de commande de la chaîne (L1) |
| [batch/README.md](batch/README.md) | Jobs batch du livrable 2 |

---

## 9. Organisation du dépôt

```
├── docker-compose.yml        # cluster complet, versions fixées
├── .env.example              # paramètres (copier en .env)
├── infra/
│   ├── hadoop/               # core-site.xml, hdfs-site.xml (réplication 2)
│   ├── spark/                # Dockerfile Spark + spark-defaults.conf (connecteur Kafka)
│   ├── prometheus/           # configuration Prometheus (supervision)
│   └── grafana/              # tableau de bord Grafana + provisioning
├── ingestion/                # producteur Kafka, schéma des événements, consumer de la DLQ
├── streaming/                # jobs Structured Streaming (archivage ; inférence au L4)
├── batch/                    # analyse batch : préparation, indicateurs, export PostgreSQL (L2) ; ML (L3)
├── dashboard/                # tableau de bord (L4, bonus)
├── scripts/                  # téléchargement, démos L1 / L2, vérification, healthcheck
├── data/                     # dataset (non versionné)
└── docs/                     # architecture, données, choix techniques, runbook, captures
│   ├── postgres/             # init.sql (table indicateurs), queries.sql (requêtes de vérification)
```

---

## 10. Équipe et contributions

| Étudiant | Livrable 1 | Livrable 2 (en cours) |
|---|---|---|
| BENRBIB Aymen | Cluster Docker et couche d'ingestion (Kafka → HDFS) | Fiabilité du streaming, préparation des données, script de démo |
| AMIRACHE Zaid | Vérification du pipeline PySpark, supervision Prometheus / Grafana, monitoring DLQ | Indicateurs Spark SQL, export vers PostgreSQL |
| ZIANE Marya | Tests de résilience (reprise sur checkpoint Spark, panne d'un DataNode) | Rapport de qualité des données |
| KARAM Ines | Choix techniques, fiabilité de l'ingestion (DLQ, healthcheck, runbook) | Base PostgreSQL, documentation |
