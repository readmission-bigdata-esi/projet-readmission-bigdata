-- Table des indicateurs du livrable 2 (remplie par batch/export_postgres.py).
-- Exécutée automatiquement à la première création du volume postgres-data ;
-- relançable sans risque : docker exec postgres psql -U readmission -d readmission -f /docker-entrypoint-initdb.d/init.sql
CREATE TABLE IF NOT EXISTS indicateurs (
    indicateur TEXT NOT NULL,
    dimension TEXT NOT NULL,
    nb_sejours INT NOT NULL,
    nb_readmis INT NOT NULL,
    taux_readmission DOUBLE PRECISION NOT NULL,
    calcule_le TIMESTAMP DEFAULT now()
);
