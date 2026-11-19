CREATE TABLE IF NOT EXISTS indicateurs (
    indicateur TEXT NOT NULL,
    dimension TEXT NOT NULL,
    nb_sejours INT NOT NULL,
    nb_readmis INT NOT NULL,
    taux_readmission DOUBLE PRECISION NOT NULL,
    calcule_le TIMESTAMP DEFAULT now()
);