-- Les 15 dimensions avec le taux de réadmission le plus élevé
SELECT
    indicateur,
    dimension,
    nb_sejours,
    nb_readmis,
    taux_readmission,
    calcule_le
FROM indicateurs
ORDER BY taux_readmission DESC
LIMIT 15;

-- Résumé par indicateur
SELECT
    indicateur,
    COUNT(*) AS nombre_dimensions,
    SUM(nb_sejours) AS total_sejours,
    SUM(nb_readmis) AS total_readmis,
    ROUND(AVG(taux_readmission)::numeric, 4) AS taux_moyen
FROM indicateurs
GROUP BY indicateur
ORDER BY taux_moyen DESC;

-- Vérifier le nombre total de lignes exportées
SELECT
    COUNT(*) AS nombre_total_indicateurs,
    MAX(calcule_le) AS dernier_calcul
FROM indicateurs;