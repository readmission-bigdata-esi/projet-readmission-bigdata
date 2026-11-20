# Demonstration du livrable 2 : Kafka -> Spark Structured Streaming -> HDFS -> analyse batch
# Usage (depuis la racine du projet) :
#   powershell -ExecutionPolicy Bypass -File scripts\demo_livrable2.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\demo_livrable2.ps1 -Events 20000 -Rate 500
param(
    [int]$Events = 5000,   # nombre d'evenements rejoues par le producteur pendant la demo
    [int]$Rate = 200       # debit du producteur (evenements / seconde)
)
Set-Location (Split-Path $PSScriptRoot -Parent)

function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }

Step "1. Demarrage du cluster (HDFS, Spark, Kafka, PostgreSQL)"
# --build : l'image Spark embarque spark-defaults.conf (driver PostgreSQL) ; reconstruite seulement si elle a change
docker compose --profile restitution up -d --build
if ($LASTEXITCODE -ne 0) { Write-Host "Echec de docker compose up" -ForegroundColor Red; exit 1 }

Step "2. Job d'archivage Kafka -> HDFS (Structured Streaming)"
powershell -ExecutionPolicy Bypass -File scripts\start_archive.ps1

Step "3. Producteur : $Events evenements a $Rate evt/s"
if (-not (Test-Path data\diabetic_data.csv)) {
    Write-Host "Dataset absent : lancez d'abord scripts\download_dataset.ps1" -ForegroundColor Red; exit 1
}
$avant = (docker exec namenode hdfs dfs -count /data/raw/hospital_discharges 2>$null)
$env:MAX_EVENTS = "$Events"
$env:EVENTS_PER_SECOND = "$Rate"
docker compose --profile ingestion up -d --force-recreate producer
$limite = (Get-Date).AddMinutes(10)
do {
    Start-Sleep 5
    $actif = docker inspect -f "{{.State.Running}}" producer 2>$null
} while ($actif -eq "true" -and (Get-Date) -lt $limite)
docker logs --tail 2 producer
Remove-Item Env:MAX_EVENTS, Env:EVENTS_PER_SECOND

Step "4. Attente du dernier micro-batch d'archivage (trigger 20 s)"
Start-Sleep 30
docker exec spark-master bash -c "grep -E '^\[batch' /tmp/archive_raw.log | tail -n 5"
Write-Host "Fichiers dans HDFS (dossiers, fichiers, octets, chemin) :"
Write-Host "  avant : $avant"
Write-Host "  apres : $(docker exec namenode hdfs dfs -count /data/raw/hospital_discharges)"

Step "5. Analyse batch sur le cluster (Spark SQL)"
$jobs = "qualite.py", "preparation.py", "indicateurs.py", "export_postgres.py"
foreach ($job in $jobs) {
    if (Test-Path "batch\$job") {
        Write-Host "`n--- spark-submit batch/$job ---" -ForegroundColor Green
        docker exec spark-master bash -c "/opt/spark/bin/spark-submit /opt/app/batch/$job 2>&1 | grep -vE ' INFO | WARN |^\s+at '"
    } else {
        Write-Host "`n(batch/$job pas encore present : etape ignoree)" -ForegroundColor Yellow
    }
}

Step "6. Resultats"
docker exec namenode hdfs dfs -ls /data/clean /data/indicators 2>$null
Write-Host "`nInterfaces :"
Write-Host "  HDFS  : http://localhost:9870/explorer.html#/data"
Write-Host "  Spark : http://localhost:8080 (applications) et http://localhost:4040 (streaming)"
Write-Host "  Kafka : http://localhost:8090"
