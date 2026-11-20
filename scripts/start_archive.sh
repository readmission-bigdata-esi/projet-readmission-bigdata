#!/usr/bin/env bash
# Lance le job Spark Structured Streaming Kafka -> HDFS sur le cluster (en arriere-plan)
# Au demarrage, le NameNode reste quelques secondes en "safe mode" (lecture seule) :
# un job lance trop tot echoue (SafeModeException). On attend sa sortie.
echo "Attente de la sortie du safe mode HDFS..."
docker exec namenode hdfs dfsadmin -safemode wait >/dev/null
docker exec -d spark-master bash -c "/opt/spark/bin/spark-submit --name archive-kafka-to-hdfs /opt/app/streaming/archive_to_hdfs.py > /tmp/archive_raw.log 2>&1"
echo "Job soumis. Journal : docker exec spark-master tail -f /tmp/archive_raw.log"
