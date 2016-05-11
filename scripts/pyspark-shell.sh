#!/bin/bash
JARS_PATH="/opt/spark/current/lib/spark-streaming-kafka.jar,/opt/spark/current/lib/scala-library-2.10.1.jar,/opt/spark/current/lib/kafka_2.10-0.8.1.1.jar,/opt/spark/current/lib/metrics-core-2.2.0.jar"
pyspark --master spark://192.168.10.4:7077 --jars $JARS_PATH
