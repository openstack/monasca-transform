#!/usr/bin/env bash

MAVEN_STUB="https://repo1.maven.org/maven2"
SPARK_JAVA_LIBS=("org/apache/kafka/kafka_2.10/0.8.1.1/kafka_2.10-0.8.1.1.jar" "org/scala-lang/scala-library/2.10.1/scala-library-2.10.1.jar" "com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar" "org/apache/spark/spark-streaming-kafka_2.10/1.6.0/spark-streaming-kafka_2.10-1.6.0.jar")
for SPARK_JAVA_LIB in "${SPARK_JAVA_LIBS[@]}"
do
   echo Would fetch ${MAVEN_STUB}/${SPARK_JAVA_LIB}
done

for SPARK_JAVA_LIB in "${SPARK_JAVA_LIBS[@]}"
do
    SPARK_LIB_NAME=`echo ${SPARK_JAVA_LIB} | sed 's/.*\///'`
    echo Got lib ${SPARK_LIB_NAME}

done