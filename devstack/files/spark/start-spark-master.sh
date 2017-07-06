#!/usr/bin/env bash
. /opt/spark/current/conf/spark-env.sh
export EXEC_CLASS=org.apache.spark.deploy.master.Master
export INSTANCE_ID=1
export SPARK_CLASSPATH=/etc/spark/conf/:/opt/spark/current/assembly/target/scala-2.10/jars/*
export log="$SPARK_LOG_DIR/spark-spark-"$EXEC_CLASS"-"$INSTANCE_ID"-127.0.0.1.out"
export SPARK_HOME=/opt/spark/current

# added for spark 2
export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.4-src.zip:${PYTHONPATH}"
export SPARK_SCALA_VERSION="2.10"

/usr/bin/java -cp "$SPARK_CLASSPATH" $SPARK_DAEMON_JAVA_OPTS -Xms1g -Xmx1g "$EXEC_CLASS" --ip "$SPARK_MASTER_IP" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_MASTER_WEBUI_PORT" --properties-file "/etc/spark/conf/spark-defaults.conf"
