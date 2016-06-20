#!/usr/bin/env bash

export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_IP=127.0.0.1
export SPARK_MASTER_PORT=7077
export SPARK_MASTERS=127.0.0.1:7077
export SPARK_MASTER_WEBUI_PORT=18080

export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=18081
export SPARK_WORKER_DIR=/var/run/spark/work

export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_CORES=2

export SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS -Dspark.history.fs.logDirectory=file://var/log/spark/events -Dspark.history.ui.port=18082"
export SPARK_LOG_DIR=/var/log/spark
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=127.0.0.1:2181 -Dspark.deploy.zookeeper.dir=/var/run/spark"
