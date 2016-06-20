#!/bin/bash
SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME
pushd ../

JARS_PATH="/opt/spark/current/lib/spark-streaming-kafka.jar,/opt/spark/current/lib/scala-library-2.10.1.jar,/opt/spark/current/lib/kafka_2.10-0.8.1.1.jar,/opt/spark/current/lib/metrics-core-2.2.0.jar,/usr/share/java/mysql.jar"
export SPARK_HOME=/opt/spark/current/
# There is a known issue where obsolete kafka offsets can cause the
# driver to crash.  However when this occurs, the saved offsets get
# deleted such that the next execution should be successful.  Therefore,
# create a loop to run spark-submit for two iterations or until
# control-c is pressed.
COUNTER=0
while [  $COUNTER -lt 2 ]; do
    spark-submit --supervise --master spark://192.168.10.4:7077,192.168.10.5:7077 --conf spark.eventLog.enabled=true --jars $JARS_PATH --py-files dist/$new_filename /opt/monasca/transform/lib/driver.py || break
    let COUNTER=COUNTER+1
done
popd
popd
