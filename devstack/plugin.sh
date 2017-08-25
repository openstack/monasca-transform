# (C) Copyright 2015 Hewlett Packard Enterprise Development Company LP
# Copyright 2016 FUJITSU LIMITED
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Monasca-transform DevStack plugin
#
# Install and start Monasca-transform service in devstack
#
# To enable Monasca-transform in devstack add an entry to local.conf that
# looks like
#
# [[local|localrc]]
# enable_plugin monasca-transform https://git.openstack.org/openstack/monasca-transform
#
# By default all Monasca services are started (see
# devstack/settings). To disable a specific service use the
# disable_service function. For example to turn off notification:
#
# disable_service monasca-notification
#
# Several variables set in the localrc section adjust common behaviors
# of Monasca (see within for additional settings):
#
# EXAMPLE VARS HERE

# Save trace setting
XTRACE=$(set +o | grep xtrace)
set -o xtrace

ERREXIT=$(set +o | grep errexit)
set -o errexit

# monasca-transform database password
export MONASCA_TRANSFORM_DB_PASSWORD=${MONASCA_TRANSFORM_DB_PASSWORD:-"password"}

export MONASCA_TRANSFORM_FILES="${DEST}"/monasca-transform/devstack/files
export DOWNLOADS_DIRECTORY=${DOWNLOADS_DIRECTORY:-"/home/${USER}/downloads"}

function pre_install_monasca_transform {
:
}

function pre_install_spark {
    for SPARK_JAVA_LIB in "${SPARK_JAVA_LIBS[@]}"
    do
        SPARK_LIB_NAME=`echo ${SPARK_JAVA_LIB} | sed 's/.*\///'`
        download_through_cache ${MAVEN_REPO}/${SPARK_JAVA_LIB} ${SPARK_LIB_NAME}
    done

    for SPARK_JAR in "${SPARK_JARS[@]}"
    do
        SPARK_JAR_NAME=`echo ${SPARK_JAR} | sed 's/.*\///'`
        download_through_cache ${MAVEN_REPO}/${SPARK_JAR} ${SPARK_JAR_NAME}
    done

    download_through_cache ${APACHE_MIRROR}/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL_NAME} ${SPARK_TARBALL_NAME} 1000


}

function install_java_libs {

    pushd /opt/spark/current/assembly/target/scala-2.10/jars/
    for SPARK_JAVA_LIB in "${SPARK_JAVA_LIBS[@]}"
    do
        SPARK_LIB_NAME=`echo ${SPARK_JAVA_LIB} | sed 's/.*\///'`
        copy_from_cache ${SPARK_LIB_NAME}
    done
    popd
}

function install_spark_jars {

    # create a directory for jars
    mkdir -p /opt/spark/current/assembly/target/scala-2.10/jars

    # copy jars to new location
    pushd /opt/spark/current/assembly/target/scala-2.10/jars
    for SPARK_JAR in "${SPARK_JARS[@]}"
    do
        SPARK_JAR_NAME=`echo ${SPARK_JAR} | sed 's/.*\///'`
        copy_from_cache ${SPARK_JAR_NAME}
    done

    # copy all jars except spark and scala to assembly/target/scala_2.10/jars
    find /opt/spark/current/jars/ -type f ! \( -iname 'spark*' -o -iname 'scala*' -o -iname 'jackson-module-scala*' -o -iname 'json4s-*' -o -iname 'breeze*' -o -iname 'spire*' -o -iname 'macro-compat*' -o -iname 'shapeless*' -o -iname 'machinist*' -o -iname 'chill*' \) -exec cp {} . \;

    # rename jars directory
    mv /opt/spark/current/jars/ /opt/spark/current/jars_original
    popd
}

function copy_from_cache {
    resource_name=$1
    target_directory=${2:-"./."}
    cp ${DOWNLOADS_DIRECTORY}/${resource_name} ${target_directory}/.
}

function download_through_cache {
    resource_location=$1
    resource_name=$2
    resource_timeout=${3:-"300"}
    if [[ ! -d ${DOWNLOADS_DIRECTORY} ]]; then
        _safe_permission_operation mkdir -p ${DOWNLOADS_DIRECTORY}
        _safe_permission_operation chown ${USER} ${DOWNLOADS_DIRECTORY}
    fi
    pushd ${DOWNLOADS_DIRECTORY}
    if [[ ! -f ${resource_name} ]]; then
        curl -m ${resource_timeout} --retry 3 --retry-delay 5 ${resource_location} -o ${resource_name}
    fi
    popd
}

function unstack_monasca_transform {

    echo_summary "Unstack Monasca-transform"
    stop_process "monasca-transform" || true

}

function delete_monasca_transform_files {

    sudo rm -rf /opt/monasca/transform || true
    sudo rm /etc/monasca-transform.conf || true

    MONASCA_TRANSFORM_DIRECTORIES=("/var/log/monasca/transform" "/var/run/monasca/transform" "/etc/monasca/transform/init")

    for MONASCA_TRANSFORM_DIRECTORY in "${MONASCA_TRANSFORM_DIRECTORIES[@]}"
    do
       sudo rm -rf ${MONASCA_TRANSFORM_DIRECTORY} || true
    done

}

function drop_monasca_transform_database {
    sudo mysql -u$DATABASE_USER -p$DATABASE_PASSWORD -h$MYSQL_HOST -e "drop database monasca_transform; drop user 'm-transform'@'%' from mysql.user; drop user 'm-transform'@'localhost' from mysql.user;"  || echo "Failed to drop database 'monasca_transform' and/or user 'm-transform' from mysql database, you may wish to do this manually."
}

function unstack_spark {

    echo_summary "Unstack Spark"

    stop_spark_worker

    stop_spark_master

}

function stop_spark_worker {

    stop_process "spark-worker"

}

function stop_spark_master {

    stop_process "spark-master"

}

function clean_spark {
    echo_summary "Clean spark"
    set +o errexit
    delete_spark_start_scripts
    delete_spark_upstart_definitions
    unlink_spark_commands
    delete_spark_directories
    sudo rm -rf `readlink /opt/spark/current` || true
    sudo rm -rf /opt/spark || true
    sudo userdel spark || true
    sudo groupdel spark || true
    set -o errexit
}

function clean_monasca_transform {
    set +o errexit
    delete_monasca_transform_files
    sudo rm /etc/init/monasca-transform.conf || true
    sudo rm -rf /etc/monasca/transform || true
    drop_monasca_transform_database
    set -o errexit
}

function create_spark_directories {

    for SPARK_DIRECTORY in "${SPARK_DIRECTORIES[@]}"
    do
       sudo mkdir -p ${SPARK_DIRECTORY}
       sudo chown ${USER} ${SPARK_DIRECTORY}
       sudo chmod 755 ${SPARK_DIRECTORY}
    done


}

function delete_spark_directories {

    for SPARK_DIRECTORY in "${SPARK_DIRECTORIES[@]}"
        do
           sudo rm -rf ${SPARK_DIRECTORY} || true
        done

}


function link_spark_commands_to_usr_bin {

    SPARK_COMMANDS=("spark-submit" "spark-class" "spark-shell" "spark-sql")
    for SPARK_COMMAND in "${SPARK_COMMANDS[@]}"
    do
        sudo ln -sf /opt/spark/current/bin/${SPARK_COMMAND} /usr/bin/${SPARK_COMMAND}
    done

}

function unlink_spark_commands {

    SPARK_COMMANDS=("spark-submit" "spark-class" "spark-shell" "spark-sql")
    for SPARK_COMMAND in "${SPARK_COMMANDS[@]}"
    do
        sudo unlink /usr/bin/${SPARK_COMMAND} || true
    done

}

function copy_and_link_config {

    SPARK_ENV_FILES=("spark-env.sh" "spark-worker-env.sh" "spark-defaults.conf")
    for SPARK_ENV_FILE in "${SPARK_ENV_FILES[@]}"
    do
        cp -f "${MONASCA_TRANSFORM_FILES}"/spark/"${SPARK_ENV_FILE}" /etc/spark/conf/.
        ln -sf /etc/spark/conf/"${SPARK_ENV_FILE}" /opt/spark/current/conf/"${SPARK_ENV_FILE}"
    done

}

function copy_spark_start_scripts {

    SPARK_START_SCRIPTS=("start-spark-master.sh" "start-spark-worker.sh")
    for SPARK_START_SCRIPT in "${SPARK_START_SCRIPTS[@]}"
    do
        cp -f "${MONASCA_TRANSFORM_FILES}"/spark/"${SPARK_START_SCRIPT}" /etc/spark/init/.
        chmod 755 /etc/spark/init/"${SPARK_START_SCRIPT}"
    done
}

function delete_spark_start_scripts {

    SPARK_START_SCRIPTS=("start-spark-master.sh" "start-spark-worker.sh")
    for SPARK_START_SCRIPT in "${SPARK_START_SCRIPTS[@]}"
    do
        rm /etc/spark/init/"${SPARK_START_SCRIPT}" || true
    done
}


function install_monasca_transform {

    echo_summary "Install Monasca-Transform"

    create_monasca_transform_directories
    copy_monasca_transform_files
    create_monasca_transform_venv

    sudo cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/monasca-transform.service /etc/systemd/system/.
    sudo cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/start-monasca-transform.sh /etc/monasca/transform/init/.
    sudo chmod +x /etc/monasca/transform/init/start-monasca-transform.sh
    sudo cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/service_runner.py /etc/monasca/transform/init/.

}


function create_monasca_transform_directories {

    MONASCA_TRANSFORM_DIRECTORIES=("/var/log/monasca/transform" "/opt/monasca/transform" "/opt/monasca/transform/lib" "/var/run/monasca/transform" "/etc/monasca/transform/init")

    for MONASCA_TRANSFORM_DIRECTORY in "${MONASCA_TRANSFORM_DIRECTORIES[@]}"
    do
       sudo mkdir -p ${MONASCA_TRANSFORM_DIRECTORY}
       sudo chown ${USER} ${MONASCA_TRANSFORM_DIRECTORY}
       chmod 755 ${MONASCA_TRANSFORM_DIRECTORY}
    done

}

function get_id () {
    echo `"$@" | grep ' id ' | awk '{print $4}'`
}

function ascertain_admin_project_id {

    source ~/devstack/openrc admin admin
    export ADMIN_PROJECT_ID=$(get_id openstack project show admin)

}

function copy_monasca_transform_files {

    cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/service_runner.py /opt/monasca/transform/lib/.
    sudo cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/monasca-transform.conf /etc/.
    cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/driver.py /opt/monasca/transform/lib/.
    ${DEST}/monasca-transform/scripts/create_zip.sh
    cp -f "${DEST}"/monasca-transform/scripts/monasca-transform.zip /opt/monasca/transform/lib/.
    ${DEST}/monasca-transform/scripts/generate_ddl_for_devstack.sh
    cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/monasca-transform_mysql.sql /opt/monasca/transform/lib/.
    cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/transform_specs.sql /opt/monasca/transform/lib/.
    cp -f "${MONASCA_TRANSFORM_FILES}"/monasca-transform/pre_transform_specs.sql /opt/monasca/transform/lib/.
    touch /var/log/monasca/transform/monasca-transform.log
    # set passwords and other variables in configuration files
    sudo sudo sed -i "s/brokers=192\.168\.15\.6:9092/brokers=${SERVICE_HOST}:9092/g" /etc/monasca-transform.conf
    sudo sudo sed -i "s/password\s=\spassword/password = ${MONASCA_TRANSFORM_DB_PASSWORD}/g" /etc/monasca-transform.conf
}

function create_monasca_transform_venv {

    sudo chown -R ${USER} ${DEST}/monasca-transform
    virtualenv /opt/monasca/transform/venv ;
    . /opt/monasca/transform/venv/bin/activate ;
    pip install -e "${DEST}"/monasca-transform/ ;
    deactivate

}

function create_and_populate_monasca_transform_database {
    # must login as root@localhost
    mysql -u$DATABASE_USER -p$DATABASE_PASSWORD -h$MYSQL_HOST < /opt/monasca/transform/lib/monasca-transform_mysql.sql || echo "Did the schema change? This process will fail on schema changes."

    # set grants for m-transform user (needs to be done from localhost)
    mysql -u$DATABASE_USER -p$DATABASE_PASSWORD -h$MYSQL_HOST -e "GRANT ALL ON monasca_transform.* TO 'm-transform'@'%' IDENTIFIED BY '${MONASCA_TRANSFORM_DB_PASSWORD}';"
    mysql -u$DATABASE_USER -p$DATABASE_PASSWORD -h$MYSQL_HOST -e "GRANT ALL ON monasca_transform.* TO 'm-transform'@'localhost' IDENTIFIED BY '${MONASCA_TRANSFORM_DB_PASSWORD}';"

    # copy rest of files after grants are ready
    mysql -um-transform -p$MONASCA_TRANSFORM_DB_PASSWORD -h$MYSQL_HOST < /opt/monasca/transform/lib/pre_transform_specs.sql
    mysql -um-transform -p$MONASCA_TRANSFORM_DB_PASSWORD -h$MYSQL_HOST <  /opt/monasca/transform/lib/transform_specs.sql
}

function install_spark {

    echo_summary "Install Spark"

    sudo mkdir /opt/spark || true

    sudo chown -R ${USER} /opt/spark

    tar -xzf ${DOWNLOADS_DIRECTORY}/${SPARK_TARBALL_NAME} -C /opt/spark/

    ln -sf /opt/spark/${SPARK_HADOOP_VERSION} /opt/spark/current

    install_spark_jars

    install_java_libs

    create_spark_directories

    link_spark_commands_to_usr_bin

    copy_and_link_config

    copy_spark_start_scripts

}

function extra_spark {

    start_spark_master
    start_spark_worker

}

function start_spark_worker {

    run_process "spark-worker" "/etc/spark/init/start-spark-worker.sh"

}

function start_spark_master {

    run_process "spark-master" "/etc/spark/init/start-spark-master.sh"

}

function post_config_monasca_transform {

    create_and_populate_monasca_transform_database

}

function post_config_spark {
:
}

function extra_monasca_transform {

    /opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 64 --topic metrics_pre_hourly

    ascertain_admin_project_id
    sudo sed -i "s/publish_kafka_project_id=d2cb21079930415a9f2a33588b9f2bb6/publish_kafka_project_id=${ADMIN_PROJECT_ID}/g" /etc/monasca-transform.conf
    start_monasca_transform

}

function start_monasca_transform {
    run_process "monasca-transform" "/etc/monasca/transform/init/start-monasca-transform.sh"
}

# check for service enabled
if is_service_enabled monasca-transform; then

    if [[ "$1" == "stack" && "$2" == "pre-install" ]]; then
        # Set up system services
        echo_summary "Configuring Spark system services"
        pre_install_spark
        echo_summary "Configuring Monasca-transform system services"
        pre_install_monasca_transform

    elif [[ "$1" == "stack" && "$2" == "install" ]]; then
        # Perform installation of service source
        echo_summary "Installing Spark"
        install_spark
        echo_summary "Installing Monasca-transform"
        install_monasca_transform

    elif [[ "$1" == "stack" && "$2" == "post-config" ]]; then
        # Configure after the other layer 1 and 2 services have been configured
        echo_summary "Configuring Spark"
        post_config_spark
        echo_summary "Configuring Monasca-transform"
        post_config_monasca_transform

    elif [[ "$1" == "stack" && "$2" == "extra" ]]; then
        # Initialize and start the Monasca service
        echo_summary "Initializing Spark"
        extra_spark
        echo_summary "Initializing Monasca-transform"
        extra_monasca_transform
    fi

    if [[ "$1" == "unstack" ]]; then
        echo_summary "Unstacking Monasca-transform"
        unstack_monasca_transform
        echo_summary "Unstacking Spark"
        unstack_spark
    fi

    if [[ "$1" == "clean" ]]; then
        # Remove state and transient data
        # Remember clean.sh first calls unstack.sh
        echo_summary "Cleaning Monasca-transform"
        clean_monasca_transform
        echo_summary "Cleaning Spark"
        clean_spark
    fi

    else
      echo_summary "Monasca-transform not enabled"
fi

#Restore errexit
$ERREXIT

# Restore xtrace
$XTRACE
