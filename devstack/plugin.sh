#
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

# Determine if we are running in devstack-gate or devstack.
if [[ $DEST ]]; then

    # We are running in devstack-gate.
    export MONASCA_TRANSFORM_BASE=${MONASCA_TRANSFORM_BASE:-"${DEST}"}

else

    # We are running in devstack.
    export MONASCA_TRANSFORM_BASE=${MONASCA_TRANSFORM_BASE:-"/opt/stack"}

fi

function pre_install_monasca_transform {
:
}

function pre_install_spark {
:
}


function install_mysql_connector {

    sudo apt-get -y install libmysql-java

}

function install_java_libs {

    pushd /opt/spark/current/lib
    #MAVEN_STUB="http://uk.maven.org/maven2"
    MAVEN_STUB="https://repo1.maven.org/maven2"
    for SPARK_JAVA_LIB in "${SPARK_JAVA_LIBS[@]}"
    do
        SPARK_LIB_NAME=`echo ${SPARK_JAVA_LIB} | sed 's/.*\///'`

        sudo -u spark curl ${MAVEN_STUB}/${SPARK_JAVA_LIB} -o ${SPARK_LIB_NAME}
    done
    popd
}

function link_spark_streaming_lib {

    pushd /opt/spark/current/lib
    ln -sf spark-streaming-kafka.jar spark-streaming-kafka_2.10-1.6.0.jar
    popd

}

function unstack_monasca_transform {

    echo_summary "Unstack Monasca-transform"
    sudo service monasca-transform stop || true

    delete_monasca_transform_files

    sudo rm /etc/init/monasca-transform.conf || true
    sudo rm -rf /etc/monasca/transform || true

    drop_monasca_transform_database

    unstack_spark

}

function delete_monasca_transform_files {

    sudo rm -rf /opt/monasca/transform || true
    sudo rm /etc/monasca-transform.conf

    MONASCA_TRANSFORM_DIRECTORIES=("/var/log/monasca/transform" "/var/run/monasca/transform" "/etc/monasca/transform/init")

    for MONASCA_TRANSFORM_DIRECTORY in "${MONASCA_TRANSFORM_DIRECTORIES[@]}"
    do
       sudo rm -rf ${MONASCA_TRANSFORM_DIRECTORY} || true
    done

}

function drop_monasca_transform_database {
    # must login as root@localhost
    sudo mysql -h "127.0.0.1" -uroot -psecretmysql < "drop database monasca_transform; drop user 'm-transform'@'%' from mysql.user; drop user 'm-transform'@'localhost' from mysql.user;"  || echo "Failed to drop database 'monasca_transform' and/or user 'm-transform' from mysql database, you may wish to do this manually."

}

function unstack_spark {

    echo_summary "Unstack Spark"

    sudo service spark-master stop || true

    sudo service spark-worker stop || true

    delete_spark_start_scripts
    delete_spark_upstart_definitions
    unlink_spark_commands
    delete_spark_directories
    sudo rm -rf `readlink /opt/spark/current` || true
    sudo rm /opt/spark/current || true
    sudo rm -rf /opt/spark/download || true
    sudo userdel spark || true
    sudo groupdel spark || true

}

function clean_monasca_transform {

    set +o errexit

    unstack_monasca_transform

    clean_monasca_transform

    set -o errexit
}

function create_spark_directories {

    for SPARK_DIRECTORY in "${SPARK_DIRECTORIES[@]}"
    do
       sudo mkdir -p ${SPARK_DIRECTORY}
       sudo chown spark:spark ${SPARK_DIRECTORY}
       sudo chmod 755 ${SPARK_DIRECTORY}
    done

    sudo mkdir -p /var/log/spark-events
    sudo chmod "a+rw" /var/log/spark-events

}

function delete_spark_directories {

    for SPARK_DIRECTORY in "${SPARK_DIRECTORIES[@]}"
        do
           sudo rm -rf ${SPARK_DIRECTORY} || true
        done

    sudo rm -rf /var/log/spark-events || true

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
        sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/spark/"${SPARK_ENV_FILE}" /etc/spark/conf/.
        sudo ln -sf /etc/spark/conf/"${SPARK_ENV_FILE}" /opt/spark/current/conf/"${SPARK_ENV_FILE}"
    done

}

function copy_spark_start_scripts {

    SPARK_START_SCRIPTS=("start-spark-master.sh" "start-spark-worker.sh")
    for SPARK_START_SCRIPT in "${SPARK_START_SCRIPTS[@]}"
    do
        sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/spark/"${SPARK_START_SCRIPT}" /etc/spark/init/.
        sudo chmod 755 /etc/spark/init/"${SPARK_START_SCRIPT}"
    done
}

function delete_spark_start_scripts {

    SPARK_START_SCRIPTS=("start-spark-master.sh" "start-spark-worker.sh")
    for SPARK_START_SCRIPT in "${SPARK_START_SCRIPTS[@]}"
    do
        sudo rm /etc/spark/init/"${SPARK_START_SCRIPT}" || true
    done
}

function copy_spark_upstart_definitions {

    SPARK_UPSTART_DEFINITIONS=("spark-master.conf" "spark-worker.conf")
    for SPARK_UPSTART_DEFINITION in "${SPARK_UPSTART_DEFINITIONS[@]}"
    do
        sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/spark/"${SPARK_UPSTART_DEFINITION}" /etc/init/.
        sudo chmod 644 /etc/init/"${SPARK_UPSTART_DEFINITION}"
    done

}

function delete_spark_upstart_definitions {

    SPARK_UPSTART_DEFINITIONS=("spark-master.conf" "spark-worker.conf")
    for SPARK_UPSTART_DEFINITION in "${SPARK_UPSTART_DEFINITIONS[@]}"
    do
        sudo rm /etc/init/${SPARK_UPSTART_DEFINITION} || true
    done

}


function install_monasca_transform {

    echo_summary "Install Monasca-Transform"

    sudo groupadd --system monasca-transform || true

    sudo useradd --system -g monasca-transform monasca-transform || true

    create_monasca_transform_directories
    copy_monasca_transform_files
    create_monasca_transform_venv

    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/monasca_transform_init.conf /etc/init/monasca-transform.conf
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/start-monasca-transform.sh /etc/monasca/transform/init/.
    sudo chmod +x /etc/monasca/transform/init/start-monasca-transform.sh
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/service_runner.py /etc/monasca/transform/init/.

    create_and_populate_monasca_transform_database

}


function create_monasca_transform_directories {

    MONASCA_TRANSFORM_DIRECTORIES=("/var/log/monasca/transform" "/opt/monasca/transform/lib" "/var/run/monasca/transform" "/etc/monasca/transform/init")

    for MONASCA_TRANSFORM_DIRECTORY in "${MONASCA_TRANSFORM_DIRECTORIES[@]}"
    do
       sudo mkdir -p ${MONASCA_TRANSFORM_DIRECTORY}
       sudo chown monasca-transform:monasca-transform ${MONASCA_TRANSFORM_DIRECTORY}
       sudo chmod 755 ${MONASCA_TRANSFORM_DIRECTORY}
    done

}

function copy_monasca_transform_files {

    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/service_runner.py /opt/monasca/transform/lib/.
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/monasca-transform.conf /etc/.
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/driver.py /opt/monasca/transform/lib/.
    ${MONASCA_TRANSFORM_BASE}/monasca-transform/scripts/create_zip.sh
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/scripts/monasca-transform.zip /opt/monasca/transform/lib/.
    ${MONASCA_TRANSFORM_BASE}/monasca-transform/scripts/generate_ddl_for_devstack.sh
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/monasca-transform_mysql.sql /opt/monasca/transform/lib/.
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/transform_specs.sql /opt/monasca/transform/lib/.
    sudo cp -f "${MONASCA_TRANSFORM_BASE}"/monasca-transform/devstack/files/monasca-transform/pre_transform_specs.sql /opt/monasca/transform/lib/.
    sudo chown -R monasca-transform:monasca-transform /opt/monasca/transform

}

function create_monasca_transform_venv {

    sudo chown -R monasca-transform:monasca-transform /opt/stack/monasca-transform
    sudo su - monasca-transform -c "
        virtualenv /opt/monasca/transform/venv ;
        . /opt/monasca/transform/venv/bin/activate ;
        pip install -e "${MONASCA_TRANSFORM_BASE}"/monasca-transform/ ;
        deactivate"

}

function create_and_populate_monasca_transform_database {
    # must login as root@localhost
    sudo mysql -h "127.0.0.1" -uroot -psecretmysql < /opt/monasca/transform/lib/monasca-transform_mysql.sql || echo "Did the schema change? This process will fail on schema changes."

    sudo mysql -h "127.0.0.1" -um-transform -ppassword < /opt/monasca/transform/lib/pre_transform_specs.sql
    sudo mysql -h "127.0.0.1" -um-transform -ppassword <  /opt/monasca/transform/lib/transform_specs.sql

}

function install_spark {

    echo_summary "Install Spark"

    sudo groupadd --system spark || true

    sudo useradd --system -g spark spark || true

    sudo mkdir -p /opt/spark/download

    sudo chown -R spark:spark /opt/spark

    if [ ! -f /opt/spark/download/${SPARK_TARBALL_NAME} ]
    then
        sudo curl -m 300 http://apache.cs.utah.edu/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL_NAME} -o /opt/spark/download/${SPARK_TARBALL_NAME}
    fi

    sudo chown spark:spark /opt/spark/download/${SPARK_TARBALL_NAME}

    sudo -u spark tar -xzf /opt/spark/download/${SPARK_TARBALL_NAME}  -C /opt/spark/

    sudo -u spark ln -sf /opt/spark/${SPARK_HADOOP_VERSION} /opt/spark/current

    install_mysql_connector

    install_java_libs

    create_spark_directories

    link_spark_commands_to_usr_bin

    copy_and_link_config

    copy_spark_start_scripts

    copy_spark_upstart_definitions

}

function extra_spark {

    sudo service spark-master start
    sleep 10
    sudo service spark-worker start

}

function post_config_monasca_transform {
:
}

function extra_monasca_transform {

    sudo service monasca-transform start

}

# check for service enabled

echo_summary "Monasca-transform plugin with service enabled = `is_service_enabled monasca-transform`"

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
    fi

    if [[ "$1" == "clean" ]]; then
        # Remove state and transient data
        # Remember clean.sh first calls unstack.sh
        echo_summary "Cleaning Monasca-transform"
        clean_monasca_transform
    fi

    else
      echo_summary "Monasca-transform not enabled"
fi

#Restore errexit
$ERREXIT

# Restore xtrace
$XTRACE
