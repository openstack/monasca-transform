#!/usr/bin/env bash

#
# for debugging
#

## turn on display command
## set -x

if grep -q devstack <<<`hostname`; then
    echo Refreshing monasca-transform
else
    echo Yikes, no - this is not devstack!
    exit 1
fi

if [ -d ~/devstack ] ; then

. ~/devstack/.stackenv

fi

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME


STOP_SLEEP=10
if [[ $USE_SCREEN == "True" ]]; then
    # stop monasca-transform process running in screen session
    if [[ -r /opt/stack/status/stack/monasca-transform.pid ]]; then
        echo "going to shutdown monasca-transform running in screen session..."
        pkill -g $(cat /opt/stack/status/stack/monasca-transform.pid)
        rc=$?
        if [[ $rc == 0 ]]; then
            echo "waiting $STOP_SLEEP seconds for monasca-transform to exit..."
            sleep $STOP_SLEEP
            screen -S stack -p monasca-transform -X stuff "\015"
            echo "monasca-transform process stopped sucessfully"
        else
            echo "monasca-transform process could not be stopped, proceeding"
        fi
    else
        echo "monasca-transform process wasnt running in screen session, proceeding"
    fi
else
    echo "going to shutdown devstack@monasca-transform service..."
    sudo systemctl stop devstack@monasca-transform
    rc=$?
    if [[ $rc == 0 ]]; then
        echo "waiting $STOP_SLEEP seconds for devstack@monasca-transform service to exit..."
        sleep $STOP_SLEEP
        echo "devstack@monasca-transform stopped successfully"
    else
        echo "devstack@monasca-transform service could not be stopped, proceeding"
    fi
fi

#
# turn on exit immediately if command fails
#
set -e

sudo rm -rf /home/vagrant/monasca-transform-source /home/vagrant/monasca-transform

echo "calling setup_local_repos.sh ..."
sudo ./setup_local_repos.sh
rc=$?
if [[ $rc == 0 ]]; then
    echo "setup_local_repos.sh completed sucessfully..."
else
    echo "Error in setup_local_repos.sh, bailing out"
    exit 1
fi

# update the database with configuration
echo "populating monasca-transform db tables with pre_transform_specs and transform_specs..."
sudo cp /home/vagrant/monasca-transform/scripts/ddl/pre_transform_specs.sql /opt/monasca/transform/lib/pre_transform_specs.sql
sudo cp /home/vagrant/monasca-transform/scripts/ddl/transform_specs.sql /opt/monasca/transform/lib/transform_specs.sql
sudo mysql -h "127.0.0.1" -um-transform -ppassword < /opt/monasca/transform/lib/pre_transform_specs.sql
sudo mysql -h "127.0.0.1" -um-transform -ppassword <  /opt/monasca/transform/lib/transform_specs.sql
echo "populating monasca-transform db tables with pre_transform_specs and transform_specs done."

# update the zip file used for spark submit
echo "copying new monasca-transform.zip to /opt/monasca/transform/lib/ ..."
sudo cp /home/vagrant/monasca-transform/scripts/monasca-transform.zip /opt/monasca/transform/lib/.
echo "copying new monasca-transform.zip to /opt/monasca/transform/lib/ done."

# update the configuration file
sudo cp /home/vagrant/monasca-transform/devstack/files/monasca-transform/monasca-transform.conf /etc/.
if [ -n "$SERVICE_HOST" ]; then
    sudo sudo sed -i "s/brokers=192\.168\.15\.6:9092/brokers=${SERVICE_HOST}:9092/g" /etc/monasca-transform.conf
fi


MONASCA_TRANSFORM_VENV="/opt/monasca/transform/venv"
MONASCA_TRANSFORM_DIR="/opt/stack/monasca-transform"

echo "refreshing $MONASCA_TRANSFORM_DIR..."

# turn on display command
set -x

# delete the venv
sudo rm -rf $MONASCA_TRANSFORM_VENV
# refresh the monasca-transform code at /opt/stack
sudo rm -rf "$MONASCA_TRANSFORM_DIR"
pushd /opt/stack
sudo git clone /home/vagrant/monasca-transform
sudo chown -R vagrant:vagrant "$MONASCA_TRANSFORM_DIR"
virtualenv "$MONASCA_TRANSFORM_VENV"
. "$MONASCA_TRANSFORM_VENV"/bin/activate
pip install -e "$MONASCA_TRANSFORM_DIR"
deactivate

# turn off display command
set +x

echo "refreshing $MONASCA_TRANSFORM_DIR done."
popd

function get_id () {
    echo `"$@" | grep ' id ' | awk '{print $4}'`
}

source ~/devstack/openrc admin admin
export ADMIN_PROJECT_ID=$(get_id openstack project show admin)
echo "updating publish_kafka_project_id to $ADMIN_PROJECT_ID in /etc/monasca-transform.conf..."
sudo sed -i "s/publish_kafka_project_id=d2cb21079930415a9f2a33588b9f2bb6/publish_kafka_project_id=${ADMIN_PROJECT_ID}/g" /etc/monasca-transform.conf
echo "updating publish_kafka_project_id to $ADMIN_PROJECT_ID in /etc/monasca-transform.conf done."


if [[ $USE_SCREEN == "True" ]]; then
    # start monasca-transform in screen session
    start_command="/etc/monasca/transform/init/start-monasca-transform.sh"
    screen -S stack -p monasca-transform -X stuff "$start_command & echo \$! >/opt/stack/status/stack/monasca-transform.pid; fg || echo \"monasca-transform failed to start\""
    screen -S stack -p monasca-transform -X stuff "\015"
    rc=$?
    if [[ $rc == 0 ]]; then
        echo "monasca-transform process started sucessfully"
    else
        echo "Error: monasca-transform process was not started. Please check screen session for error messages"
        exit 1
    fi
else
    echo "going to start devstack@monasca-transform service ..."
    sudo systemctl start devstack@monasca-transform
    rc=$?
    if [[ $rc == 0 ]]; then
        echo "devstack@monasca-transform service started sucessfully"
    else
        echo "Error: devstack@monasca-transform was not started. Please check /var/log/monasca/transform/monasca-transform.log for errors"
        exit 1
    fi
fi
echo "***********************************************"
echo "*                                             *"
echo "* SUCCESS!! refresh monasca transform done.   *"
echo "*                                             *"
echo "***********************************************"

popd
