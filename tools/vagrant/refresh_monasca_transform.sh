#!/usr/bin/env bash

if grep -q pg-tips <<<`hostname`; then
    echo Refreshing monasca-transform
else
    echo Yikes, no - this is not pg-tips!
    exit 1
fi
if [ -d "/home/ubuntu/devstack" ] ; then

. /home/ubuntu/devstack/.stackenv

fi

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME


if grep -q running <<<`sudo service monasca-transform status`; then
    sudo service monasca-transform stop
else
    echo "monasca-transform service not running"
fi

sudo rm -rf /home/ubuntu/monasca-transform-source /home/ubuntu/monasca-transform

sudo ./setup_local_repos.sh

# update the database with configuration
sudo cp /home/ubuntu/monasca-transform/scripts/ddl/pre_transform_specs.sql /opt/monasca/transform/lib/pre_transform_specs.sql
sudo cp /home/ubuntu/monasca-transform/scripts/ddl/transform_specs.sql /opt/monasca/transform/lib/transform_specs.sql
sudo mysql -h "127.0.0.1" -um-transform -ppassword < /opt/monasca/transform/lib/pre_transform_specs.sql
sudo mysql -h "127.0.0.1" -um-transform -ppassword <  /opt/monasca/transform/lib/transform_specs.sql

# update the zip file used for spark submit
sudo cp /home/ubuntu/monasca-transform/scripts/monasca-transform.zip /opt/monasca/transform/lib/.

# update the configuration file
sudo cp /home/ubuntu/monasca-transform/devstack/files/monasca-transform/monasca-transform.conf /etc/.
if [ -n "$SERVICE_HOST" ]; then
    sudo sudo sed -i "s/brokers=192\.168\.15\.6:9092/brokers=${SERVICE_HOST}:9092/g" /etc/monasca-transform.conf
fi

# delete the venv
sudo rm -rf /opt/monasca/transform/venv

# refresh the monasca-transform code to /opt/stack
sudo rm -rf /opt/stack/monasca-transform
pushd /opt/stack
sudo git clone /home/ubuntu/monasca-transform
sudo chown -R monasca-transform:monasca-transform /opt/stack/monasca-transform
sudo su - monasca-transform -c "
        virtualenv /opt/monasca/transform/venv ;
        . /opt/monasca/transform/venv/bin/activate ;
        pip install -e /opt/stack/monasca-transform/ ;
        deactivate"
popd

function get_id () {
    echo `"$@" | grep ' id ' | awk '{print $4}'`
}

source ~/devstack/openrc admin admin
export ADMIN_PROJECT_ID=$(get_id openstack project show admin)
sudo sed -i "s/publish_kafka_project_id=d2cb21079930415a9f2a33588b9f2bb6/publish_kafka_project_id=${ADMIN_PROJECT_ID}/g" /etc/monasca-transform.conf

sudo service monasca-transform start

popd