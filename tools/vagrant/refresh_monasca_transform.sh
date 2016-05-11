#!/usr/bin/env bash

if grep -q pg-tips <<<`hostname`; then
    echo Refreshing monasca-transform
else
    echo Yikes, no - this is not pg-tips!
    exit 1
fi

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME


if grep -q running <<<`sudo service monasca-transform status`; then
    sudo service monasca-transform stop
else
    echo "monasca-transform service not running"
fi

sudo rm -rf /home/vagrant/monasca-transform-source /home/vagrant/monasca-transform

sudo ./setup_local_repos.sh

# update the database with configuration
sudo cp /home/vagrant/monasca-transform/scripts/ddl/pre_transform_specs.sql /opt/monasca/transform/lib/pre_transform_specs.sql
sudo cp /home/vagrant/monasca-transform/scripts/ddl/transform_specs.sql /opt/monasca/transform/lib/transform_specs.sql
sudo mysql -h "127.0.0.1" -um-transform -ppassword < /opt/monasca/transform/lib/pre_transform_specs.sql
sudo mysql -h "127.0.0.1" -um-transform -ppassword <  /opt/monasca/transform/lib/transform_specs.sql

# update the zip file used for spark submit
sudo cp /home/vagrant/monasca-transform/scripts/monasca-transform.zip /opt/monasca/transform/lib/.

# update the configuration file
sudo cp /home/vagrant/monasca-transform/devstack/files/monasca-transform/monasca-transform.conf /etc/.

# delete the venv
sudo rm -rf /opt/monasca/transform/venv

# refresh the monasca-transform code to /opt/stack
sudo rm -rf /opt/stack/monasca-transform
pushd /opt/stack
sudo git clone /home/vagrant/monasca-transform
sudo chown -R monasca-transform:monasca-transform /opt/stack/monasca-transform
sudo su - monasca-transform -c "
        virtualenv /opt/monasca/transform/venv ;
        . /opt/monasca/transform/venv/bin/activate ;
        pip install -e /opt/stack/monasca-transform/ ;
        deactivate"
popd

sudo service monasca-transform start

popd