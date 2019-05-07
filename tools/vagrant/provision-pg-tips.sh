#!/usr/bin/env bash
echo Id - `id`

echo Configuring git via https
git config --global url."https://".insteadOf git://

if [ -d devstack ]
then
    echo devstack directory already cloned
else
    git clone https://opendev.org/openstack/devstack
fi

if [ -d monasca-transform ]
then
    echo removing monasca-transform
    sudo rm -rf monasca-transform
fi
