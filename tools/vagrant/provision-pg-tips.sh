#!/usr/bin/env bash
echo Id - `id`

echo Configuring git via https
git config --global url."https://".insteadOf git://

if [ -d devstack ]
then
    echo devstack directory already cloned
else
    git clone https://git.openstack.org/openstack-dev/devstack
fi

if [ -d monasca-api ]
then
    echo monasca-api directory already cloned
else
    git clone https://github.com/openstack/monasca-api.git
fi

if [ -d monasca-transform ]
then
    echo removing monasca-transform
    sudo rm -rf monasca-transform
fi
