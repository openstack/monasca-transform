#!/usr/bin/env bash

rsync -a --exclude='tools/vagrant/.vagrant' /monasca-transform-source /home/vagrant/
mv /home/vagrant/monasca-transform-source /home/vagrant/monasca-transform
pushd /home/vagrant/monasca-transform
# prepare the codebase
#
# generate the sql scripts to populate the database
scripts/generate_ddl.sh
# build the zip
scripts/create_zip.sh

git config --global user.email "local.devstack.committer@hpe.com"
git config --global user.name "Local devstack committer"
git add --all
git commit -m "Local commit"

cd ../monasca-api

git add --all
git commit -m "Local commit"
popd
