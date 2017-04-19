#!/usr/bin/env bash

echo "going to copy /monasca-transform-source to /home/vagrant ..."
rsync -a --exclude='tools/vagrant/.vagrant' /monasca-transform-source /home/vagrant/

echo "going to move /home/vagrant/monasca-transform-source /home/vagrant/monasca-transform..."
mv /home/vagrant/monasca-transform-source /home/vagrant/monasca-transform
pushd /home/vagrant/monasca-transform

# prepare the codebase
#
# generate the sql scripts which will populate the database
# with the pre_transform and transform specs
scripts/generate_ddl.sh
rc=$?
if [[ $rc == 0 ]]; then
    echo "scripts/generate_ddl.sh completed sucessfully..."
else
    echo "Error in scripts/generate_ddl.sh, bailing out"
    exit 1
fi


# build the zip (to be submitted with spark application)
scripts/create_zip.sh
rc=$?
if [[ $rc == 0 ]]; then
    echo "scripts/create_zip.sh completed sucessfully..."
else
    echo "Error in scripts/create_zip.sh, bailing out"
    exit 1
fi

echo "creating a local commit..."
git config --global user.email "local.devstack.committer@hpe.com"
git config --global user.name "Local devstack committer"
git add --all
git commit -m "Local commit"
echo "creating a local commit done."

CURRENT_BRANCH=`git status | grep 'On branch' | sed 's/On branch //'`
if [ ${CURRENT_BRANCH} != 'master' ]
then
    echo "Maintaining current branch ${CURRENT_BRANCH}"
    # set the branch to what we're using in local.conf
    if [[ -z `grep ${CURRENT_BRANCH} /home/vagrant/devstack/local.conf` ]]; then
        sed -i "s/enable_plugin monasca-transform \/home\/vagrant\/monasca-transform//g" /home/vagrant/devstack/local.conf
        sed -i "s/# END DEVSTACK LOCAL.CONF CONTENTS//g" /home/vagrant/devstack/local.conf
        printf "enable_plugin monasca-transform /home/vagrant/monasca-transform ${CURRENT_BRANCH}\n" >> /home/vagrant/devstack/local.conf
        printf "# END DEVSTACK LOCAL.CONF CONTENTS" >> /home/vagrant/devstack/local.conf
    fi
fi
popd
