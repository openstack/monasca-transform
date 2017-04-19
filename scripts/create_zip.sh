#!/usr/bin/env bash

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME

echo "create_zip.py: creating a zip file at ../monasca_transform/monasca-transform.zip..."
python create_zip.py
rc=$?
if [[ $rc == 0 ]]; then
    echo "created zip file at ../monasca_transfom/monasca-transform.zip sucessfully"
else
    echo "error creating zip file at ../monasca_transform/monasca-transform.zip, bailing out"
    exit 1
fi
popd
