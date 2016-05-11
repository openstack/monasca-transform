#!/usr/bin/env bash

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME

python create_zip.py

popd