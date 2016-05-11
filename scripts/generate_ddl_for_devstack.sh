#!/usr/bin/env bash

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME

./generate_ddl.sh

cp ddl/pre_transform_specs.sql ../devstack/files/monasca-transform/pre_transform_specs.sql
cp ddl/transform_specs.sql ../devstack/files/monasca-transform/transform_specs.sql

popd