#!/usr/bin/env bash

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME

python ddl/generate_ddl.py -t pre_transform_spec -i ddl/pre_transform_specs_template.sql -s ../monasca_transform/data_driven_specs/pre_transform_specs/pre_transform_specs.json -o ddl/pre_transform_specs.sql
python ddl/generate_ddl.py -t transform_spec -i ddl/transform_specs_template.sql -s ../monasca_transform/data_driven_specs/transform_specs/transform_specs.json -o ddl/transform_specs.sql

popd