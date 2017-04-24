#!/usr/bin/env bash

SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME

PRE_TRANSFORM_SPECS_JSON="../monasca_transform/data_driven_specs/pre_transform_specs/pre_transform_specs.json"
PRE_TRANSFORM_SPECS_SQL="ddl/pre_transform_specs.sql"

TRANSFORM_SPECS_JSON="../monasca_transform/data_driven_specs/transform_specs/transform_specs.json"
TRANSFORM_SPECS_SQL="ddl/transform_specs.sql"

echo "converting {$PRE_TRANSFORM_SPECS_JSON} to {$PRE_TRANSFORM_SPECS_SQL} ..."
python ddl/generate_ddl.py -t pre_transform_spec -i ddl/pre_transform_specs_template.sql -s "$PRE_TRANSFORM_SPECS_JSON" -o "$PRE_TRANSFORM_SPECS_SQL"
rc=$?
if [[ $rc == 0 ]]; then
    echo "converting {$PRE_TRANSFORM_SPECS_JSON} to {$PRE_TRANSFORM_SPECS_SQL} sucessfully..."
else
    echo "error in converting {$PRE_TRANSFORM_SPECS_JSON} to {$PRE_TRANSFORM_SPECS_SQL}, bailing out"
    exit 1
fi

echo "converting {$TRANSFORM_SPECS_JSON} to {$TRANSFORM_SPECS_SQL}..."
python ddl/generate_ddl.py -t transform_spec -i ddl/transform_specs_template.sql -s "$TRANSFORM_SPECS_JSON" -o "$TRANSFORM_SPECS_SQL"
rc=$?
if [[ $rc == 0 ]]; then
    echo "converting {$TRANSFORM_SPECS_JSON} to {$TRANSFORM_SPECS_SQL} sucessfully..."
else
    echo "error in converting {$TRANSFORM_SPECS_JSON} to {$TRANSFORM_SPECS_SQL}, bailing out"
    exit 1
fi
popd
