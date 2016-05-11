#!/usr/bin/env bash


SCRIPT_HOME=$(dirname $(readlink -f $BASH_SOURCE))
pushd $SCRIPT_HOME
pushd ../
rm -rf build monasca-transform.egg-info dist
python setup.py bdist_egg

found_egg=`ls dist`
echo 
echo
echo Created egg file at dist/$found_egg
dev=dev
find_dev_index=`expr index $found_egg $dev`
new_filename=${found_egg:0:$find_dev_index - 1 }egg
echo Copying dist/$found_egg to dist/$new_filename
cp dist/$found_egg dist/$new_filename
popd
popd