#!/bin/bash -xe

# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# This script is executed inside post_test_hook function in devstack gate

function generate_testr_results {
    if [ -f .testrepository/0 ]; then
        sudo .tox/functional/bin/testr last --subunit > $WORKSPACE/testrepository.subunit
        sudo mv $WORKSPACE/testrepository.subunit $BASE/logs/testrepository.subunit
        sudo /usr/os-testr-env/bin/subunit2html $BASE/logs/testrepository.subunit $BASE/logs/testr_results.html
        sudo gzip -9 $BASE/logs/testrepository.subunit
        sudo gzip -9 $BASE/logs/testr_results.html
        sudo chown $USER:$USER $BASE/logs/testrepository.subunit.gz $BASE/logs/testr_results.html.gz
        sudo chmod a+r $BASE/logs/testrepository.subunit.gz $BASE/logs/testr_results.html.gz
    fi
}

export MONASCA_TRANSFORM_DIR="$BASE/new/monasca-transform"

export MONASCA_TRANSFORM_LOG_DIR="/var/log/monasca/transform/"

# Go to the monasca-transform dir
cd $MONASCA_TRANSFORM_DIR

if [[ -z "$STACK_USER" ]]; then
    export STACK_USER=stack
fi

sudo chown -R $STACK_USER:stack $MONASCA_TRANSFORM_DIR

# create a log dir
sudo mkdir -p $MONASCA_TRANSFORM_LOG_DIR
sudo chown -R $STACK_USER:stack $MONASCA_TRANSFORM_LOG_DIR

# Run tests
echo "Running monasca-transform functional test suite"
set +e


sudo -E -H -u ${STACK_USER:-${USER}} tox -efunctional
EXIT_CODE=$?
set -e

# Collect and parse result
generate_testr_results
exit $EXIT_CODE
