# Monasca-transform DevStack Plugin

The Monasca-transform DevStack plugin is tested only on Ubuntu 16.04 (Xenial).

A short cut to running monasca-transform in devstack is implemented with vagrant.

## Variables
* DATABASE_PASSWORD(default: *secretmysql*) - password to upload monasca-transform schema
* MONASCA_TRANSFORM_DB_PASSWORD(default: *password*) - password for m-transform user

## To run monasca-transform using the provided vagrant environment

### Using any changes made locally to monasca-transform

    cd tools/vagrant
    vagrant up
    vagrant ssh
    cd devstack
    ./stack.sh

The devstack vagrant environment is set up to share the monasca-transform
directory with the vm, copy it and commit any changes in the vm copy.  This is
because the devstack deploy process checks out the master branch to

    /opt/stack

and deploys using that.  Changes made by the user need to be committed in order
to be used in the devstack instance.  It is important therefore that changes
should not be pushed from the vm as the unevaluated commit would be pushed.

N.B. If you are running with virtualbox you may find that the `./stack.sh` fails with the filesystem becoming read only.  There is a work around:

 1. vagrant up --no-provision && vagrant halt
 2. open virtualbox gui
 3. open target vm settings and change storage controller from SCSI to SATA
 4. vagrant up

### Using the upstream committed state of monasca-transform

This should operate the same as for any other devstack plugin.  However, to use
the plugin from the upstream repo with the vagrant environment as described
above it is sufficient to do:

    cd tools/vagrant
    vagrant up
    vagrant ssh
    cd devstack
    vi local.conf

and change the line

    enable_plugin monasca-transform /home/ubuntu/monasca-transform

to

    enable_plugin monasca-transform https://git.openstack.org/openstack/monasca-transform

before running

    ./stack.sh

### Connecting to devstack

The host key changes with each ```vagrant destroy```/```vagrant up``` cycle so
it is necessary to manage host key verification for your workstation:

    ssh-keygen -R 192.168.15.6

The devstack vm vagrant up process generates a private key which can be used for
passwordless ssh to the host as follows:

    cd tools/vagrant
    ssh -i .vagrant/machines/default/virtualbox/private_key ubuntu@192.168.15.6

### Running tox on devstack

Once the deploy is up use the following commands to set up tox.

    sudo su monasca-transform
    cd /opt/stack/monasca-transform
    virtualenv .venv
    . .venv/bin/activate
    pip install tox
    tox

### Updating the code for dev

To regenerate the environment for development purposes a script is provided
on the devstack instance at
/opt/stack/monasca-transform/tools/vagrant/refresh_monasca_transform.sh
To run the refresh_monasca_transform.sh script on devstack instance

    cd /opt/stack/monasca-transform
    tools/vagrant/refresh_monasca_transform.sh

(note: to use/run tox after running this script, the
"Running tox on devstack" steps above have to be re-executed)

This mostly re-does the work of the devstack plugin, updating the code from the
shared directory, regenerating the venv and the zip that is passed to spark
during the spark-submit call.  The configuration and the transform and
pre transform specs in the database are updated with fresh copies, along
with driver and service python code.

If refresh_monasca_transform.sh script completes successfully you should see
a message like the following in the console.

        ***********************************************
        *                                             *
        * SUCCESS!! refresh monasca transform done.   *
        *                                             *
        ***********************************************

### Development workflow

Here are the normal steps a developer can take to make any code changes. It is
essential that the developer runs all tests in functional tests in a devstack
environment before submitting any changes for review/merge.

Please follow steps mentioned in
"To run monasca-transform using the provided vagrant environment" section above
to create a devstack VM environment before following steps below:

    1. Make code changes on the host machine (e.g. ~/monasca-transform)
    2. vagrant ssh (to connect to the devstack VM)
    3. cd /opt/stack/monasca-transform
    4. tools/vagrant/refresh_monasca_transform.sh (See "Updating the code for dev"
                                                   section above)
    5. cd /opt/stack/monasca-transform (since monasca-transform folder
                                        gets recreated in Step 4. above)
    6. tox -e pep8
    7. tox -e py27
    8. tox -e functional

Note: It is mandatory to run functional unit tests before submitting any changes
for review/merge. These can be currently be run only in a devstack VM since tests
need access to Apache Spark libraries. This is accomplished by setting
SPARK_HOME environment variable which is being done in tox.ini.

    export SPARK_HOME=/opt/spark/current

#### How to find and fix test failures ?

To find which tests failed after running functional tests (After you have run
functional tests as per steps in Development workflow)

    export OS_TEST_PATH=tests/functional
    export SPARK_HOME=/opt/spark/current
    source .tox/functional/bin/activate
    testr run
    testr failing (to get list of tests that failed)

You can add

    import pdb
    pdb.set_trace()

in test or in code where you want to start python debugger.

Run test using

    python -m testtools.run <test>

For example:

    python -m testtools.run  \
        tests.functional.usage.test_host_cpu_usage_component_second_agg.SparkTest

Reference: https://wiki.openstack.org/wiki/Testr

## Access Spark Streaming and Spark Master/Worker User Interface

In a devstack environment ports on which Spark Streaming UI (4040), Spark Master(18080)
and Spark Worker (18081) UI are available are forwarded to the host and are
accessible from the host machine.

    http://<host_machine_ip>:4040/ (Note: Spark Streaming UI,
                                    is available only when
                                    monasca-transform application
                                    is running)
    http://<host_machine_ip>:18080/ (Spark Master UI)
    http://<host_machine_ip>:18081/ (Spark Worker UI)

## To run monasca-transform using a different deployment technology

Monasca-transform requires supporting services, such as Kafka and
Zookeeper, also are set up. So just adding "enable_plugin monasca-transform"
to a default DevStack local.conf is not sufficient to configure a working
DevStack deployment unless these services are also added.

Please reference the devstack/settings file for an example of a working list of
plugins and services as used by the Vagrant deployment.

## WIP

This is a work in progress.  There are a number of improvements necessary to
improve value as a development tool.


###TODO

1. Shorten initial deploy
Currently the services deployed are the default set plus all of monasca.  It's
quite possible that not all of this is necessary to develop monasca-transform.
So some services may be dropped in order to shorten the deploy.

