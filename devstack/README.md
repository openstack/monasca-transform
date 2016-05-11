# Monasca-transform DevStack Plugin

The Monasca-transform DevStack plugin is tested only on Ubuntu 14.04 (Trusty).

A short cut to running monasca-transform in devstack is implemented with vagrant.

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

    enable_plugin monasca-transform /home/vagrant/monasca-transform
    
to

    enable_plugin monasca-transform https://gitlab.gozer.hpcloud.net/host/capacityplanning.git

before running 

    ./stack.sh
    
### Connecting to devstack
    
The host key changes with each ```vagrant destroy```/```vagrant up``` cycle so 
it is necessary to manage host key verification for your workstation:

    ssh-keygen -R 192.168.12.5

The devstack vm vagrant up process generates a private key which can be used for
passwordless ssh to the host as follows:

    cd tools/vagrant
    ssh -i .vagrant/machines/default/virtualbox/private_key vagrant@192.168.12.5

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

    tools/vagrant/refresh_monasca_transform.sh

(note: to use/run tox after running this script, the
"Running tox on devstack" steps above have to be re-executed)
    
This mostly re-does the work of the devstack plugin, updating the code from the
shared directory, regenerating the venv and the zip that is passed to spark 
during the spark-submit call.  The configuration and the contents of the
database are updated with fresh copies also though the start scripts, driver and 
service python code are left as they are (because I'm not envisaging much change
in those).
    
## WIP

This is a work in progress.  There are a number of improvements necessary to 
improve value as a development tool.


###TODO

1. Shorten initial deploy 
Currently the services deployed are the default set plus all of monasca.  It's
quite possible that not all of this is necessary to develop monasca-transform.
So some services may be dropped in order to shorten the deploy.