#!/usr/bin/env bash

if [ -d /root/.m2 ]
then
    echo /root/.m2 already present `ls -la /root`
else
    echo Creating directory for maven in /root
    cd /root
    echo Present working directory is `pwd`
    sudo mkdir .m2
    echo Created .m2 directory `ls -la`
fi

echo Now we need to move the settings.xml into place...
cp /home/ubuntu/settings.xml /root/.m2/.
chown root:root /root/.m2/settings.xml
