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

echo Move the settings.xml into place

ACTUAL_PROXY=`echo ${http_proxy} | awk 'BEGIN { FS = "//"} ;{ print $2 }'`
PROXY_HOST=`echo $ACTUAL_PROXY | awk 'BEGIN { FS = ":"} ;{ print $1 }'`
PROXY_PORT=`echo $ACTUAL_PROXY | awk 'BEGIN { FS = ":"} ;{ gsub("[^0-9]", "", $2) ; print $2 }'`
echo Assuming http proxy host = ${PROXY_HOST}, port = ${PROXY_PORT}
sed -i "s/proxy_host/${PROXY_HOST}/g" /home/vagrant/settings.xml
sed -i "s/proxy_port/${PROXY_PORT}/g" /home/vagrant/settings.xml
cp /home/vagrant/settings.xml /root/.m2/.
chown root:root /root/.m2/settings.xml
