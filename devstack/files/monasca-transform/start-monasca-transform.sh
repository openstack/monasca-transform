#!/usr/bin/env bash

start-stop-daemon -c monasca-transform:monasca-transform -m\
    --pidfile /var/run/monasca/transform/transform.pid \
    --start --exec /opt/monasca/transform/venv/bin/python /etc/monasca/transform/init/service_runner.py \
    >> /var/log/monasca/transform/monasca-transform.log 2>> /var/log/monasca/transform/monasca-transform.log