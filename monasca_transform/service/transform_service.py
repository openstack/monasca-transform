# Copyright 2016 Hewlett Packard Enterprise Development Company LP
#
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

import os
import signal
import socket
from subprocess import call
import sys
import threading
import time

from oslo_config import cfg
from oslo_log import log
from oslo_service import service as os_service

from tooz import coordination

from monasca_transform.config.config_initializer import ConfigInitializer


LOG = log.getLogger(__name__)
log.register_options(cfg.CONF)
log.set_defaults()
log.setup(cfg.CONF, 'transform')

CONF = cfg.CONF


def main():
    transform_service = TransformService()
    transform_service.start()


def shutdown_all_threads_and_die():
    """Shut down all threads and exit process.
    Hit it with a hammer to kill all threads and die.
    """
    LOG.info('Monasca Transform service stopping...')
    os._exit(1)


class Transform(os_service.Service):
    """Class used with Openstack service.
    """

    def __init__(self, threads=1):
        super(Transform, self).__init__(threads)

    def signal_handler(self, signal_number, stack_frame):
        # Catch stop requests and appropriately shut down
        shutdown_all_threads_and_die()

    def start(self):
        try:
            # Register to catch stop requests
            signal.signal(signal.SIGTERM, self.signal_handler)

            main()

        except Exception:
            LOG.exception('Monasca Transform service encountered fatal error. '
                          'Shutting down all threads and exiting')
            shutdown_all_threads_and_die()

    def stop(self, graceful):
        shutdown_all_threads_and_die()


class TransformService(threading.Thread):

    previously_running = False

    # A unique name used for establishing election candidacy
    my_host_name = socket.getfqdn()

    def __init__(self):
        super(TransformService, self).__init__()

    def when_i_am_elected_leader(self, event):
        try:
            LOG.info('Monasca Transform service running on ' +
                     self.my_host_name + ' has been elected leader')
            self.previously_running = True

            if CONF.service.spark_python_files:
                pyfiles = " --py-files %s" % CONF.service.spark_python_files
            else:
                pyfiles = ''

            if (CONF.service.spark_event_logging_enabled and
                    CONF.service.spark_event_logging_dest):
                event_logging_dest = (" --conf spark.eventLog.dir=file://%s" %
                                      CONF.service.spark_event_logging_dest)
            else:
                event_logging_dest = ''

            # Build the command to start the Spark driver
            spark_cmd = ("export SPARK_HOME=" +
                         CONF.service.spark_home + " && "
                         "spark-submit --supervise --master " +
                         CONF.service.spark_master_list +
                         " --conf spark.eventLog.enabled=" +
                         CONF.service.spark_event_logging_enabled +
                         event_logging_dest +
                         " --jars " + CONF.service.spark_jars_list +
                         pyfiles +
                         " " + CONF.service.spark_driver)

            # Start the Spark driver (specify shell=True in order to
            # correctly handle wildcards in the spark_cmd
            call(spark_cmd, shell=True)

        except Exception:
            LOG.exception(
                'TransformService on ' + self.my_host_name +
                ' encountered fatal exception. '
                'Shutting down all threads and exiting')
            shutdown_all_threads_and_die()

    def run(self):
        LOG.info('The host of this Monasca Transform service is ' +
                 self.my_host_name)

        # Loop until the service is stopped
        while True:

            self.previously_running = False

            # Start an election coordinator
            coordinator = coordination.get_coordinator(
                CONF.service.coordinator_address, self.my_host_name)
            coordinator.start()

            # Create a coordination/election group
            group = CONF.service.coordinator_group
            try:
                request = coordinator.create_group(group)
                request.get()
            except coordination.GroupAlreadyExist:
                LOG.info('Group %s already exists' % group)

            # Join the coordination/election group
            try:
                request = coordinator.join_group(group)
                request.get()
            except coordination.MemberAlreadyExist:
                LOG.info('Host already joined to group %s as %s' %
                         (group, self.my_host_name))

            # Announce the candidacy and wait to be elected
            coordinator.watch_elected_as_leader(group,
                                                self.when_i_am_elected_leader)

            while self.previously_running is False:
                LOG.info('Monasca Transform service on %s is checking'
                         ' election results...' % self.my_host_name)
                coordinator.heartbeat()
                coordinator.run_watchers()
                if self.previously_running is True:
                    try:
                        # Leave/exit the coordination/election group
                        request = coordinator.leave_group(group)
                        request.get()
                    except coordination.MemberNotJoined:
                        LOG.info('Host has not yet joined group %s as %s' %
                                 (group, self.my_host_name))
                time.sleep(float(CONF.service.election_polling_frequency))

            coordinator.stop()


def main_service():
    """Method to use with Openstack service.
    """
    ConfigInitializer.basic_config()

    launcher = os_service.ServiceLauncher(CONF)
    launcher.launch_service(Transform())
    launcher.wait()


# Used if run without Openstack service.
if __name__ == "__main__":
    sys.exit(main())
