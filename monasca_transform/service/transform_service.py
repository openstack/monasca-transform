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
import psutil
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback

from oslo_config import cfg
from oslo_log import log
from oslo_service import loopingcall
from oslo_service import service as os_service
from tooz import coordination

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.log_utils import LogUtils

CONF = cfg.CONF

SPARK_SUBMIT_PROC_NAME = "spark-submit"


def main():
    transform_service = TransformService()
    transform_service.start()


def shutdown_all_threads_and_die():
    """Shut down all threads and exit process.
    Hit it with a hammer to kill all threads and die.
    """
    LOG = log.getLogger(__name__)
    LOG.info('Monasca Transform service stopping...')
    os._exit(1)


def get_process(proc_name):
    """Get process given  string in
    process cmd line.
    """
    LOG = log.getLogger(__name__)
    proc = None
    try:
        for pr in psutil.process_iter():
            for args in pr.cmdline():
                if proc_name in args.split(" "):
                    proc = pr
                    return proc
    except BaseException:
        # pass
        LOG.error("Error fetching {%s} process..." % proc_name)
    return None


def stop_spark_submit_process():
    """Stop spark submit program."""
    LOG = log.getLogger(__name__)
    try:
        # get the driver proc
        pr = get_process(SPARK_SUBMIT_PROC_NAME)

        if pr:
            # terminate (SIGTERM) spark driver proc
            for cpr in pr.children(recursive=False):
                LOG.info("Terminate child pid {%s} ..." % str(cpr.pid))
                cpr.terminate()

            # terminate spark submit proc
            LOG.info("Terminate pid {%s} ..." % str(pr.pid))
            pr.terminate()

    except Exception as e:
        LOG.error("Error killing spark submit "
                  "process: got exception: {%s}" % e.message)


class Transform(os_service.Service):
    """Class used with Openstack service.
    """

    LOG = log.getLogger(__name__)

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

        except BaseException:
            self.LOG.exception("Monasca Transform service "
                               "encountered fatal error. "
                               "Shutting down all threads and exiting")
            shutdown_all_threads_and_die()

    def stop(self):
        stop_spark_submit_process()
        super(os_service.Service, self).stop()


class TransformService(threading.Thread):

    previously_running = False
    LOG = log.getLogger(__name__)

    def __init__(self):
        super(TransformService, self).__init__()

        self.coordinator = None

        self.group = CONF.service.coordinator_group

        # A unique name used for establishing election candidacy
        self.my_host_name = socket.getfqdn()

        # periodic check
        leader_check = loopingcall.FixedIntervalLoopingCall(
            self.periodic_leader_check)
        leader_check.start(interval=float(
            CONF.service.election_polling_frequency))

    def check_if_still_leader(self):
        """Return true if the this host is the
        leader
        """
        leader = None
        try:
            leader = self.coordinator.get_leader(self.group).get()
        except BaseException:
            self.LOG.info('No leader elected yet for group %s' %
                          (self.group))
        if leader and self.my_host_name == leader:
            return True
        # default
        return False

    def periodic_leader_check(self):
        self.LOG.debug("Called periodic_leader_check...")
        try:
            if self.previously_running:
                if not self.check_if_still_leader():

                    # stop spark submit process
                    stop_spark_submit_process()

                    # stand down as a leader
                    try:
                        self.coordinator.stand_down_group_leader(
                            self.group)
                    except BaseException as e:
                        self.LOG.info("Host %s cannot stand down as "
                                      "leader for group %s: "
                                      "got exception {%s}" %
                                      (self.my_host_name, self.group,
                                       e.message))
                    # reset state
                    self.previously_running = False
        except BaseException as e:
            self.LOG.info("periodic_leader_check: "
                          "caught unhandled exception: {%s}" % e.message)

    def when_i_am_elected_leader(self, event):
        """Callback when this host gets elected leader."""

        # set running state
        self.previously_running = True

        self.LOG.info("Monasca Transform service running on %s "
                      "has been elected leader" % str(self.my_host_name))

        if CONF.service.spark_python_files:
            pyfiles = (" --py-files %s"
                       % CONF.service.spark_python_files)
        else:
            pyfiles = ''

        event_logging_dest = ''
        if (CONF.service.spark_event_logging_enabled and
                CONF.service.spark_event_logging_dest):
            event_logging_dest = (
                "--conf spark.eventLog.dir="
                "file://%s" %
                CONF.service.spark_event_logging_dest)

        # Build the command to start the Spark driver
        spark_cmd = "".join((
            "export SPARK_HOME=",
            CONF.service.spark_home,
            " && ",
            "spark-submit --master ",
            CONF.service.spark_master_list,
            " --conf spark.eventLog.enabled=",
            CONF.service.spark_event_logging_enabled,
            event_logging_dest,
            " --jars " + CONF.service.spark_jars_list,
            pyfiles,
            " " + CONF.service.spark_driver))

        # Start the Spark driver
        # (specify shell=True in order to
        #  correctly handle wildcards in the spark_cmd)
        subprocess.call(spark_cmd, shell=True)

    def run(self):

        self.LOG.info('The host of this Monasca Transform service is ' +
                      self.my_host_name)

        # Loop until the service is stopped
        while True:

            try:

                self.previously_running = False

                # Start an election coordinator
                self.coordinator = coordination.get_coordinator(
                    CONF.service.coordinator_address, self.my_host_name)

                self.coordinator.start()

                # Create a coordination/election group
                try:
                    request = self.coordinator.create_group(self.group)
                    request.get()
                except coordination.GroupAlreadyExist:
                    self.LOG.info('Group %s already exists' % self.group)

                # Join the coordination/election group
                try:
                    request = self.coordinator.join_group(self.group)
                    request.get()
                except coordination.MemberAlreadyExist:
                    self.LOG.info('Host already joined to group %s as %s' %
                                  (self.group, self.my_host_name))

                # Announce the candidacy and wait to be elected
                self.coordinator.watch_elected_as_leader(
                    self.group,
                    self.when_i_am_elected_leader)

                while self.previously_running is False:
                    self.LOG.debug('Monasca Transform service on %s is '
                                   'checking election results...'
                                   % self.my_host_name)
                    self.coordinator.heartbeat()
                    self.coordinator.run_watchers()
                    if self.previously_running is True:
                        try:
                            # Leave/exit the coordination/election group
                            request = self.coordinator.leave_group(self.group)
                            request.get()
                        except coordination.MemberNotJoined:
                            self.LOG.info("Host has not yet "
                                          "joined group %s as %s" %
                                          (self.group, self.my_host_name))
                    time.sleep(float(CONF.service.election_polling_frequency))

                self.coordinator.stop()

            except BaseException as e:
                # catch any unhandled exception and continue
                self.LOG.info("Ran into unhandled exception: {%s}" % e.message)
                self.LOG.info("Going to restart coordinator again...")
                traceback.print_exc()


def main_service():
    """Method to use with Openstack service.
    """
    ConfigInitializer.basic_config()
    LogUtils.init_logger(__name__)
    launcher = os_service.ServiceLauncher(cfg.CONF)
    launcher.launch_service(Transform())
    launcher.wait()

# Used if run without Openstack service.
if __name__ == "__main__":
    sys.exit(main())
