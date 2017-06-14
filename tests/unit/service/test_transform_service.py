# Copyright 2016-2017 Hewlett Packard Enterprise Development Company LP
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

import mock
from mock import MagicMock
import os

from oslo_config import cfg
import oslo_service

import time
import traceback
import unittest

from monasca_transform.config.config_initializer import ConfigInitializer

from monasca_transform.service import transform_service

from tooz.drivers.zookeeper import KazooDriver

ConfigInitializer.basic_config(
    default_config_files=[
        'tests/unit/test_resources/config/'
        'test_config.conf']
)


class TransformServiceTestBase(unittest.TestCase):

    def setUp(self):
        super(TransformServiceTestBase, self).setUp()
        self.conf = cfg.CONF

    def _spawn_transform_service(self):
        """Launch transform service and get pid."""
        status = 0
        pid = os.fork()
        if pid == 0:
            try:
                os.setsid()
                # start transform service
                launcher = oslo_service.service.launch(
                    self.conf,
                    transform_service.Transform(),
                    workers=1)
                status = launcher.wait()
            except SystemExit as exc:
                traceback.print_exc()
                status = exc.code
            except BaseException:
                try:
                    traceback.print_exc()
                except BaseException:
                    print("Could not print traceback")
                status = 2
            os._exit(status or 0)
        return pid

    @mock.patch('tooz.coordination.get_coordinator')
    def test_transform_service_heartbeat(self, coordinator):

        # mock coordinator
        fake_kazoo_driver = MagicMock(name="MagicKazooDriver",
                                      spec=KazooDriver)
        coordinator.return_value = fake_kazoo_driver

        # option1
        serv_thread = transform_service.TransformService()
        serv_thread.daemon = True
        serv_thread.start()
        time.sleep(2)

        # option2
        # mocks dont seem to work when spawning a service
        # pid = _spawn_transform_service()
        # time.sleep()
        # os.kill(pid, signal.SIGNAL_SIGTERM)

        fake_kazoo_driver.heartbeat.assert_called_with()

    @mock.patch('tooz.coordination.get_coordinator')
    @mock.patch('monasca_transform.service.transform_service'
                '.stop_spark_submit_process')
    def test_transform_service_periodic_check(self,
                                              stopspark,
                                              coordinator):

        # mock coordinator
        fake_kazoo_driver = MagicMock(name="MagicKazooDriver",
                                      spec=KazooDriver)
        fake_kazoo_driver.get_leader.get.return_value = "someotherhost"
        coordinator.return_value = fake_kazoo_driver

        # set up transform service
        serv_thread = transform_service.TransformService()
        serv_thread.daemon = True
        # assume that the driver was running
        serv_thread.previously_running = True
        # set the coordinator
        serv_thread.coordinator = fake_kazoo_driver

        # call periodic leader check
        serv_thread.periodic_leader_check()

        # verify if standown leader was called
        fake_kazoo_driver.stand_down_group_leader.assert_called_with(
            'monasca-transform')
        # verify if stop spark submit process was called
        stopspark.assert_called()

    @mock.patch('tooz.coordination.get_coordinator')
    @mock.patch('subprocess.call')
    def test_transform_service_leader_election(self,
                                               spark_submit_call,
                                               coordinator):
        # mock coordinator
        fake_kazoo_driver = MagicMock(name="MagicKazooDriver",
                                      spec=KazooDriver)
        coordinator.return_value = fake_kazoo_driver

        # set up transform service
        serv_thread = transform_service.TransformService()
        serv_thread.daemon = True

        # test previously running value
        self.assertFalse(serv_thread.previously_running)

        fake_event = MagicMock(name="fake_event",
                               spec='tooz.coordination.Event')

        # call leader election function
        serv_thread.when_i_am_elected_leader(fake_event)

        # test if subcall was called
        spark_submit_call.assert_called()

        # test previously running value
        self.assertTrue(serv_thread.previously_running)
