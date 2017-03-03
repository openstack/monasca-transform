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
import datetime
import unittest

from monasca_transform.config.config_initializer import ConfigInitializer

ConfigInitializer.basic_config(
    default_config_files=[
        'tests/unit/test_resources/config/'
        'test_config.conf']
)
from monasca_transform.processor.processor_util import PreHourlyProcessorUtil
from monasca_transform.processor.processor_util import ProcessUtilDataProvider


class PreHourlyProcessorTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_is_time_to_run_before_late_metric_slack_time(self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=11,
            minute=9, second=59, microsecond=0)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=(check_time + datetime.timedelta(hours=-1)))
        self.assertFalse(PreHourlyProcessorUtil.is_time_to_run(check_time))

    def test_is_time_to_run_after_late_metric_slack_time(self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=11,
            minute=10, second=0, microsecond=1)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=(check_time + datetime.timedelta(hours=-1)))
        self.assertTrue(PreHourlyProcessorUtil.is_time_to_run(check_time))

    def test_is_time_to_run_with_already_done_this_hour(self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=11,
            minute=30, second=0, microsecond=0)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=check_time)
        self.assertFalse(PreHourlyProcessorUtil.is_time_to_run(check_time))

    def test_is_time_to_run_after_midnight_but_before_late_metric_slack_time(
            self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=0,
            minute=5, second=0, microsecond=0)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=(check_time + datetime.timedelta(hours=-1)))
        self.assertFalse(PreHourlyProcessorUtil.is_time_to_run(check_time))

    def test_is_time_to_run_after_midnight_and_after_late_metric_slack_time(
            self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=0,
            minute=10, second=0, microsecond=1)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=(check_time + datetime.timedelta(hours=-1)))
        self.assertTrue(PreHourlyProcessorUtil.is_time_to_run(check_time))

    def test_after_midnight_having_already_run(
            self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=0,
            minute=20, second=0, microsecond=1)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=(check_time + datetime.timedelta(minutes=-10)))
        self.assertFalse(PreHourlyProcessorUtil.is_time_to_run(check_time))

    def test_am_pm_behaviour(self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=22,
            minute=10, second=0, microsecond=1)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=(check_time + datetime.timedelta(hours=-12)))
        self.assertTrue(PreHourlyProcessorUtil.is_time_to_run(check_time))

    def test_same_time_different_day_behaviour(self):
        check_time = datetime.datetime(
            year=2016, month=11, day=7, hour=22,
            minute=10, second=0, microsecond=1)
        PreHourlyProcessorUtil.get_data_provider().set_last_processed(
            date_time=(check_time + datetime.timedelta(days=-1)))
        self.assertTrue(PreHourlyProcessorUtil.is_time_to_run(check_time))


class TestProcessUtilDataProvider(ProcessUtilDataProvider):

    _last_processed_date_time = None

    def get_last_processed(self):
        return self._last_processed_date_time

    def set_last_processed(self, date_time=None):
        self._last_processed_date_time = date_time
