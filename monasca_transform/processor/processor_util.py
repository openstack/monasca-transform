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
import abc
import datetime
from monasca_common.simport import simport
from oslo_config import cfg

from monasca_transform.log_utils import LogUtils


log = LogUtils.init_logger(__name__)


class PreHourlyProcessorUtil(object):

    data_provider = None

    @staticmethod
    def get_last_processed():
        return PreHourlyProcessorUtil.get_data_provider().get_last_processed()

    @staticmethod
    def get_data_provider():
        if not PreHourlyProcessorUtil.data_provider:
            PreHourlyProcessorUtil.data_provider = simport.load(
                cfg.CONF.pre_hourly_processor.data_provider)()
        return PreHourlyProcessorUtil.data_provider

    @staticmethod
    def is_time_to_run(check_time):
        """return True if its time to run this processor.
        For now it just checks to see if its start of the hour
        i.e. the minute is 00.
        """
        this_hour = int(datetime.datetime.strftime(check_time, '%H'))
        this_date = check_time.replace(minute=0, second=0,
                                       microsecond=0, hour=0)
        drift_delta = datetime.timedelta(
            seconds=cfg.CONF.pre_hourly_processor.late_metric_slack_time)

        top_of_the_hour = check_time.replace(minute=0, second=0,
                                             microsecond=0)
        earliest_acceptable_run_time = top_of_the_hour + drift_delta
        last_processed = PreHourlyProcessorUtil.get_last_processed()
        if last_processed:
            hour_last_processed = int(
                datetime.datetime.strftime(
                    last_processed, '%H'))
            date_last_processed = last_processed.replace(minute=0, second=0,
                                                         microsecond=0,
                                                         hour=0)
        else:
            date_last_processed = None
            hour_last_processed = None

        if this_hour == hour_last_processed:
            earliest_acceptable_run_time = (
                top_of_the_hour +
                datetime.timedelta(hours=1) +
                drift_delta
            )
        log.debug(
            "Pre-hourly task check: Check time = %s, "
            "Last processed at %s (hour = %s), "
            "Earliest acceptable run time %s "
            "(based on configured pre hourly late metrics slack time of %s "
            "seconds)" % (
                check_time,
                last_processed,
                hour_last_processed,
                earliest_acceptable_run_time,
                cfg.CONF.pre_hourly_processor.late_metric_slack_time
            ))
        # run pre hourly processor only once from the
        # configured time after the top of the hour
        if (not hour_last_processed or (
                ((not this_hour == hour_last_processed) or
                    (this_date > date_last_processed)) and
                check_time >= earliest_acceptable_run_time)):
            log.debug("Pre-hourly: Yes, it's time to process")
            return True
        log.debug("Pre-hourly: No, it's NOT time to process")
        return False


class ProcessUtilDataProvider(object):

    @abc.abstractmethod
    def get_last_processed(self):
        """return data on last run of processor"""
        raise NotImplementedError(
            "Class %s doesn't implement is_time_to_run()"
            % self.__class__.__name__)
