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
    def is_time_to_run(check_date_time):
        """return True if its time to run this processor.
        It is time to run the processor if:
            The processor has no previous recorded run time.
            It is more than the configured 'late_metric_slack_time' (to allow
            for the arrival of tardy metrics) past the hour and the processor
            has not yet run for this hour
        """

        check_hour = int(datetime.datetime.strftime(check_date_time, '%H'))
        check_date = check_date_time.replace(minute=0, second=0,
                                             microsecond=0, hour=0)
        slack = datetime.timedelta(
            seconds=cfg.CONF.pre_hourly_processor.late_metric_slack_time)

        top_of_the_hour_date_time = check_date_time.replace(
            minute=0, second=0, microsecond=0)
        earliest_acceptable_run_date_time = top_of_the_hour_date_time + slack
        last_processed_date_time = PreHourlyProcessorUtil.get_last_processed()
        if last_processed_date_time:
            last_processed_hour = int(
                datetime.datetime.strftime(
                    last_processed_date_time, '%H'))
            last_processed_date = last_processed_date_time.replace(
                minute=0, second=0, microsecond=0, hour=0)
        else:
            last_processed_date = None
            last_processed_hour = None

        if (check_hour == last_processed_hour
                and last_processed_date == check_date):
            earliest_acceptable_run_date_time = (
                top_of_the_hour_date_time +
                datetime.timedelta(hours=1) +
                slack
            )
        log.debug(
            "Pre-hourly task check: Now date: %s, "
            "Date last processed: %s, Check time = %s, "
            "Last processed at %s (hour = %s), "
            "Earliest acceptable run time %s "
            "(based on configured pre hourly late metrics slack time of %s "
            "seconds)" % (
                check_date,
                last_processed_date,
                check_date_time,
                last_processed_date_time,
                last_processed_hour,
                earliest_acceptable_run_date_time,
                cfg.CONF.pre_hourly_processor.late_metric_slack_time
            ))
        # run pre hourly processor only once from the
        # configured time after the top of the hour
        if (not last_processed_date_time or (
                ((not check_hour == last_processed_hour) or
                    (check_date > last_processed_date)) and
                check_date_time >= earliest_acceptable_run_date_time)):
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
