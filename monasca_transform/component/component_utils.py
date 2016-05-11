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

import logging

LOG = logging.getLogger(__name__)


class ComponentUtils(object):

    @staticmethod
    def _get_group_by_period_list(aggregation_period):
        """get a list of columns for an aggregation period."""
        group_by_period_list = []
        if (aggregation_period == "daily"):
            group_by_period_list = ["event_date"]
        elif (aggregation_period == "hourly"):
            group_by_period_list = ["event_date", "event_hour"]
        elif (aggregation_period == "minutely"):
            group_by_period_list = ["event_date", "event_hour", "event_minute"]
        elif (aggregation_period == "secondly"):
            group_by_period_list = ["event_date", "event_hour",
                                    "event_minute", "event_second"]
        return group_by_period_list

    @staticmethod
    def _get_instance_group_by_period_list(aggregation_period):
        """get a list of columns for an aggregation period."""
        group_by_period_list = []
        if (aggregation_period == "daily"):
            group_by_period_list = ["usage_date"]
        elif (aggregation_period == "hourly"):
            group_by_period_list = ["usage_date", "usage_hour"]
        elif (aggregation_period == "minutely"):
            group_by_period_list = ["usage_date", "usage_hour", "usage_minute"]
        elif (aggregation_period == "secondly"):
            group_by_period_list = ["usage_date", "usage_hour",
                                    "usage_minute", "usage_second"]
        return group_by_period_list
