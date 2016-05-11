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

from collections import namedtuple


class Component(object):

    SOURCE_COMPONENT_TYPE = "source"
    USAGE_COMPONENT_TYPE = "usage"
    SETTER_COMPONENT_TYPE = "setter"
    INSERT_COMPONENT_TYPE = "insert"

    DEFAULT_UNAVAILABLE_VALUE = "NA"


InstanceUsageDataAggParamsBase = namedtuple('InstanceUsageDataAggParams',
                                            ['instance_usage_data',
                                             'agg_params'])


class InstanceUsageDataAggParams(InstanceUsageDataAggParamsBase):
    """A tuple which is a wrapper containing the instance usage data
    and aggregation params

    namdetuple contains:

    instance_usage_data - instance usage
    agg_params - aggregation params dict

    """
