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


TransformContextBase = namedtuple("TransformContext",
                                  ["config_info",
                                   "offset_info",
                                   "transform_spec_df_info",
                                   "batch_time_info"])


class TransformContext(TransformContextBase):
    """A tuple which contains all the configuration information
    to drive processing


    namedtuple contains:

    config_info - configuration information from oslo config
    offset_info - current kafka offset information
    transform_spec_df - processing information from
                        transform_spec aggregation driver table
    batch_datetime_info -  current batch processing datetime
    """

RddTransformContextBase = namedtuple("RddTransformContext",
                                     ["rdd_info",
                                      "transform_context_info"])


class RddTransformContext(RddTransformContextBase):
    """A tuple which is a wrapper containing the RDD and transform_context

    namdetuple contains:

    rdd_info - rdd
    transform_context_info - transform context
    """


class TransformContextUtils(object):
    """utility method to get TransformContext"""

    @staticmethod
    def get_context(transform_context_info=None,
                    config_info=None,
                    offset_info=None,
                    transform_spec_df_info=None,
                    batch_time_info=None):

        if transform_context_info is None:
            return TransformContext(config_info,
                                    offset_info,
                                    transform_spec_df_info,
                                    batch_time_info)
        else:
            if config_info is None or config_info == "":
                # get from passed in transform_context
                config_info = transform_context_info.config_info

            if offset_info is None or offset_info == "":
                # get from passed in transform_context
                offset_info = transform_context_info.offset_info

            if transform_spec_df_info is None or \
                    transform_spec_df_info == "":
                # get from passed in transform_context
                transform_spec_df_info = \
                    transform_context_info.transform_spec_df_info

            if batch_time_info is None or \
                    batch_time_info == "":
                # get from passed in transform_context
                batch_time_info = \
                    transform_context_info.batch_time_info

            return TransformContext(config_info,
                                    offset_info,
                                    transform_spec_df_info,
                                    batch_time_info)
