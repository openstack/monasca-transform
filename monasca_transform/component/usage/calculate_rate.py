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

from pyspark.sql import SQLContext

from monasca_transform.component import Component
from monasca_transform.component.setter.rollup_quantity import RollupQuantity
from monasca_transform.component.usage.fetch_quantity import FetchQuantity
from monasca_transform.component.usage import UsageComponent
from monasca_transform.transform.transform_utils import InstanceUsageUtils

import json


class CalculateRateException(Exception):
    """Exception thrown when calculating rate
    Attributes:
    value: string representing the error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class CalculateRate(UsageComponent):

    @staticmethod
    def usage(transform_context, record_store_df):
        """component which groups together record store records by
        provided group by columns list,sorts within the group by event
        timestamp field, calculates the rate of change between the
        oldest and latest values, and returns the resultant value as an
        instance usage dataframe
        """
        instance_usage_data_json_list = []

        transform_spec_df = transform_context.transform_spec_df_info

        # get aggregated metric name
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregated_metric_name"). \
            collect()[0].asDict()
        aggregated_metric_name = agg_params["aggregated_metric_name"]

        # get aggregation period
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_period").collect()[0].asDict()
        aggregation_period = agg_params["aggregation_period"]

        # Fetch the oldest quantities
        latest_instance_usage_df = \
            FetchQuantity().usage_by_operation(transform_context,
                                               record_store_df,
                                               "avg")

        # Roll up the latest quantities
        latest_rolled_up_instance_usage_df = \
            RollupQuantity().setter_by_operation(transform_context,
                                                 latest_instance_usage_df,
                                                 "sum")

        # Fetch the oldest quantities
        oldest_instance_usage_df = \
            FetchQuantity().usage_by_operation(transform_context,
                                               record_store_df,
                                               "oldest")

        # Roll up the oldest quantities
        oldest_rolled_up_instance_usage_df = \
            RollupQuantity().setter_by_operation(transform_context,
                                                 oldest_instance_usage_df,
                                                 "sum")

        # Calculate the rate change by percentage
        oldest_dict = oldest_rolled_up_instance_usage_df.collect()[0].asDict()
        oldest_quantity = float(oldest_dict['quantity'])

        latest_dict = latest_rolled_up_instance_usage_df.collect()[0].asDict()
        latest_quantity = float(latest_dict['quantity'])

        rate_percentage = \
            ((oldest_quantity - latest_quantity) / oldest_quantity) * 100

        #  create a new instance usage dict
        instance_usage_dict = {"tenant_id":
                               latest_dict.get("tenant_id", "all"),
                               "user_id":
                                   latest_dict.get("user_id", "all"),
                               "resource_uuid":
                                   latest_dict.get("resource_uuid", "all"),
                               "geolocation":
                                   latest_dict.get("geolocation", "all"),
                               "region":
                                   latest_dict.get("region", "all"),
                               "zone":
                                   latest_dict.get("zone", "all"),
                               "host":
                                   latest_dict.get("host", "all"),
                               "project_id":
                                   latest_dict.get("project_id", "all"),
                               "aggregated_metric_name":
                                   aggregated_metric_name,
                               "quantity": rate_percentage,
                               "firstrecord_timestamp_unix":
                                   oldest_dict["firstrecord_timestamp_unix"],
                               "firstrecord_timestamp_string":
                                   oldest_dict["firstrecord_timestamp_string"],
                               "lastrecord_timestamp_unix":
                                   latest_dict["lastrecord_timestamp_unix"],
                               "lastrecord_timestamp_string":
                                   latest_dict["lastrecord_timestamp_string"],
                               "record_count": oldest_dict["record_count"] +
                                   latest_dict["record_count"],
                               "service_group":
                                   latest_dict.get("service_group",
                                                   Component.
                                                   DEFAULT_UNAVAILABLE_VALUE),
                               "service_id":
                                   latest_dict.get("service_id",
                                                   Component.
                                                   DEFAULT_UNAVAILABLE_VALUE),
                               "usage_date": latest_dict["usage_date"],
                               "usage_hour": latest_dict["usage_hour"],
                               "usage_minute": latest_dict["usage_minute"],
                               "aggregation_period": aggregation_period,
                               "processing_meta":
                                   {"event_type":
                                    latest_dict.get("event_type",
                                                    Component.
                                                    DEFAULT_UNAVAILABLE_VALUE),
                                    "oldest_timestamp_string":
                                    oldest_dict[
                                        "firstrecord_timestamp_string"],
                                    "oldest_quantity": oldest_quantity,
                                    "latest_timestamp_string":
                                        latest_dict[
                                            "lastrecord_timestamp_string"],
                                    "latest_quantity": latest_quantity
                                    }
                               }

        instance_usage_data_json = json.dumps(instance_usage_dict)
        instance_usage_data_json_list.append(instance_usage_data_json)
        spark_context = record_store_df.rdd.context

        instance_usage_rdd = \
            spark_context.parallelize(instance_usage_data_json_list)

        sql_context = SQLContext\
            .getOrCreate(record_store_df.rdd.context)
        instance_usage_df = InstanceUsageUtils.create_df_from_json_rdd(
            sql_context,
            instance_usage_rdd)

        return instance_usage_df
