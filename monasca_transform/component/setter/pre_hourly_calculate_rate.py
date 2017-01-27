# (c) Copyright 2016 Hewlett Packard Enterprise Development LP
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

from pyspark.sql import functions
from pyspark.sql import SQLContext

from monasca_transform.component import Component
from monasca_transform.component.setter import SetterComponent
from monasca_transform.transform.transform_utils import InstanceUsageUtils

import json


class PreHourlyCalculateRateException(Exception):
    """Exception thrown when doing pre-hourly rate calculations
    Attributes:
    value: string representing the error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class PreHourlyCalculateRate(SetterComponent):

    @staticmethod
    def _calculate_rate(instance_usage_df):
        instance_usage_data_json_list = []

        try:
            sorted_oldest_ascending_df = instance_usage_df.sort(
                functions.asc("processing_meta.oldest_timestamp_string"))

            sorted_latest_descending_df = instance_usage_df.sort(
                functions.desc("processing_meta.latest_timestamp_string"))

            # Calculate the rate change by percentage
            oldest_dict = sorted_oldest_ascending_df.collect()[0].asDict()
            oldest_quantity = float(oldest_dict[
                                    "processing_meta"]["oldest_quantity"])

            latest_dict = sorted_latest_descending_df.collect()[0].asDict()
            latest_quantity = float(latest_dict[
                                    "processing_meta"]["latest_quantity"])

            rate_percentage = 100 * (
                (oldest_quantity - latest_quantity) / oldest_quantity)
        except Exception as e:
            raise PreHourlyCalculateRateException(
                "Exception occurred in pre-hourly rate calculation. Error: %s"
                % str(e))

        #  create a new instance usage dict
        instance_usage_dict = {"tenant_id":
                               latest_dict.get("tenant_id", "all"),
                               "user_id":
                               latest_dict.get("user_id", "all"),
                               "resource_uuid":
                               latest_dict.get("resource_uuid", "all"),
                               "namespace":
                                   latest_dict.get("namespace", "all"),
                               "pod_name":
                                   latest_dict.get("pod_name", "all"),
                               "app":
                                   latest_dict.get("app", "all"),
                               "container_name":
                                   latest_dict.get("container_name", "all"),
                               "interface":
                                   latest_dict.get("interface", "all"),
                               "deployment":
                                   latest_dict.get("deployment", "all"),
                               "daemon_set":
                                   latest_dict.get("daemon_set", "all"),
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
                               latest_dict["aggregated_metric_name"],
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
                               "aggregation_period":
                               latest_dict["aggregation_period"]
                               }

        instance_usage_data_json = json.dumps(instance_usage_dict)
        instance_usage_data_json_list.append(instance_usage_data_json)

        # convert to rdd
        spark_context = instance_usage_df.rdd.context
        return spark_context.parallelize(instance_usage_data_json_list)

    @staticmethod
    def do_rate_calculation(instance_usage_df):
        instance_usage_json_rdd = PreHourlyCalculateRate._calculate_rate(
            instance_usage_df)

        sql_context = SQLContext.getOrCreate(instance_usage_df.rdd.context)
        instance_usage_trans_df = InstanceUsageUtils.create_df_from_json_rdd(
            sql_context,
            instance_usage_json_rdd)

        return instance_usage_trans_df
