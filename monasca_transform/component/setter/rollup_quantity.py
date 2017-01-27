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

import datetime

from monasca_transform.component import Component
from monasca_transform.component.component_utils import ComponentUtils
from monasca_transform.component.setter import SetterComponent
from monasca_transform.transform.transform_utils import InstanceUsageUtils

import json


class RollupQuantityException(Exception):
    """Exception thrown when doing quantity rollup
    Attributes:
    value: string representing the error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RollupQuantity(SetterComponent):

    @staticmethod
    def _supported_rollup_operations():
        return ["sum", "max", "min", "avg"]

    @staticmethod
    def _is_valid_rollup_operation(operation):
        if operation in RollupQuantity._supported_rollup_operations():
            return True
        else:
            return False

    @staticmethod
    def _rollup_quantity(instance_usage_df,
                         setter_rollup_group_by_list,
                         setter_rollup_operation):

        instance_usage_data_json_list = []

        # check if operation is valid
        if not RollupQuantity.\
                _is_valid_rollup_operation(setter_rollup_operation):
            raise RollupQuantityException(
                "Operation %s is not supported" % setter_rollup_operation)

        # call required operation on grouped data
        # e.g. sum, max, min, avg etc
        agg_operations_map = {
            "quantity": str(setter_rollup_operation),
            "firstrecord_timestamp_unix": "min",
            "lastrecord_timestamp_unix": "max",
            "record_count": "sum"}

        # do a group by
        grouped_data = instance_usage_df.groupBy(
            *setter_rollup_group_by_list)
        rollup_df = grouped_data.agg(agg_operations_map)

        for row in rollup_df.collect():

            # first record timestamp
            earliest_record_timestamp_unix = getattr(
                row, "min(firstrecord_timestamp_unix)",
                Component.DEFAULT_UNAVAILABLE_VALUE)
            earliest_record_timestamp_string = \
                datetime.datetime.utcfromtimestamp(
                    earliest_record_timestamp_unix).strftime(
                    '%Y-%m-%d %H:%M:%S')

            # last record_timestamp
            latest_record_timestamp_unix = getattr(
                row, "max(lastrecord_timestamp_unix)",
                Component.DEFAULT_UNAVAILABLE_VALUE)
            latest_record_timestamp_string = \
                datetime.datetime.utcfromtimestamp(
                    latest_record_timestamp_unix).strftime('%Y-%m-%d %H:%M:%S')

            # record count
            record_count = getattr(row, "sum(record_count)", 0.0)

            # quantity
            # get expression that will be used to select quantity
            # from rolled up data
            select_quant_str = "".join((setter_rollup_operation, "(quantity)"))
            quantity = getattr(row, select_quant_str, 0.0)

            try:
                processing_meta = row.processing_meta
            except AttributeError:
                processing_meta = {}

            # create a new instance usage dict
            instance_usage_dict = {"tenant_id": getattr(row, "tenant_id",
                                                        "all"),
                                   "user_id":
                                       getattr(row, "user_id", "all"),
                                   "resource_uuid":
                                       getattr(row, "resource_uuid", "all"),
                                   "namespace":
                                       getattr(row, "namespace", "all"),
                                   "pod_name":
                                       getattr(row, "pod_name", "all"),
                                   "app":
                                       getattr(row, "app", "all"),
                                   "container_name":
                                       getattr(row, "container_name", "all"),
                                   "interface":
                                       getattr(row, "interface", "all"),
                                   "deployment":
                                       getattr(row, "deployment", "all"),
                                   "daemon_set":
                                       getattr(row, "daemon_set", "all"),
                                   "geolocation":
                                       getattr(row, "geolocation", "all"),
                                   "region":
                                       getattr(row, "region", "all"),
                                   "zone":
                                       getattr(row, "zone", "all"),
                                   "host":
                                       getattr(row, "host", "all"),
                                   "project_id":
                                       getattr(row, "tenant_id", "all"),
                                   "aggregated_metric_name":
                                       getattr(row, "aggregated_metric_name",
                                               "all"),
                                   "quantity":
                                       quantity,
                                   "firstrecord_timestamp_unix":
                                       earliest_record_timestamp_unix,
                                   "firstrecord_timestamp_string":
                                       earliest_record_timestamp_string,
                                   "lastrecord_timestamp_unix":
                                       latest_record_timestamp_unix,
                                   "lastrecord_timestamp_string":
                                       latest_record_timestamp_string,
                                   "record_count": record_count,
                                   "service_group":
                                       getattr(row, "service_group", "all"),
                                   "service_id":
                                       getattr(row, "service_id", "all"),
                                   "usage_date":
                                       getattr(row, "usage_date", "all"),
                                   "usage_hour":
                                       getattr(row, "usage_hour", "all"),
                                   "usage_minute":
                                       getattr(row, "usage_minute", "all"),
                                   "aggregation_period":
                                       getattr(row, "aggregation_period",
                                               "all"),
                                   "processing_meta": processing_meta
                                   }

            instance_usage_data_json = json.dumps(instance_usage_dict)
            instance_usage_data_json_list.append(instance_usage_data_json)

        # convert to rdd
        spark_context = instance_usage_df.rdd.context
        return spark_context.parallelize(instance_usage_data_json_list)

    @staticmethod
    def setter(transform_context, instance_usage_df):

        transform_spec_df = transform_context.transform_spec_df_info

        # get rollup operation (sum, max, avg, min)
        agg_params = transform_spec_df.select(
            "aggregation_params_map.setter_rollup_operation").\
            collect()[0].asDict()
        setter_rollup_operation = agg_params["setter_rollup_operation"]

        instance_usage_trans_df = RollupQuantity.setter_by_operation(
            transform_context,
            instance_usage_df,
            setter_rollup_operation)

        return instance_usage_trans_df

    @staticmethod
    def setter_by_operation(transform_context, instance_usage_df,
                            setter_rollup_operation):

        transform_spec_df = transform_context.transform_spec_df_info

        # get fields we want to group by for a rollup
        agg_params = transform_spec_df.select(
            "aggregation_params_map.setter_rollup_group_by_list"). \
            collect()[0].asDict()
        setter_rollup_group_by_list = agg_params["setter_rollup_group_by_list"]

        # get aggregation period
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_period").collect()[0].asDict()
        aggregation_period = agg_params["aggregation_period"]
        group_by_period_list = \
            ComponentUtils._get_instance_group_by_period_list(
                aggregation_period)

        # group by columns list
        group_by_columns_list = \
            group_by_period_list + setter_rollup_group_by_list

        # perform rollup operation
        instance_usage_json_rdd = RollupQuantity._rollup_quantity(
            instance_usage_df,
            group_by_columns_list,
            str(setter_rollup_operation))

        sql_context = SQLContext.getOrCreate(instance_usage_df.rdd.context)
        instance_usage_trans_df = InstanceUsageUtils.create_df_from_json_rdd(
            sql_context,
            instance_usage_json_rdd)

        return instance_usage_trans_df

    @staticmethod
    def do_rollup(setter_rollup_group_by_list,
                  aggregation_period,
                  setter_rollup_operation,
                  instance_usage_df):

        # get aggregation period
        group_by_period_list = \
            ComponentUtils._get_instance_group_by_period_list(
                aggregation_period)

        # group by columns list
        group_by_columns_list = group_by_period_list + \
            setter_rollup_group_by_list

        # perform rollup operation
        instance_usage_json_rdd = RollupQuantity._rollup_quantity(
            instance_usage_df,
            group_by_columns_list,
            str(setter_rollup_operation))

        sql_context = SQLContext.getOrCreate(instance_usage_df.rdd.context)
        instance_usage_trans_df = InstanceUsageUtils.create_df_from_json_rdd(
            sql_context,
            instance_usage_json_rdd)

        return instance_usage_trans_df
