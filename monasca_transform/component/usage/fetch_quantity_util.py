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

from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql import SQLContext

from monasca_transform.component import Component
from monasca_transform.component.component_utils import ComponentUtils
from monasca_transform.component.usage.fetch_quantity import FetchQuantity
from monasca_transform.component.usage import UsageComponent

from monasca_transform.transform.transform_utils import InstanceUsageUtils

import json


class FetchQuantityUtilException(Exception):
    """Exception thrown when fetching quantity
    Attributes:
    value: string representing the error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class FetchQuantityUtil(UsageComponent):

    @staticmethod
    def _supported_fetch_quantity_util_operations():
        # The results of "sum", "max", and "min" don't make sense and/or
        # may be misleading (the latter two due to the metrics which are
        # used as input to the utilization calculation potentially not
        # being from the same time period...e.g., one being from the
        # beginning of the streaming intervale and the other being from
        # the end.
        return ["avg", "latest", "oldest"]

    @staticmethod
    def _is_valid_fetch_quantity_util_operation(operation):
        """return true if its a valid fetch operation"""
        if operation in FetchQuantityUtil.\
                _supported_fetch_quantity_util_operations():
            return True
        else:
            return False

    @staticmethod
    def _format_quantity_util(row):
        """calculate the utilized quantity based on idle percentage
        quantity and convert to instance usage format
        """
        #
        tenant_id = getattr(row, "tenant_id", "all")
        resource_uuid = getattr(row, "resource_uuid",
                                Component.DEFAULT_UNAVAILABLE_VALUE)
        user_id = getattr(row, "user_id",
                          Component.DEFAULT_UNAVAILABLE_VALUE)
        geolocation = getattr(row, "geolocation",
                              Component.DEFAULT_UNAVAILABLE_VALUE)
        region = getattr(row, "region", Component.DEFAULT_UNAVAILABLE_VALUE)
        zone = getattr(row, "zone", Component.DEFAULT_UNAVAILABLE_VALUE)
        host = getattr(row, "host", "all")

        usage_date = getattr(row, "usage_date",
                             Component.DEFAULT_UNAVAILABLE_VALUE)
        usage_hour = getattr(row, "usage_hour",
                             Component.DEFAULT_UNAVAILABLE_VALUE)
        usage_minute = getattr(row, "usage_minute",
                               Component.DEFAULT_UNAVAILABLE_VALUE)

        aggregated_metric_name = getattr(row, "aggregated_metric_name",
                                         Component.DEFAULT_UNAVAILABLE_VALUE)

        # get utilized quantity
        quantity = row.utilized_quantity

        firstrecord_timestamp_unix = \
            getattr(row, "firstrecord_timestamp_unix",
                    Component.DEFAULT_UNAVAILABLE_VALUE)
        firstrecord_timestamp_string = \
            getattr(row, "firstrecord_timestamp_string",
                    Component.DEFAULT_UNAVAILABLE_VALUE)
        lastrecord_timestamp_unix = \
            getattr(row, "lastrecord_timestamp_unix",
                    Component.DEFAULT_UNAVAILABLE_VALUE)
        lastrecord_timestamp_string = \
            getattr(row, "lastrecord_timestamp_string",
                    Component.DEFAULT_UNAVAILABLE_VALUE)
        record_count = getattr(row, "record_count",
                               Component.DEFAULT_UNAVAILABLE_VALUE)

        # service id
        service_group = Component.DEFAULT_UNAVAILABLE_VALUE
        service_id = Component.DEFAULT_UNAVAILABLE_VALUE

        # aggregation period
        aggregation_period = Component.DEFAULT_UNAVAILABLE_VALUE

        instance_usage_dict = {"tenant_id": tenant_id, "user_id": user_id,
                               "resource_uuid": resource_uuid,
                               "geolocation": geolocation, "region": region,
                               "zone": zone, "host": host,
                               "aggregated_metric_name":
                                   aggregated_metric_name,
                               "quantity": quantity,
                               "firstrecord_timestamp_unix":
                                   firstrecord_timestamp_unix,
                               "firstrecord_timestamp_string":
                                   firstrecord_timestamp_string,
                               "lastrecord_timestamp_unix":
                                   lastrecord_timestamp_unix,
                               "lastrecord_timestamp_string":
                                   lastrecord_timestamp_string,
                               "record_count": record_count,
                               "service_group": service_group,
                               "service_id": service_id,
                               "usage_date": usage_date,
                               "usage_hour": usage_hour,
                               "usage_minute": usage_minute,
                               "aggregation_period": aggregation_period}

        instance_usage_data_json = json.dumps(instance_usage_dict)

        return instance_usage_data_json

    @staticmethod
    def usage(transform_context, record_store_df):
        """component which groups together record store records by
        provided group by columns list, sorts within the group by event
        timestamp field, applies group stats udf and returns the latest
        quantity as a instance usage dataframe

        This component does groups records by event_type (a.k.a metric name)
        and expects two kinds of records in record_store data
        total quantity records - the total available quantity
        e.g. cpu.total_logical_cores
        idle perc records - percentage that is idle
        e.g. cpu.idle_perc

        To calculate the utilized quantity  this component uses following
        formula:

        utilized quantity = (100 - idle_perc) * total_quantity / 100

        """

        sql_context = SQLContext.getOrCreate(record_store_df.rdd.context)

        transform_spec_df = transform_context.transform_spec_df_info

        # get rollup operation (sum, max, avg, min)
        agg_params = transform_spec_df.select(
            "aggregation_params_map.usage_fetch_operation"). \
            collect()[0].asDict()
        usage_fetch_operation = agg_params["usage_fetch_operation"]

        # check if operation is valid
        if not FetchQuantityUtil. \
                _is_valid_fetch_quantity_util_operation(usage_fetch_operation):
            raise FetchQuantityUtilException(
                "Operation %s is not supported" % usage_fetch_operation)

        # get the quantities for idle perc and quantity
        instance_usage_df = FetchQuantity().usage(
            transform_context, record_store_df)

        # get aggregation period for instance usage dataframe
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_period").collect()[0].asDict()
        aggregation_period = agg_params["aggregation_period"]
        group_by_period_list = ComponentUtils.\
            _get_instance_group_by_period_list(aggregation_period)

        # get what we want to group by
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_group_by_list").\
            collect()[0].asDict()
        aggregation_group_by_list = agg_params["aggregation_group_by_list"]

        # group by columns list
        group_by_columns_list = group_by_period_list + \
            aggregation_group_by_list

        # get quantity event type
        agg_params = transform_spec_df.select(
            "aggregation_params_map.usage_fetch_util_quantity_event_type").\
            collect()[0].asDict()
        usage_fetch_util_quantity_event_type = \
            agg_params["usage_fetch_util_quantity_event_type"]

        # check if driver parameter is provided
        if usage_fetch_util_quantity_event_type is None or \
                usage_fetch_util_quantity_event_type == "":
            raise FetchQuantityUtilException(
                "Driver parameter  '%s' is missing"
                % "usage_fetch_util_quantity_event_type")

        # get idle perc event type
        agg_params = transform_spec_df.select(
            "aggregation_params_map.usage_fetch_util_idle_perc_event_type").\
            collect()[0].asDict()
        usage_fetch_util_idle_perc_event_type = \
            agg_params["usage_fetch_util_idle_perc_event_type"]

        # check if driver parameter is provided
        if usage_fetch_util_idle_perc_event_type is None or \
                usage_fetch_util_idle_perc_event_type == "":
            raise FetchQuantityUtilException(
                "Driver parameter  '%s' is missing"
                % "usage_fetch_util_idle_perc_event_type")

        # get quantity records dataframe
        event_type_quantity_clause = "processing_meta.event_type='%s'" \
            % usage_fetch_util_quantity_event_type
        quantity_df = instance_usage_df.select('*').where(
            event_type_quantity_clause).alias("quantity_df_alias")

        # get idle perc records dataframe
        event_type_idle_perc_clause = "processing_meta.event_type='%s'" \
            % usage_fetch_util_idle_perc_event_type
        idle_perc_df = instance_usage_df.select('*').where(
            event_type_idle_perc_clause).alias("idle_perc_df_alias")

        # join quantity records with idle perc records
        # create a join condition without the event_type
        cond = [item for item in group_by_columns_list
                if item != 'event_type']
        quant_idle_perc_df = quantity_df.join(idle_perc_df, cond, 'left')

        #
        # Find utilized quantity based on idle percentage
        #
        # utilized quantity = (100 - idle_perc) * total_quantity / 100
        #
        quant_idle_perc_calc_df = quant_idle_perc_df.select(
            col("quantity_df_alias.*"),
            when(col("idle_perc_df_alias.quantity") != 0.0,
                 (100.0 - col(
                     "idle_perc_df_alias.quantity")) * col(
                     "quantity_df_alias.quantity") / 100.0)
            .otherwise(col("quantity_df_alias.quantity"))
            .alias("utilized_quantity"),

            col("quantity_df_alias.quantity")
            .alias("total_quantity"),

            col("idle_perc_df_alias.quantity")
            .alias("idle_perc"))

        instance_usage_json_rdd = \
            quant_idle_perc_calc_df.rdd.map(
                FetchQuantityUtil._format_quantity_util)

        instance_usage_df = \
            InstanceUsageUtils.create_df_from_json_rdd(sql_context,
                                                       instance_usage_json_rdd)

        return instance_usage_df
