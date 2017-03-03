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
import datetime

from pyspark.sql import functions
from pyspark.sql import SQLContext


from monasca_transform.component import Component
from monasca_transform.component.component_utils import ComponentUtils
from monasca_transform.component.usage import UsageComponent
from monasca_transform.transform.grouping.group_sort_by_timestamp \
    import GroupSortbyTimestamp
from monasca_transform.transform.grouping.group_sort_by_timestamp_partition \
    import GroupSortbyTimestampPartition
from monasca_transform.transform.transform_utils import InstanceUsageUtils

import json


class FetchQuantityException(Exception):
    """Exception thrown when fetching quantity
    Attributes:
    value: string representing the error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


GroupedDataWithOperation = namedtuple("GroupedDataWithOperation",
                                      ["grouped_data",
                                       "usage_fetch_operation"])


class GroupedDataWithOperation(GroupedDataWithOperation):
    """A tuple which is a wrapper containing record store data
    and the usage operation

    namdetuple contains:

    grouped_data - grouped record store data
    usage_fetch_operation - operation
    """


class FetchQuantity(UsageComponent):

    @staticmethod
    def _supported_fetch_operations():
        return ["sum", "max", "min", "avg", "latest", "oldest"]

    @staticmethod
    def _is_valid_fetch_operation(operation):
        """return true if its a valid fetch operation"""
        if operation in FetchQuantity._supported_fetch_operations():
            return True
        else:
            return False

    @staticmethod
    def _get_latest_oldest_quantity(grouping_results_with_operation):
        """get quantity for each group by performing the requested
        usage operation and return a instance usage data.
        """

        # row
        grouping_results = grouping_results_with_operation.\
            grouped_data

        # usage fetch operation
        usage_fetch_operation = grouping_results_with_operation.\
            usage_fetch_operation

        group_by_dict = grouping_results.grouping_key_dict

        #
        tenant_id = group_by_dict.get("tenant_id",
                                      Component.DEFAULT_UNAVAILABLE_VALUE)
        resource_uuid = group_by_dict.get("resource_uuid",
                                          Component.DEFAULT_UNAVAILABLE_VALUE)
        user_id = group_by_dict.get("user_id",
                                    Component.DEFAULT_UNAVAILABLE_VALUE)
        namespace = group_by_dict.get("namespace",
                                      Component.DEFAULT_UNAVAILABLE_VALUE)
        pod_name = group_by_dict.get("pod_name",
                                     Component.DEFAULT_UNAVAILABLE_VALUE)
        app = group_by_dict.get("app",
                                Component.DEFAULT_UNAVAILABLE_VALUE)
        container_name = group_by_dict.get("container_name",
                                           Component.DEFAULT_UNAVAILABLE_VALUE)
        interface = group_by_dict.get("interface",
                                      Component.DEFAULT_UNAVAILABLE_VALUE)
        deployment = group_by_dict.get("deployment",
                                       Component.DEFAULT_UNAVAILABLE_VALUE)
        daemon_set = group_by_dict.get("daemon_set",
                                       Component.DEFAULT_UNAVAILABLE_VALUE)

        geolocation = group_by_dict.get("geolocation",
                                        Component.DEFAULT_UNAVAILABLE_VALUE)
        region = group_by_dict.get("region",
                                   Component.DEFAULT_UNAVAILABLE_VALUE)
        zone = group_by_dict.get("zone", Component.DEFAULT_UNAVAILABLE_VALUE)
        host = group_by_dict.get("host", Component.DEFAULT_UNAVAILABLE_VALUE)

        usage_date = group_by_dict.get("event_date",
                                       Component.DEFAULT_UNAVAILABLE_VALUE)
        usage_hour = group_by_dict.get("event_hour",
                                       Component.DEFAULT_UNAVAILABLE_VALUE)
        usage_minute = group_by_dict.get("event_minute",
                                         Component.DEFAULT_UNAVAILABLE_VALUE)

        aggregated_metric_name = group_by_dict.get(
            "aggregated_metric_name", Component.DEFAULT_UNAVAILABLE_VALUE)

        # stats
        agg_stats = grouping_results.results

        # get quantity for this host
        quantity = None
        if (usage_fetch_operation == "latest"):
            quantity = agg_stats["lastrecord_quantity"]
        elif usage_fetch_operation == "oldest":
            quantity = agg_stats["firstrecord_quantity"]

        firstrecord_timestamp_unix = agg_stats["firstrecord_timestamp_unix"]
        firstrecord_timestamp_string = \
            agg_stats["firstrecord_timestamp_string"]
        lastrecord_timestamp_unix = agg_stats["lastrecord_timestamp_unix"]
        lastrecord_timestamp_string = agg_stats["lastrecord_timestamp_string"]
        record_count = agg_stats["record_count"]

        # service id
        service_group = Component.DEFAULT_UNAVAILABLE_VALUE
        service_id = Component.DEFAULT_UNAVAILABLE_VALUE

        # aggregation period
        aggregation_period = Component.DEFAULT_UNAVAILABLE_VALUE

        # event type
        event_type = group_by_dict.get("event_type",
                                       Component.DEFAULT_UNAVAILABLE_VALUE)

        instance_usage_dict = {"tenant_id": tenant_id, "user_id": user_id,
                               "resource_uuid": resource_uuid,
                               "namespace": namespace,
                               "pod_name": pod_name,
                               "app": app,
                               "container_name": container_name,
                               "interface": interface,
                               "deployment": deployment,
                               "daemon_set": daemon_set,
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
                               "aggregation_period": aggregation_period,
                               "processing_meta": {"event_type": event_type}
                               }
        instance_usage_data_json = json.dumps(instance_usage_dict)

        return instance_usage_data_json

    @staticmethod
    def _get_quantity(grouped_record_with_operation):

        # row
        row = grouped_record_with_operation.grouped_data

        # usage fetch operation
        usage_fetch_operation = grouped_record_with_operation.\
            usage_fetch_operation

        # first record timestamp # FIXME: beginning of epoch?
        earliest_record_timestamp_unix = getattr(
            row, "min(event_timestamp_unix_for_min)",
            Component.DEFAULT_UNAVAILABLE_VALUE)
        earliest_record_timestamp_string = \
            datetime.datetime.utcfromtimestamp(
                earliest_record_timestamp_unix).strftime(
                '%Y-%m-%d %H:%M:%S')

        # last record_timestamp # FIXME: beginning of epoch?
        latest_record_timestamp_unix = getattr(
            row, "max(event_timestamp_unix_for_max)",
            Component.DEFAULT_UNAVAILABLE_VALUE)
        latest_record_timestamp_string = \
            datetime.datetime.utcfromtimestamp(
                latest_record_timestamp_unix).strftime('%Y-%m-%d %H:%M:%S')

        # record count
        record_count = getattr(row, "count(event_timestamp_unix)", 0.0)

        # quantity
        # get expression that will be used to select quantity
        # from rolled up data
        select_quant_str = "".join((usage_fetch_operation, "(event_quantity)"))
        quantity = getattr(row, select_quant_str, 0.0)

        #  create a new instance usage dict
        instance_usage_dict = {"tenant_id": getattr(row, "tenant_id",
                                                    Component.
                                                    DEFAULT_UNAVAILABLE_VALUE),
                               "user_id":
                                   getattr(row, "user_id",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "resource_uuid":
                                   getattr(row, "resource_uuid",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "geolocation":
                                   getattr(row, "geolocation",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "region":
                                   getattr(row, "region",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "zone":
                                   getattr(row, "zone",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "host":
                                   getattr(row, "host",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "project_id":
                                   getattr(row, "tenant_id",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "namespace":
                                   getattr(row, "namespace",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "pod_name":
                                   getattr(row, "pod_name",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "app":
                                   getattr(row, "app",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "container_name":
                                   getattr(row, "container_name",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "interface":
                                   getattr(row, "interface",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "deployment":
                                   getattr(row, "deployment",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "daemon_set":
                                   getattr(row, "daemon_set",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "aggregated_metric_name":
                                   getattr(row, "aggregated_metric_name",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
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
                                   getattr(row, "service_group",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "service_id":
                                   getattr(row, "service_id",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "usage_date":
                                   getattr(row, "event_date",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "usage_hour":
                                   getattr(row, "event_hour",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "usage_minute":
                                   getattr(row, "event_minute",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "aggregation_period":
                                   getattr(row, "aggregation_period",
                                           Component.
                                           DEFAULT_UNAVAILABLE_VALUE),
                               "processing_meta": {"event_type": getattr(
                                   row, "event_type",
                                   Component.DEFAULT_UNAVAILABLE_VALUE)}
                               }

        instance_usage_data_json = json.dumps(instance_usage_dict)
        return instance_usage_data_json

    @staticmethod
    def usage(transform_context, record_store_df):
        """component which groups together record store records by
        provided group by columns list , sorts within the group by event
        timestamp field, applies group stats udf and returns the latest
        quantity as a instance usage dataframe
        """
        transform_spec_df = transform_context.transform_spec_df_info

        # get rollup operation (sum, max, avg, min)
        agg_params = transform_spec_df.select(
            "aggregation_params_map.usage_fetch_operation").\
            collect()[0].asDict()
        usage_fetch_operation = agg_params["usage_fetch_operation"]

        instance_usage_df = FetchQuantity.usage_by_operation(
            transform_context, record_store_df, usage_fetch_operation)

        return instance_usage_df

    @staticmethod
    def usage_by_operation(transform_context, record_store_df,
                           usage_fetch_operation):
        """component which groups together record store records by
        provided group by columns list , sorts within the group by event
        timestamp field, applies group stats udf and returns the latest
        quantity as a instance usage dataframe
        """
        transform_spec_df = transform_context.transform_spec_df_info

        # check if operation is valid
        if not FetchQuantity. \
                _is_valid_fetch_operation(usage_fetch_operation):
            raise FetchQuantityException(
                "Operation %s is not supported" % usage_fetch_operation)

        # get aggregation period
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_period").collect()[0].asDict()
        aggregation_period = agg_params["aggregation_period"]
        group_by_period_list = ComponentUtils._get_group_by_period_list(
            aggregation_period)

        # retrieve filter specifications
        agg_params = transform_spec_df.select(
            "aggregation_params_map.filter_by_list"). \
            collect()[0].asDict()
        filter_by_list = \
            agg_params["filter_by_list"]

        # if filter(s) have been specified, apply them one at a time
        if filter_by_list:
            for filter_element in filter_by_list:
                field_to_filter = filter_element["field_to_filter"]
                filter_expression = filter_element["filter_expression"]
                filter_operation = filter_element["filter_operation"]

                if (field_to_filter and
                        filter_expression and
                        filter_operation and
                        (filter_operation == "include" or
                         filter_operation == "exclude")):
                    if filter_operation == "include":
                        match = True
                    else:
                        match = False
                    # apply the specified filter to the record store
                    record_store_df = record_store_df.where(
                        functions.col(str(field_to_filter)).rlike(
                            str(filter_expression)) == match)
                else:
                    raise FetchQuantityException(
                        "Encountered invalid filter details: "
                        "field to filter = %s, filter expression = %s, "
                        "filter operation = %s.  All values must be "
                        "supplied and filter operation must be either "
                        "'include' or 'exclude'." % (field_to_filter,
                                                     filter_expression,
                                                     filter_operation))

        # get what we want to group by
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_group_by_list"). \
            collect()[0].asDict()
        aggregation_group_by_list = agg_params["aggregation_group_by_list"]

        # group by columns list
        group_by_columns_list = group_by_period_list + \
            aggregation_group_by_list

        instance_usage_json_rdd = None
        if (usage_fetch_operation == "latest" or
                usage_fetch_operation == "oldest"):

            grouped_rows_rdd = None

            # FIXME:
            # select group by method
            IS_GROUP_BY_PARTITION = False

            if (IS_GROUP_BY_PARTITION):
                # GroupSortbyTimestampPartition is a more scalable
                # since it creates groups using repartitioning and sorting
                # but is disabled

                # number of groups should be more than what is expected
                # this might be hard to guess. Setting this to a very
                # high number is adversely affecting performance
                num_of_groups = 100
                grouped_rows_rdd = \
                    GroupSortbyTimestampPartition. \
                    fetch_group_latest_oldest_quantity(
                        record_store_df, transform_spec_df,
                        group_by_columns_list,
                        num_of_groups)
            else:
                # group using key-value pair RDD's groupByKey()
                grouped_rows_rdd = \
                    GroupSortbyTimestamp. \
                    fetch_group_latest_oldest_quantity(
                        record_store_df, transform_spec_df,
                        group_by_columns_list)

            grouped_data_rdd_with_operation = grouped_rows_rdd.map(
                lambda x:
                GroupedDataWithOperation(x,
                                         str(usage_fetch_operation)))

            instance_usage_json_rdd = \
                grouped_data_rdd_with_operation.map(
                    FetchQuantity._get_latest_oldest_quantity)
        else:

            record_store_df_int = \
                record_store_df.select(
                    record_store_df.event_timestamp_unix.alias(
                        "event_timestamp_unix_for_min"),
                    record_store_df.event_timestamp_unix.alias(
                        "event_timestamp_unix_for_max"),
                    "*")

            # for standard sum, max, min, avg operations on grouped data
            agg_operations_map = {
                "event_quantity": str(usage_fetch_operation),
                "event_timestamp_unix_for_min": "min",
                "event_timestamp_unix_for_max": "max",
                "event_timestamp_unix": "count"}
            # do a group by
            grouped_data = record_store_df_int.groupBy(*group_by_columns_list)
            grouped_record_store_df = grouped_data.agg(agg_operations_map)

            grouped_data_rdd_with_operation = grouped_record_store_df.map(
                lambda x:
                GroupedDataWithOperation(x,
                                         str(usage_fetch_operation)))

            instance_usage_json_rdd = grouped_data_rdd_with_operation.map(
                FetchQuantity._get_quantity)

        sql_context = SQLContext.getOrCreate(record_store_df.rdd.context)
        instance_usage_df = \
            InstanceUsageUtils.create_df_from_json_rdd(sql_context,
                                                       instance_usage_json_rdd)
        return instance_usage_df
