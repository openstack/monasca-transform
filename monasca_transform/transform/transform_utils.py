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
from pyspark.sql.types import ArrayType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import MapType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


class TransformUtils(object):
    """utility methods for different kinds of data."""

    @staticmethod
    def _rdd_to_df(rdd, schema):
        """convert rdd to dataframe using schema."""
        spark_context = rdd.context
        sql_context = SQLContext.getOrCreate(spark_context)
        if schema is None:
            df = sql_context.createDataFrame(rdd)
        else:
            df = sql_context.createDataFrame(rdd, schema)
        return df


class InstanceUsageUtils(TransformUtils):
    """utility methods to transform instance usage data."""
    @staticmethod
    def _get_instance_usage_schema():
        """get instance usage schema."""

        # Initialize columns for all string fields
        columns = ["tenant_id", "user_id", "resource_uuid",
                   "geolocation", "region", "zone", "host", "project_id",
                   "aggregated_metric_name", "firstrecord_timestamp_string",
                   "lastrecord_timestamp_string",
                   "service_group", "service_id",
                   "usage_date", "usage_hour", "usage_minute",
                   "aggregation_period", "namespace", "pod_name", "app",
                   "container_name", "interface", "deployment", "daemon_set"]

        columns_struct_fields = [StructField(field_name, StringType(), True)
                                 for field_name in columns]

        # Add columns for non-string fields
        columns_struct_fields.append(StructField("firstrecord_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("lastrecord_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("quantity",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("record_count",
                                                 DoubleType(), True))

        columns_struct_fields.append(StructField("processing_meta",
                                                 MapType(StringType(),
                                                         StringType(),
                                                         True),
                                                 True))
        schema = StructType(columns_struct_fields)

        return schema

    @staticmethod
    def create_df_from_json_rdd(sql_context, jsonrdd):
        """create instance usage df from json rdd."""
        schema = InstanceUsageUtils._get_instance_usage_schema()
        instance_usage_schema_df = sql_context.jsonRDD(jsonrdd, schema)
        return instance_usage_schema_df


class RecordStoreUtils(TransformUtils):
    """utility methods to transform record store data."""
    @staticmethod
    def _get_record_store_df_schema():
        """get instance usage schema."""

        columns = ["event_timestamp_string",
                   "event_type", "event_quantity_name",
                   "event_status", "event_version",
                   "record_type", "resource_uuid", "tenant_id",
                   "user_id", "region", "zone",
                   "host", "project_id", "service_group", "service_id",
                   "event_date", "event_hour", "event_minute",
                   "event_second", "metric_group", "metric_id",
                   "namespace", "pod_name", "app", "container_name",
                   "interface", "deployment", "daemon_set"]

        columns_struct_fields = [StructField(field_name, StringType(), True)
                                 for field_name in columns]

        # Add a column for a non-string fields
        columns_struct_fields.insert(0,
                                     StructField("event_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.insert(0,
                                     StructField("event_quantity",
                                                 DoubleType(), True))

        schema = StructType(columns_struct_fields)

        return schema

    @staticmethod
    def recordstore_rdd_to_df(record_store_rdd):
        """convert record store rdd to a dataframe."""
        schema = RecordStoreUtils._get_record_store_df_schema()
        return TransformUtils._rdd_to_df(record_store_rdd, schema)

    @staticmethod
    def create_df_from_json(sql_context, jsonpath):
        """create a record store df from json file."""
        schema = RecordStoreUtils._get_record_store_df_schema()
        record_store_df = sql_context.read.json(jsonpath, schema)
        return record_store_df


class TransformSpecsUtils(TransformUtils):
    """utility methods to transform_specs."""

    @staticmethod
    def _get_transform_specs_df_schema():
        """get transform_specs df schema."""

        # FIXME: change when transform_specs df is finalized
        source = StructField("source", StringType(), True)
        usage = StructField("usage", StringType(), True)
        setters = StructField("setters", ArrayType(StringType(),
                                                   containsNull=False), True)
        insert = StructField("insert", ArrayType(StringType(),
                                                 containsNull=False), True)

        aggregation_params_map = \
            StructField("aggregation_params_map",
                        StructType([StructField("aggregation_period",
                                                StringType(), True),
                                    StructField("dimension_list",
                                                ArrayType(StringType(),
                                                          containsNull=False),
                                                True),
                                    StructField("aggregation_group_by_list",
                                                ArrayType(StringType(),
                                                          containsNull=False),
                                                True),
                                    StructField("usage_fetch_operation",
                                                StringType(),
                                                True),
                                    StructField("filter_by_list",
                                                ArrayType(MapType(StringType(),
                                                                  StringType(),
                                                                  True)
                                                          )
                                                ),
                                    StructField(
                                    "usage_fetch_util_quantity_event_type",
                                    StringType(),
                                    True),

                                    StructField(
                                    "usage_fetch_util_idle_perc_event_type",
                                    StringType(),
                                    True),

                                    StructField("setter_rollup_group_by_list",
                                                ArrayType(StringType(),
                                                          containsNull=False),
                                                True),
                                    StructField("setter_rollup_operation",
                                                StringType(), True),
                                    StructField("aggregated_metric_name",
                                                StringType(), True),
                                    StructField("pre_hourly_group_by_list",
                                                ArrayType(StringType(),
                                                          containsNull=False),
                                                True),
                                    StructField("pre_hourly_operation",
                                                StringType(), True),
                                    StructField("aggregation_pipeline",
                                                StructType([source, usage,
                                                            setters, insert]),
                                                True)
                                    ]), True)
        metric_id = StructField("metric_id", StringType(), True)

        schema = StructType([aggregation_params_map, metric_id])

        return schema

    @staticmethod
    def transform_specs_rdd_to_df(transform_specs_rdd):
        """convert transform_specs rdd to a dataframe."""
        schema = TransformSpecsUtils._get_transform_specs_df_schema()
        return TransformUtils._rdd_to_df(transform_specs_rdd, schema)

    @staticmethod
    def create_df_from_json(sql_context, jsonpath):
        """create a metric processing df from json file."""
        schema = TransformSpecsUtils._get_transform_specs_df_schema()
        transform_specs_df = sql_context.read.json(jsonpath, schema)
        return transform_specs_df


class MonMetricUtils(TransformUtils):
    """utility methods to transform raw metric."""

    @staticmethod
    def _get_mon_metric_json_schema():
        """get the schema of the incoming monasca metric."""
        dimensions = ["apache_host", "apache_port", "component",
                      "consumer_group", "device", "hostname",
                      "mode", "mount", "mount_point", "observer_host",
                      "process_name", "project_id", "resource_id", "service",
                      "test_type", "tenantId", "tenant_id", "topic", "url",
                      "state", "state_description", "instanceId",
                      "namespace", "pod_name", "app", "container_name",
                      "interface", "deployment", "daemon_set"]

        dimensions_struct_fields = [
            StructField(field_name, StringType(), True)
            for field_name in dimensions]

        value_meta = ["host"]

        value_meta_struct_fields = [
            StructField(field_name, StringType(), True)
            for field_name in value_meta]

        metric_struct_field = StructField(
            "metric",
            StructType([StructField("dimensions",
                                    StructType(dimensions_struct_fields)),
                        StructField("value_meta",
                                    StructType(value_meta_struct_fields)),
                        StructField("name", StringType(), True),
                        StructField("timestamp", StringType(), True),
                        StructField("value", StringType(), True)]), True)

        meta_struct_field = StructField(
            "meta",
            StructType([StructField("timestamp", StringType(), True),
                        StructField("region", StringType(), True),
                        StructField("tenantId", StringType(), True),
                        StructField("userId", StringType(), True),
                        StructField("zone", StringType(), True),
                        StructField("geolocation", StringType(), True)]))

        creation_time_struct_field = StructField("creation_time",
                                                 StringType(), True)

        schema = StructType([creation_time_struct_field,
                             meta_struct_field, metric_struct_field])

        return schema

    @staticmethod
    def create_mon_metrics_df_from_json_rdd(sql_context, jsonrdd):
        """create mon metrics df from json rdd."""
        schema = MonMetricUtils._get_mon_metric_json_schema()
        mon_metrics_df = sql_context.jsonRDD(jsonrdd, schema)
        return mon_metrics_df


class PreTransformSpecsUtils(TransformUtils):
    """utility methods to transform pre_transform_specs"""

    @staticmethod
    def _get_pre_transform_specs_df_schema():
        """get pre_transform_specs df schema."""

        # FIXME: change when pre_transform_specs df is finalized

        event_type = StructField("event_type", StringType(), True)

        metric_id_list = StructField("metric_id_list",
                                     ArrayType(StringType(),
                                               containsNull=False),
                                     True)
        required_raw_fields_list = StructField("required_raw_fields_list",
                                               ArrayType(StringType(),
                                                         containsNull=False),
                                               True)
        service_id = StructField("service_id", StringType(), True)

        event_processing_params = \
            StructField("event_processing_params",
                        StructType([StructField("set_default_zone_to",
                                                StringType(), True),
                                    StructField("set_default_geolocation_to",
                                                StringType(), True),
                                    StructField("set_default_region_to",
                                                StringType(), True),
                                    ]), True)

        schema = StructType([event_processing_params, event_type,
                             metric_id_list, required_raw_fields_list,
                             service_id])

        return schema

    @staticmethod
    def pre_transform_specs_rdd_to_df(pre_transform_specs_rdd):
        """convert pre_transform_specs processing rdd to a dataframe."""
        schema = PreTransformSpecsUtils._get_pre_transform_specs_df_schema()
        return TransformUtils._rdd_to_df(pre_transform_specs_rdd, schema)

    @staticmethod
    def create_df_from_json(sql_context, jsonpath):
        """create a pre_transform_specs df from json file."""
        schema = PreTransformSpecsUtils._get_pre_transform_specs_df_schema()
        pre_transform_specs_df = sql_context.read.json(jsonpath, schema)
        return pre_transform_specs_df


class GroupingResultsUtils(TransformUtils):
    """utility methods to transform record store data."""
    @staticmethod
    def _get_grouping_results_df_schema(group_by_column_list):
        """get grouping results schema."""

        group_by_field_list = [StructField(field_name, StringType(), True)
                               for field_name in group_by_column_list]

        # Initialize columns for string fields
        columns = ["firstrecord_timestamp_string",
                   "lastrecord_timestamp_string"]

        columns_struct_fields = [StructField(field_name, StringType(), True)
                                 for field_name in columns]

        # Add columns for non-string fields
        columns_struct_fields.append(StructField("firstrecord_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("lastrecord_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("firstrecord_quantity",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("lastrecord_quantity",
                                                 DoubleType(), True))
        columns_struct_fields.append(StructField("record_count",
                                                 DoubleType(), True))

        instance_usage_schema_part = StructType(columns_struct_fields)

        grouping_results = \
            StructType([StructField("grouping_key",
                                    StringType(), True),
                        StructField("results",
                                    instance_usage_schema_part,
                                    True),
                        StructField("grouping_key_dict",
                                    StructType(group_by_field_list))])

        # schema = \
        #     StructType([StructField("GroupingResults", grouping_results)])
        return grouping_results

    @staticmethod
    def grouping_results_rdd_to_df(grouping_results_rdd, group_by_list):
        """convert record store rdd to a dataframe."""
        schema = GroupingResultsUtils._get_grouping_results_df_schema(
            group_by_list)
        return TransformUtils._rdd_to_df(grouping_results_rdd, schema)
