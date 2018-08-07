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

from monasca_transform.component import Component


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
                   "usage_date", "usage_hour", "usage_minute",
                   "aggregation_period"]

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

        columns_struct_fields.append(StructField("extra_data_map",
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
        instance_usage_schema_df = sql_context.read.json(jsonrdd, schema)
        return instance_usage_schema_df

    @staticmethod
    def prepare_instance_usage_group_by_list(group_by_list):
        """Prepare group by list.

        If the group by list contains any instances of "dimensions#", "meta#" or "value_meta#" then
        prepend the column value by "extra_data_map." since those columns are available in
        extra_data_map column.

        """
        return [InstanceUsageUtils.prepare_group_by_item(item) for item in group_by_list]

    @staticmethod
    def prepare_group_by_item(item):
        """Prepare group by list item.

        Convert replaces any special "dimensions#", "meta#" or "value_meta#" occurrences into
        spark sql syntax to retrieve data from extra_data_map column.
        """
        if (item.startswith("dimensions#") or
                item.startswith("meta#") or
                item.startswith("value_meta#")):
            return ".".join(("extra_data_map", item))
        else:
            return item

    @staticmethod
    def prepare_extra_data_map(extra_data_map):
        """Prepare extra data map.

        Replace any occurances of "dimensions." or "meta." or "value_meta."
        to "dimensions#", "meta#" or "value_meta#" in extra_data_map.

        """
        prepared_extra_data_map = {}
        for column_name in list(extra_data_map):
            column_value = extra_data_map[column_name]
            if column_name.startswith("dimensions."):
                column_name = column_name.replace("dimensions.", "dimensions#")
            elif column_name.startswith("meta."):
                column_name = column_name.replace("meta.", "meta#")
            elif column_name.startswith("value_meta."):
                column_name = column_name.replace("value_meta.", "value_meta#")
            elif column_name.startswith("extra_data_map."):
                column_name = column_name.replace("extra_data_map.", "")
            prepared_extra_data_map[column_name] = column_value
        return prepared_extra_data_map

    @staticmethod
    def grouped_data_to_map(row, group_by_columns_list):
        """Iterate through group by column values from grouped data set and extract any values.

        Return a dictionary which contains original group by columns name and value pairs, if they
        are available from the grouped data set.

        """
        extra_data_map = getattr(row, "extra_data_map", {})
        # add group by fields data to extra data map
        for column_name in group_by_columns_list:
            column_value = getattr(row, column_name, Component.
                                   DEFAULT_UNAVAILABLE_VALUE)
            if (column_value == Component.DEFAULT_UNAVAILABLE_VALUE
                    and (column_name.startswith("dimensions.")
                         or column_name.startswith("meta.")
                         or column_name.startswith("value_meta.")
                         or column_name.startswith("extra_data_map."))):
                split_column_name = column_name.split(".", 1)[-1]
                column_value = getattr(row, split_column_name, Component.
                                       DEFAULT_UNAVAILABLE_VALUE)
            extra_data_map[column_name] = column_value
        return extra_data_map

    @staticmethod
    def extract_dimensions(instance_usage_dict, dimension_list):
        """Extract dimensions from instance usage.

        """
        dimensions_part = {}
        # extra_data_map
        extra_data_map = instance_usage_dict.get("extra_data_map", {})

        for dim in dimension_list:
            value = instance_usage_dict.get(dim)
            if value is None:
                # lookup for value in extra_data_map
                if len(list(extra_data_map)) > 0:
                    value = extra_data_map.get(dim, "all")
            if dim.startswith("dimensions#"):
                dim = dim.replace("dimensions#", "")
            elif dim.startswith("meta#"):
                dim = dim.replace("meta#", "")
            elif dim.startswith("value_meta#"):
                dim = dim.replace("value_meta#", "")
            dimensions_part[dim] = value

        return dimensions_part


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
                   "host", "project_id",
                   "event_date", "event_hour", "event_minute",
                   "event_second", "metric_group", "metric_id"]

        columns_struct_fields = [StructField(field_name, StringType(), True)
                                 for field_name in columns]

        # Add a column for a non-string fields
        columns_struct_fields.insert(0,
                                     StructField("event_timestamp_unix",
                                                 DoubleType(), True))
        columns_struct_fields.insert(0,
                                     StructField("event_quantity",
                                                 DoubleType(), True))

        # map to metric meta
        columns_struct_fields.append(StructField("meta",
                                                 MapType(StringType(),
                                                         StringType(),
                                                         True),
                                                 True))
        # map to dimensions
        columns_struct_fields.append(StructField("dimensions",
                                                 MapType(StringType(),
                                                         StringType(),
                                                         True),
                                                 True))
        # map to value_meta
        columns_struct_fields.append(StructField("value_meta",
                                                 MapType(StringType(),
                                                         StringType(),
                                                         True),
                                                 True))

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

    @staticmethod
    def prepare_recordstore_group_by_list(group_by_list):
        """Prepare record store group by list.

        If the group by list contains any instances of "dimensions#", "meta#" or "value#meta" then
        convert into proper dotted notation, since original raw "dimensions", "meta" and
        "value_meta" are available in record_store data.

        """
        return [RecordStoreUtils.prepare_group_by_item(item) for item in group_by_list]

    @staticmethod
    def prepare_group_by_item(item):
        """Prepare record store item for group by.

        Convert replaces any special "dimensions#", "meta#" or "value#meta" occurrences into
        "dimensions.", "meta." and value_meta.".

        """
        if item.startswith("dimensions#"):
            item = item.replace("dimensions#", "dimensions.")
        elif item.startswith("meta#"):
            item = item.replace("meta#", "meta.")
        elif item.startswith("value_meta#"):
            item = item.replace("value_meta#", "value_meta.")
        return item


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

        metric_struct_field = StructField(
            "metric",
            StructType([StructField("dimensions",
                                    MapType(StringType(),
                                            StringType(),
                                            True),
                                    True),
                        StructField("value_meta",
                                    MapType(StringType(),
                                            StringType(),
                                            True),
                                    True),
                        StructField("name", StringType(), True),
                        StructField("timestamp", StringType(), True),
                        StructField("value", StringType(), True)]), True)

        meta_struct_field = StructField("meta",
                                        MapType(StringType(),
                                                StringType(),
                                                True),
                                        True)

        creation_time_struct_field = StructField("creation_time",
                                                 StringType(), True)

        schema = StructType([creation_time_struct_field,
                             meta_struct_field, metric_struct_field])
        return schema

    @staticmethod
    def create_mon_metrics_df_from_json_rdd(sql_context, jsonrdd):
        """create mon metrics df from json rdd."""
        schema = MonMetricUtils._get_mon_metric_json_schema()
        mon_metrics_df = sql_context.read.json(jsonrdd, schema)
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
                             metric_id_list, required_raw_fields_list])

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

    @staticmethod
    def prepare_required_raw_fields_list(group_by_list):
        """Prepare required fields list.

        If the group by list contains any instances of "dimensions#field", "meta#field" or
        "value_meta#field" then convert them into metric.dimensions["field"] syntax.

        """
        return [PreTransformSpecsUtils.prepare_required_raw_item(item) for item in group_by_list]

    @staticmethod
    def prepare_required_raw_item(item):
        """Prepare required field item.

        Convert replaces any special "dimensions#", "meta#" or "value_meta" occurrences into
        spark rdd syntax to fetch field value.

        """
        if item.startswith("dimensions#"):
            field_name = item.replace("dimensions#", "")
            return "metric.dimensions['%s']" % field_name
        elif item.startswith("meta#"):
            field_name = item.replace("meta#", "")
            return "meta['%s']" % field_name
        elif item.startswith("value_meta#"):
            field_name = item.replace("value_meta#", "")
            return "metric.value_meta['%s']" % field_name
        else:
            return item


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
