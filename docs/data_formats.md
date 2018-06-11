Team and repository tags
========================

[![Team and repositorytags](https://governance.openstack.org/badges/monasca-transform.svg)](https://governance.openstack.org/reference/tags/index.html)

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Monasca Transform Data Formats](#monasca-transform-data-formats)
  - [Record Store Data Format](#record-store-data-format)
  - [Instance Usage Data Format](#instance-usage-data-format)
  - [References](#references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Monasca Transform Data Formats

There are two data formats used by monasca transform. The following sections describes the schema
(Spark's DataFrame[1] schema) for the two formats.

Note: These are internal formats used by Monasca Transform when aggregating data. If you are a user
who wants to create new aggregation pipeline using an existing framework, you don't need to know or
care about these two formats.

As a developer, if you want to write new aggregation components then you might have to know how to
enhance the record store data format or instance usage data format with additional fields that you
may need or to write new aggregation components that aggregate data from the additional fields.

**Source Metric**

This is an example monasca metric. Monasca metric is transformed into `record_store` data format and
later transformed/aggregated using re-usable generic aggregation components, to derive
'instance_usage` data format.

Example of a monasca metric:

```
{"metric":{"timestamp":1523323485360.6650390625,
           "name":"monasca.collection_time_sec",
           "dimensions":{"hostname":"devstack",
                         "component":"monasca-agent",
                         "service":"monitoring"},
           "value":0.0340659618,
           "value_meta":null},
 "meta":{"region":"RegionOne","tenantId":"d6bece1bbeff47c1b8734cd4e544dc02"},
 "creation_time":1523323489}
```

## Record Store Data Format ##

Data Frame Schema:

| Column Name | Column Data Type | Description |
| :---------- | :--------------- | :---------- |
| event_quantity | `pyspark.sql.types.DoubleType` | mapped to `metric.value`|
| event_timestamp_unix | `pyspark.sql.types.DoubleType` | calculated as `metric.timestamp`/`1000` from source metric|
| event_timestamp_string | `pyspark.sql.types.StringType` | mapped to `metric.timestamp` from the source metric|
| event_type | `pyspark.sql.types.StringType` | placeholder for the future. mapped to `metric.name` from source metric|
| event_quantity_name | `pyspark.sql.types.StringType` | mapped to `metric.name` from source metric|
| resource_uuid | `pyspark.sql.types.StringType` | mapped to `metric.dimensions.instanceId` or `metric.dimensions.resource_id` from source metric  |
| tenant_id | `pyspark.sql.types.StringType` | mapped to `metric.dimensions.tenant_id` or `metric.dimensions.tenantid` or `metric.dimensions.project_id`  |
| user_id | `pyspark.sql.types.StringType` | mapped to `meta.userId` |
| region | `pyspark.sql.types.StringType` | placeholder of the future. mapped to `meta.region`, defaults to `event_processing_params.set_default_region_to` (`pre_transform_spec`) |
| zone | `pyspark.sql.types.StringType` | placeholder for the future. mapped to `meta.zone`, defaults to `event_processing_params.set_default_zone_to` (`pre_transform_spec`) |
| host | `pyspark.sql.types.StringType` | mapped to `metric.dimensions.hostname` or `metric.value_meta.host` |
| project_id | `pyspark.sql.types.StringType` | mapped to metric tenant_id |
| event_date | `pyspark.sql.types.StringType` | "YYYY-mm-dd". Extracted from `metric.timestamp` |
| event_hour | `pyspark.sql.types.StringType` | "HH". Extracted from `metric.timestamp` |
| event_minute | `pyspark.sql.types.StringType` | "MM". Extracted from `metric.timestamp` |
| event_second | `pyspark.sql.types.StringType` | "SS". Extracted from `metric.timestamp` |
| metric_group | `pyspark.sql.types.StringType` | identifier for transform spec group |
| metric_id | `pyspark.sql.types.StringType` | identifier for transform spec |

## Instance Usage Data Format ##

Data Frame Schema:

| Column Name | Column Data Type | Description |
| :---------- | :--------------- | :---------- |
| tenant_id | `pyspark.sql.types.StringType` | project_id, defaults to `NA` |
| user_id | `pyspark.sql.types.StringType` | user_id, defaults to `NA`|
| resource_uuid | `pyspark.sql.types.StringType` | resource_id, defaults to `NA`|
| geolocation | `pyspark.sql.types.StringType` | placeholder for future, defaults to `NA`|
| region | `pyspark.sql.types.StringType` | placeholder for future, defaults to `NA`|
| zone | `pyspark.sql.types.StringType` | placeholder for future, defaults to `NA`|
| host | `pyspark.sql.types.StringType` | compute hostname, defaults to `NA`|
| project_id | `pyspark.sql.types.StringType` | project_id, defaults to `NA`|
| aggregated_metric_name | `pyspark.sql.types.StringType` | aggregated metric name, defaults to `NA`|
| firstrecord_timestamp_string | `pyspark.sql.types.StringType` | timestamp of the first metric used to derive this aggregated metric|
| lastrecord_timestamp_string | `pyspark.sql.types.StringType` | timestamp of the last metric used to derive this aggregated metric|
| usage_date | `pyspark.sql.types.StringType` | "YYYY-mm-dd" date|
| usage_hour | `pyspark.sql.types.StringType` | "HH" hour|
| usage_minute | `pyspark.sql.types.StringType` | "MM" minute|
| aggregation_period | `pyspark.sql.types.StringType` | "hourly" or "minutely"  |
| firstrecord_timestamp_unix | `pyspark.sql.types.DoubleType` | epoch timestamp of the first metric used to derive this aggregated metric |
| lastrecord_timestamp_unix | `pyspark.sql.types.DoubleType` | epoch timestamp of the first metric used to derive this aggregated metric |
| quantity | `pyspark.sql.types.DoubleType` | aggregated metric quantity |
| record_count | `pyspark.sql.types.DoubleType` | number of source metrics that were used to derive this aggregated metric. For informational purposes only. |
| processing_meta | `pyspark.sql.types.MapType(pyspark.sql.types.StringType, pyspark.sql.types.StringType, True)` | Key-Value pairs to store additional information, to aid processing |
| extra_data_map | `pyspark.sql.types.MapType(pyspark.sql.types.StringType, pyspark.sql.types.StringType, True)` | Key-Value pairs to store group by column key value pair |

## References

[1] [Spark SQL, DataFrames and Datasets
Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

[2] [Spark
DataTypes](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.types.DataType)
