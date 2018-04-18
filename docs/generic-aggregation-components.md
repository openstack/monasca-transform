Team and repository tags
========================

[![Team and repository tags](https://governance.openstack.org/badges/monasca-transform.svg)](https://governance.openstack.org/reference/tags/index.html)

<!-- Change things from this point on -->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
- [Monasca Transform Generic Aggregation Components](#monasca-transform-generic-aggregation-components)

- [Monasca Transform Generic Aggregation Components](#monasca-transform-generic-aggregation-components)
- [Introduction](#introduction)
  - [1: Conversion of incoming metrics to record store data format](#1-conversion-of-incoming-metrics-to-record-store-data-format)
    - [Pre Transform Spec](#pre-transform-spec)
  - [2: Data aggregation using generic aggregation components](#2-data-aggregation-using-generic-aggregation-components)
    - [Transform Specs](#transform-specs)
      - [aggregation_params_map](#aggregation_params_map)
        - [aggregation_pipeline](#aggregation_pipeline)
        - [Other parameters](#other-parameters)
    - [metric_group and metric_id](#metric_group-and-metric_id)
  - [Generic Aggregation Components](#generic-aggregation-components)
    - [Usage Components](#usage-components)
      - [fetch_quantity](#fetch_quantity)
      - [fetch_quantity_util](#fetch_quantity_util)
      - [calculate_rate](#calculate_rate)
    - [Setter Components](#setter-components)
      - [set_aggregated_metric_name](#set_aggregated_metric_name)
      - [set_aggregated_period](#set_aggregated_period)
      - [rollup_quantity](#rollup_quantity)
    - [Insert Components](#insert-components)
      - [insert_data](#insert_data)
      - [insert_data_pre_hourly](#insert_data_pre_hourly)
  - [Processors](#processors)
    - [pre_hourly_processor](#pre_hourly_processor)
  - [Special notation](#special-notation)
    - [pre_transform spec](#pre_transform-spec)
    - [transform spec](#transform-spec)
- [Putting it all together](#putting-it-all-together)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
# Monasca Transform Generic Aggregation Components

# Introduction

Monasca Transform uses standard ETL (Extract-Transform-Load) design pattern to aggregate monasca
metrics and uses innovative data/configuration driven mechanism to drive processing. It accomplishes
data aggregation in two distinct steps, each is driven using external configuration specifications,
namely *pre-transform_spec* and *transform_spec*.

## 1: Conversion of incoming metrics to record store data format ##

In the first step, the incoming metrics are converted into a canonical data format called as record
store data using *pre_transform_spec*.

This logical processing data flow is explained in more detail in [Monasca/Transform wiki: Logical
processing data flow section: Conversion to record store
format](https://wiki.openstack.org/wiki/Monasca/Transform#Logical_processing_data_flow) and includes
following operations:

 * identifying metrics that are required (or in other words filtering out of unwanted metrics)

 * validation and extraction of essential data in metric

 * generating multiple records for incoming metrics if they are to be aggregated in multiple ways,
   and finally

 * conversion of the incoming metrics to canonical record store data format. Please refer to record
   store section in [Data Formats](data_formats.md) for more information on record store format.

### Pre Transform Spec ###

Example *pre_transform_spec* for metric

```
{
  "event_processing_params":{"set_default_zone_to":"1","set_default_geolocation_to":"1","set_default_region_to":"W"},
  "event_type":"cpu.total_logical_cores",
  "metric_id_list":["cpu_total_all","cpu_total_host","cpu_util_all","cpu_util_host"],
  "required_raw_fields_list":["creation_time"],
}
```

*List of fields*

| field name | values | description |
| :--------- | :----- | :---------- |
| event_processing_params | Set default field values `set_default_zone_to`, `set_default_geolocation_to`, `set_default_region_to`| Set default values for certain fields in the record store data |
| event_type | Name of the metric | identifies metric that needs to be aggregated |
| metric_id_list | List of `metric_id`'s | List of identifiers, should match `metric_id` in transform specs. This is used by record generation step to generate multiple records if this metric is to be aggregated in multiple ways|
| required_raw_fields_list | List of `field`'s  | List of fields (use [Special notation](#special-notation)) that are required in the incoming metric, used for validating incoming metric. The validator checks if field is present and is not empty. If the field is absent or empty the validator filters such metrics out from aggregation. |

## 2: Data aggregation using generic aggregation components ##

In the second step, the canonical record store data is aggregated using *transform_spec*. Each
*transform_spec* defines series of generic aggregation components, which are specified in
`aggregation_params_map.aggregation_pipeline` section. (See *transform_spec* example below).

Any parameters used by the generic aggregation components are also specified in the
`aggregation_params_map` section (See *Other parameters* e.g. `aggregated_metric_name`, `aggregation_period`,
`aggregation_group_by_list` etc. in *transform_spec* example below)

###  Transform Specs ###

Example *transform_spec* for metric
```
{"aggregation_params_map":{
    "aggregation_pipeline":{
        "source":"streaming",
        "usage":"fetch_quantity",
        "setters":["rollup_quantity","set_aggregated_metric_name","set_aggregated_period"],
        "insert":["prepare_data","insert_data_pre_hourly"]
    },
    "aggregated_metric_name":"cpu.total_logical_cores_agg",
    "aggregation_period":"hourly",
    "aggregation_group_by_list": ["host", "metric_id", "tenant_id"],
    "usage_fetch_operation": "avg",
    "filter_by_list": [],
    "setter_rollup_group_by_list": [],
    "setter_rollup_operation": "sum",
    "dimension_list":["aggregation_period","host","project_id"],
    "pre_hourly_operation":"avg",
    "pre_hourly_group_by_list":["default"]
 },
 "metric_group":"cpu_total_all",
 "metric_id":"cpu_total_all"
}
```

#### aggregation_params_map ####

This section specifies *aggregation_pipeline*, *Other parameters* (used by generic aggregation
components in *aggregation_pipeline*).

##### aggregation_pipeline #####

Specifies generic aggregation components that should be used to process incoming metrics.

Note: generic aggregation components are re-usable and can be used to build different aggregation
pipelines as required.

*List of fields*

| field name | values | description |
| :--------- | :----- | :---------- |
| source | ```streaming``` | source is ```streaming```. In the future this can be used to specify a component which can fetch data directly from monasca datastore |
| usage | ```fetch_quantity```, ```fetch_quantity_util```, ```calculate_rate``` | [Usage Components](https://github.com/openstack/monasca-transform/tree/master/monasca_transform/component/usage)|
| setters | ```pre_hourly_calculate_rate```, ```rollup_quantity```, ```set_aggregated_metric_name```, ```set_aggregated_period``` | [Setter Components](https://github.com/openstack/monasca-transform/tree/master/monasca_transform/component/setter)|
| insert | ```insert_data```, ```insert_data_pre_hourly``` | [Insert Components](https://github.com/openstack/monasca-transform/tree/master/monasca_transform/component/insert)|


##### Other parameters #####

Specifies parameters that generic aggregation components use to process and aggregate data.

*List of Other parameters*

| Parameter Name | Values | Description | Used by |
| :------------- | :----- | :---------- | :------ |
| aggregated_metric_name| e.g. "cpu.total_logical_cores_agg" | Name of the aggregated metric | [set_aggregated_metric_name](#set_aggregated_metric_name) |
| aggregation_period |"hourly", "minutely" or "secondly" | Period over which to aggregate data. | [fetch_quantity](#fetch_quantity), [fetch_quantity_util](#fetch_quantity_util), [calculate_rate](#calculate_rate), [set_aggregated_period](#set_aggregated_period), [rollup_quantity](#rollup_quantity) |[fetch_quantity](#fetch_quantity), [fetch_quantity_util](#fetch_quantity_util), [calculate_rate](#calculate_rate) |
| aggregation_group_by_list | e.g. "project_id", "hostname" | Group `record_store` data with these columns. Please also see [Special notation](#special-notation) below | [fetch_quantity](#fetch_quantity), [fetch_quantity_util](#fetch_quantity_util), [calculate_rate](#calculate_rate) |
| usage_fetch_operation | e.g "sum" | After the data is grouped by `aggregation_group_by_list`, perform this operation to find the aggregated metric value | [fetch_quantity](#fetch_quantity), [fetch_quantity_util](#fetch_quantity_util), [calculate_rate](#calculate_rate) |
| filter_by_list | Filter regex | Filter data using regex on a `record_store` column value| [fetch_quantity](#fetch_quantity), [fetch_quantity_util](#fetch_quantity_util), [calculate_rate](#calculate_rate) |
| setter_rollup_group_by_list | e.g. "project_id" | Group `instance_usage` data with these columns rollup operation. Please also see [Special notation](#special-notation) below | [rollup_quantity](#rollup_quantity) |
| setter_rollup_operation | e.g. "avg" | After data is grouped by `setter_rollup_group_by_list`, perform this operation to find aggregated metric value | [rollup_quantity](#rollup_quantity) |
| dimension_list | e.g. "aggregation_period", "host", "project_id" | List of fields which specify dimensions in aggregated metric. Please also see [Special notation](#special-notation) below | [insert_data](#insert_data), [insert_data_pre_hourly](#insert_data_pre_hourly)|
| pre_hourly_group_by_list | e.g.  "default" | List of `instance usage data` fields to do a group by operation to aggregate data. Please also see [Special notation](#special-notation) below | [pre_hourly_processor](#pre_hourly_processor) |
| pre_hourly_operation | e.g. "avg" | When aggregating data published to `metrics_pre_hourly` every hour, perform this operation to find hourly aggregated metric value | [pre_hourly_processor](#pre_hourly_processor) |

### metric_group and metric_id ###

Specifies a metric or list of metrics from the record store data, which will be processed by this
*transform_spec*.  Note: This can be a single metric or a group of metrics that will be combined to
produce the final aggregated metric.

*List of fields*

| field name | values | description |
| :--------- | :----- | :---------- |
| metric_group | unique transform spec group identifier | group identifier for this transform spec e.g. "cpu_total_all" |
| metric_id | unique transform spec identifier | identifier for this transform spec e.g. "cpu_total_all" |

**Note:** "metric_id" is a misnomer, it is not really a metric group/or metric identifier but rather
identifier for transformation spec. This will be changed to "transform_spec_id" in the future.

## Generic Aggregation Components ##

*List of Generic Aggregation Components*

### Usage Components ###

All usage components implement a method

```
    def usage(transform_context, record_store_df):
    ..
    ..
    return instance_usage_df
```

#### fetch_quantity ####

This component groups record store records by `aggregation_group_by_list`, sorts within
group by timestamp field, finds usage based on `usage_fetch_operation`. Optionally this
component also takes `filter_by_list` to include for exclude certain records from usage
calculation.

*Other parameters*

  * **aggregation_group_by_list**

    List of fields to group by.

    Possible values: any set of fields in record store data. Please also see [Special notation](#special-notation).

    Example:

    ```
    "aggregation_group_by_list": ["tenant_id"]
    ```
  * **usage_fetch_operation**

    Operation to be performed on grouped data set.

    *Possible values:* "sum", "max", "min", "avg", "latest", "oldest"

  * **aggregation_period**

    Period to aggregate by.

    *Possible values:* 'daily', 'hourly', 'minutely', 'secondly'.

    Example:

    ```
    "aggregation_period": "hourly"
    ```

  * **filter_by_list**

    Filter (include or exclude) record store data as specified.

    Example:

    ```
    filter_by_list": "[{"field_to_filter": "hostname",
                        "filter_expression": "comp-(\d)+",
                        "filter_operation": "include"}]
    ```

    OR

    ```
    filter_by_list": "[{"field_to_filter": "hostname",
                        "filter_expression": "controller-(\d)+",
                        "filter_operation": "exclude"}]
    ```

#### fetch_quantity_util ####

This component finds the utilized quantity based on *total_quantity* and *idle_perc* using
following calculation

```
utilized_quantity = (100 - idle_perc) * total_quantity / 100
```

where,

  * **total_quantity** data, identified by `usage_fetch_util_quantity_event_type` parameter and

  * **idle_perc** data, identified by `usage_fetch_util_idle_perc_event_type` parameter

This component initially groups record store records by `aggregation_group_by_list` and
`event_type`, sorts within group by timestamp field, calculates `total_quantity` and
`idle_perc` values based on `usage_fetch_operation`. `utilized_quantity` is then calculated
using the formula given above.

*Other parameters*

  * **aggregation_group_by_list**

    List of fields to group by.

    Possible values: any set of fields in record store data. Please also see [Special notation](#special-notation) below.

    Example:

    ```
    "aggregation_group_by_list": ["tenant_id"]
    ```
  * **usage_fetch_operation**

    Operation to be performed on grouped data set

    *Possible values:* "sum", "max", "min", "avg", "latest", "oldest"

  * **aggregation_period**

    Period to aggregate by.

    *Possible values:* 'daily', 'hourly', 'minutely', 'secondly'.

    Example:

    ```
    "aggregation_period": "hourly"
    ```

  * **filter_by_list**

    Filter (include or exclude) record store data as specified

    Example:

    ```
    filter_by_list": "[{"field_to_filter": "hostname",
                        "filter_expression": "comp-(\d)+",
                        "filter_operation": "include"}]
    ```

    OR

    ```
    filter_by_list": "[{"field_to_filter": "hostname",
                        "filter_expression": "controller-(\d)+",
                        "filter_operation": "exclude"}]
    ```

  * **usage_fetch_util_quantity_event_type**

    event type (metric name) to identify data which will be used to calculate `total_quantity`

    *Possible values:* metric name

    Example:

    ```
    "usage_fetch_util_quantity_event_type": "cpu.total_logical_cores"
    ```


  * **usage_fetch_util_idle_perc_event_type**

    event type (metric name) to identify data which will be used to calculate `total_quantity`

    *Possible values:* metric name

    Example:

    ```
    "usage_fetch_util_idle_perc_event_type": "cpu.idle_perc"
    ```

#### calculate_rate ####

This component finds the rate of change of quantity (in percent) over a time period using
following calculation

```
rate_of_change (in percent) = ((oldest_quantity - latest_quantity)/oldest_quantity) * 100
```

where,

  * **oldest_quantity**: oldest (or earliest) `average` quantity if there are multiple quantites in a
                         group for a given time period.

  * **latest_quantity**: latest `average` quantity if there are multiple quantities in a group
                         for a given time period

*Other parameters*

  * **aggregation_group_by_list**

    List of fields to group by.

    Possible values: any set of fields in record store data. Please also see [Special notation](#special-notation) below.

    Example:

    ```
    "aggregation_group_by_list": ["tenant_id"]
    ```
  * **usage_fetch_operation**

    Operation to be performed on grouped data set

    *Possible values:* "sum", "max", "min", "avg", "latest", "oldest"

  * **aggregation_period**

    Period to aggregate by.

    *Possible values:* 'daily', 'hourly', 'minutely', 'secondly'.

    Example:

    ```
    "aggregation_period": "hourly"
    ```

  * **filter_by_list**

    Filter (include or exclude) record store data as specified

    Example:

    ```
    filter_by_list": "[{"field_to_filter": "hostname",
                        "filter_expression": "comp-(\d)+",
                        "filter_operation": "include"}]
    ```

    OR

    ```
    filter_by_list": "[{"field_to_filter": "hostname",
                        "filter_expression": "controller-(\d)+",
                        "filter_operation": "exclude"}]
    ```


### Setter Components ###

All usage components implement a method

```
    def setter(transform_context, instance_usage_df):
    ..
    ..
    return instance_usage_df
```

#### set_aggregated_metric_name ####

This component sets final aggregated metric name by setting `aggregated_metric_name` field in
`instance_usage` data.

*Other parameters*

  * **aggregated_metric_name**

    Name of the metric name being generated.

    *Possible values:* any aggregated metric name. Convention is to end the metric name
    with "_agg".

    Example:
    ```
    "aggregated_metric_name":"cpu.total_logical_cores_agg"
    ```

#### set_aggregated_period ####

This component sets final aggregated metric name by setting `aggregation_period` field in
`instance_usage` data.

*Other parameters*

  * **aggregated_period**

    Name of the metric name being generated.

    *Possible values:* 'daily', 'hourly', 'minutely', 'secondly'.

    Example:
    ```
    "aggregation_period": "hourly"
    ```

**Note** If you are publishing metrics to *metrics_pre_hourly* kafka topic using
`insert_data_pre_hourly` component(See *insert_data_pre_hourly* component below),
`aggregation_period` will have to be set to `hourly`since by default all data in
*metrics_pre_hourly* topic, by default gets aggregated every hour by `Pre Hourly Processor` (See
`Processors` section below)

#### rollup_quantity ####

This component groups `instance_usage` records by `setter_rollup_group_by_list`, sorts within
group by timestamp field, finds usage based on `setter_fetch_operation`.

*Other parameters*

  * **setter_rollup_group_by_list**

    List of fields to group by.

    Possible values: any set of fields in record store data. Please also see [Special notation](#special-notation) below.

    Example:
    ```
    "setter_rollup_group_by_list": ["tenant_id"]
    ```
  * **setter_fetch_operation**

    Operation to be performed on grouped data set

    *Possible values:* "sum", "max", "min", "avg"

    Example:
    ```
    "setter_fetch_operation": "avg"
    ```

  * **aggregation_period**

    Period to aggregate by.

    *Possible values:* 'daily', 'hourly', 'minutely', 'secondly'.

    Example:

    ```
    "aggregation_period": "hourly"
    ```

### Insert Components ###

All usage components implement a method

```
    def insert(transform_context, instance_usage_df):
    ..
    ..
    return instance_usage_df
```

#### insert_data ####

This component converts `instance_usage` data into monasca metric format and writes the metric to
`metrics` topic in kafka.

*Other parameters*

  * **dimension_list**

    List of fields in `instance_usage` data that should be converted to monasca metric dimensions.

    *Possible values:* any fields in `instance_usage` data or use [Special notation](#special-notation) below.

    Example:
    ```
    "dimension_list":["aggregation_period","host","project_id"]
    ```

#### insert_data_pre_hourly ####

This component converts `instance_usage` data into monasca metric format and writes the metric to
`metrics_pre_hourly` topic in kafka.

*Other parameters*

  * **dimension_list**

    List of fields in `instance_usage` data that should be converted to monasca metric dimensions.

    *Possible values:* any fields in `instance_usage` data

    Example:
    ```
    "dimension_list":["aggregation_period","host","project_id"]
    ```

## Processors ##

Processors are special components that process data from a kafka topic, at the desired time
interval. These are different from generic aggregation components since they process data from
specific kafka topic.

All processor components implement following methods

```
def get_app_name(self):
    [...]
    return app_name

def is_time_to_run(self, current_time):
    if current_time > last_invoked + 1:
        return True
    else:
        return False

def run_processor(self, time):
    # do work...
```

### pre_hourly_processor ###

Pre Hourly Processor, runs every hour and aggregates `instance_usage` data published to
`metrics_pre_hourly` topic.

Pre Hourly Processor by default is set to run 10 minutes after the top of the hour and processes
data from previous hour. `instance_usage` data is grouped by `pre_hourly_group_by_list`

*Other parameters*

  * **pre_hourly_group_by_list**

    List of fields to group by.

    Possible values: any set of fields in `instance_usage` data or to `default`. Please also see
    [Special notation](#special-notation) below.

    Note: setting to `default` will group `instance_usage` data by `tenant_id`, `user_id`,
    `resource_uuid`, `geolocation`, `region`, `zone`, `host`, `project_id`,
    `aggregated_metric_name`, `aggregation_period`

    Example:
    ```
    "pre_hourly_group_by_list": ["tenant_id"]
    ```

    OR

    ```
    "pre_hourly_group_by_list": ["default"]
    ```

  * **pre_hourly_operation**

    Operation to be performed on grouped data set.

    *Possible values:* "sum", "max", "min", "avg", "rate"

    Example:

    ```
    "pre_hourly_operation": "avg"
    ```


## Special notation ##

### pre_transform spec ###

To specify `required_raw_fields_list` please use special notation
`dimensions#{$field_name}` or `meta#{$field_name}` or`value_meta#{$field_name}` to refer to any field in
dimension, meta or value_meta field in the incoming raw metric.

For example if you want to check that for a particular metric say dimension called "pod_name" is
present and is non-empty, then simply add `dimensions#pod_name` to the
`required_raw_fields_list`.

Example `pre_transform` spec
```
{"event_processing_params":{"set_default_zone_to":"1",
                            "set_default_geolocation_to":"1",
                            "set_default_region_to":"W"},
 "event_type":"pod.net.in_bytes_sec",
 "metric_id_list":["pod_net_in_b_per_sec_per_namespace"],
 "required_raw_fields_list":["creation_time",
                             "meta#tenantId",
                             "dimensions#namespace",
                             "dimensions#pod_name",
                             "dimensions#app"]
}
```

### transform spec ###

To specify `aggregation_group_by_list`, `setter_rollup_group_by_list`, `pre_hourly_group_by_list`,
`dimension_list`, you can also use special notation `dimensions#{$field_name}` or `meta#{$field_name}`
or`value_meta#$field_name` to refer to any field in dimension, meta or value_meta field in the
incoming raw metric.

For example following `transform_spec` will aggregate by "app", "namespace" and "pod_name"
dimensions, then will do a rollup of the aggregated data by "namespace" dimension, and write final
aggregated metric with "app", "namespace" and "pod_name" dimensions.  Note that "app" and "pod_name"
will be set to "all" since the final rollup operation was done only based on "namespace" dimension.

```
{
 "aggregation_params_map":{
       "aggregation_pipeline":{"source":"streaming",
       "usage":"fetch_quantity",
       "setters":["rollup_quantity",
                  "set_aggregated_metric_name",
                  "set_aggregated_period"],
       "insert":["prepare_data",
                 "insert_data_pre_hourly"]},
       "aggregated_metric_name":"pod.net.in_bytes_sec_agg",
       "aggregation_period":"hourly",
       "aggregation_group_by_list": ["tenant_id",
                                     "dimensions#app",
                                     "dimensions#namespace",
                                     "dimensions#pod_name"],
       "usage_fetch_operation": "avg",
       "filter_by_list": [],
       "setter_rollup_group_by_list":["dimensions#namespace"],
       "setter_rollup_operation": "sum",
       "dimension_list":["aggregation_period",
                         "dimensions#app",
                         "dimensions#namespace",
                         "dimensions#pod_name"],
       "pre_hourly_operation":"avg",
       "pre_hourly_group_by_list":["aggregation_period",
                                   "dimensions#namespace]'"]},
 "metric_group":"pod_net_in_b_per_sec_per_namespace",
 "metric_id":"pod_net_in_b_per_sec_per_namespace"}
```

# Putting it all together
Please refer to [Create a new aggregation pipeline](create-new-aggregation-pipeline.md) document to
create a new aggregation pipeline.
