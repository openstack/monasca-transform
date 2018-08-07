Team and repository tags
========================

[![Team and repository tags](https://governance.openstack.org/badges/monasca-transform.svg)](https://governance.openstack.org/reference/tags/index.html)

<!-- Change things from this point on -->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Create a new aggregation pipeline](#create-a-new-aggregation-pipeline)
  - [Using existing generic aggregation components](#using-existing-generic-aggregation-components)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- Change things from this point on -->

# Create a new aggregation pipeline

Monasca Transform allows you to create new aggregation by creating *pre_transform_spec* and
*transform_spec* for any set of Monasca metrics. This page gives you steps on how to create a new
aggregation pipeline and test the pipeline in your DevStack environment.

Pre-requisite for following steps on this page is that you have already created a devstack
development environment for Monasca Transform, following instructions in
[devstack/README.md](devstack/README.md)


## Using existing generic aggregation components ##

Most of the use cases will fall into this category where you should be able to create new
aggregation for new set of metrics using existing set of generic aggregation components.

Let's consider a use case where we want to find out

* Maximum time monasca-agent takes to submit metrics over a period of an hour across all hosts

* Maximum time monasca-agent takes to submit metrics over period of a hour per host.

We know that monasca-agent on each host generates a small number of
[monasca-agent metrics](https://github.com/openstack/monasca-agent/blob/master/docs/Plugins.md).

The metric we are interested in is

* **"monasca.collection_time_sec"**: Amount of time that the collector took for this collection run

**Steps:**

  * **Step 1**: Identify the monasca metric to be aggregated from the Kafka topic
    ```
    /opt/kafka_2.11-0.9.0.1/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic metrics | grep "monasca.collection_time_sec"

    {"metric":{"timestamp":1523323485360.6650390625,"name":"monasca.collection_time_sec",
    "dimensions":{"hostname":"devstack","component":"monasca-agent",
    "service":"monitoring"},"value":0.0340659618, "value_meta":null},
    "meta":{"region":"RegionOne","tenantId":"d6bece1bbeff47c1b8734cd4e544dc02"},
    "creation_time":1523323489}
    ```
    Note: "hostname" is available as a dimension, which we will use to find maximum collection time for each host.

  * **Step 2**: Create a **pre_transform_spec**

    "pre_transform_spec" drives the pre-processing of monasca metric to record store format. Look
    for existing example in
    "/monasca-transform-source/monasca_transform/data_driven_specs/pre_transform_specs/pre_transform_specs.json"

    **pre_transform_spec**
    ```
    {
      "event_processing_params":{
        "set_default_zone_to":"1",
        "set_default_geolocation_to":"1",
        "set_default_region_to":"W"
      },
      "event_type":"monasca.collection_time_sec", <-- EDITED
      "metric_id_list":["monasca_collection_host"], <-- EDITED
      "required_raw_fields_list":["creation_time", "metric.dimensions.hostname"], <--EDITED
    }
    ```
    Lets look at all the fields that were edited (Marked as `<-- EDITED` above):

    **event_type**: set to "monasca.collection_time_sec". These are the metrics we want to
    transform/aggregate.

    **metric_id_list**: set to ['monasca_collection_host']. This is a transformation spec
    identifier. During pre-processing record generator generates additional "record_store" data for
    each item in this list. (To be renamed to transform_spec_list)

    **required_raw_fields_list**: set to ["creation_time", "metric.dimensions.hostname"]
    This should list fields in the incoming metrics that are required. Pre-processing will
    eliminate or remove metrics which have missing required fields, during validation.

    **Note:** "metric_id" is a misnomer, it is not really a metric identifier but rather identifier
    for transformation spec. This will be changed to transform_spec_id in the future.

  * **Step 3**: Create a "transform_spec" to find maximum metric value for each host

    "transform_spec" drives the aggregation of record store data created during pre-processing
     to final aggregated metric. Look for existing example in
    "/monasca-transform-source/monasca_transform/data_driven_specs/transform_specs/transform_specs.json"

    **transform_spec**
    ```
    {
      "aggregation_params_map":{

      "aggregation_pipeline":{
        "source":"streaming",
        "usage":"fetch_quantity", <-- EDITED
        "setters":["set_aggregated_metric_name","set_aggregated_period"], <-- EDITED
        "insert":["insert_data_pre_hourly"] <-- EDITED
      },

      "aggregated_metric_name":"monasca.collection_time_sec_host_agg", <-- EDITED
      "aggregation_period":"hourly", <-- EDITED
      "aggregation_group_by_list": ["host"],
      "usage_fetch_operation": "max", <-- EDITED
      "filter_by_list": [],
      "dimension_list":["aggregation_period","host"], <-- EDITED

      "pre_hourly_operation":"max",
      "pre_hourly_group_by_list":["default"]},

      "metric_group":"monasca_collection_host", <-- EDITED
      "metric_id":"monasca_collection_host" <-- EDITED
    }
    ```
    Lets look at all the fields that were edited (Marked as `<-- EDITED` above):

    aggregation pipeline fields

    * **usage**: set to "fetch_quantity" Use "fetch_quantity" generic aggregation component.  This
      component takes a "aggregation_group_by_list", "usage_fetch_operation" and "filter_by_list" as
      parameters.
        * **aggregation_group_by_list** set to ["host"]. Since we want to find monasca agent
          collection time for each host.
        * **usage_fetch_operation** set to "max". Since we want to find maximum value for
          monasca agent collection time.
        * **filter_by_list** set to []. Since we dont want filter out/ignore any metrics (based on
          say particular host or set of hosts)

    * **setters**: set to ["set_aggregated_metric_name","set_aggregated_period"] These components set
    aggregated metric name and aggregation period in final aggregated metric.
      * **set_aggregated_metric_name** sets final aggregated metric name. This setter component takes
      "aggregated_metric_name" as a parameter.
        * **aggregated_metric_name**: set to "monasca.collection_time_sec_host_agg"
      * **set_aggregated_period** sets final aggregated metric period. This setter component takes
        "aggregation_period" as a parameter.
        * **aggregation_period**: set to "hourly"

    * **insert**: set to ["insert_data_pre_hourly"]. These components are responsible for
      transforming instance usage data records to final metric format and writing the data to kafka
      topic.
      * **insert_data_pre_hourly** writes the to "metrics_pre_hourly" kafka topic, which gets
        processed by the pre hourly processor every hour.

    pre hourly processor fields

    * **pre_hourly_operation**  set to "max"
       Find the hourly maximum value from records that were written to "metrics_pre_hourly" topic

    * **pre_hourly_group_by_list**  set to ["default"]

    transformation spec identifier fields

    * **metric_group** set to "monasca_collection_host". Group identifier for this transformation
      spec

    * **metric_id** set to "monasca_collection_host". Identifier for this transformation spec.

    **Note:** metric_group" and "metric_id" are misnomers, it is not really a metric identifier but
    rather identifier for transformation spec. This will be changed to "transform_group" and
    "transform_spec_id" in the future. (Please see Story
    [2001815](https://storyboard.openstack.org/#!/story/2001815))

  * **Step 4**: Create a "transform_spec" to find maximum metric value across all hosts

    Now let's create another transformation spec to find maximum metric value across all hosts.

    **transform_spec**
    ```
    {
      "aggregation_params_map":{

      "aggregation_pipeline":{
        "source":"streaming",
        "usage":"fetch_quantity", <-- EDITED
        "setters":["set_aggregated_metric_name","set_aggregated_period"], <-- EDITED
        "insert":["insert_data_pre_hourly"] <-- EDITED
      },

      "aggregated_metric_name":"monasca.collection_time_sec_all_agg", <-- EDITED
      "aggregation_period":"hourly", <-- EDITED
      "aggregation_group_by_list": [],
      "usage_fetch_operation": "max", <-- EDITED
      "filter_by_list": [],
      "dimension_list":["aggregation_period"], <-- EDITED

      "pre_hourly_operation":"max",
      "pre_hourly_group_by_list":["default"]},

      "metric_group":"monasca_collection_all", <-- EDITED
      "metric_id":"monasca_collection_all" <-- EDITED
    }
    ```

    The transformation spec above is almost identical to transformation spec created in **Step 3**
    with a few additional changes.

    **aggregation_group_by_list** is set to [] i.e. empty list, since we want to find maximum value
    across all hosts (consider all the incoming metric data).

    **aggregated_metric_name** is set to "monasca.collection_time_sec_all_agg".

    **metric_group** is set to "monasca_collection_all", since we need a new transfomation spec
    group identifier.

    **metric_id** is set to "monasca_collection_all", since we need a new transformation spec
    identifier.

  * **Step 5**: Update "pre_transform_spec" with new transformation spec identifier

    In **Step 4** we created a new transformation spec, with new "metric_id", namely
    "monasca_collection_all". We will have to now update the "pre_transform_spec" that we
    created in **Step 2** with new "metric_id" by adding it to the "metric_id_list"

    **pre_transform_spec**
    ```
    {
      "event_processing_params":{
          "set_default_zone_to":"1",
          "set_default_geolocation_to":"1",
          "set_default_region_to":"W"
      },
      "event_type":"monasca.collection_time_sec",
      "metric_id_list":["monasca_collection_host", "monasca_collection_all"], <-- EDITED
      "required_raw_fields_list":["creation_time", "metric.dimensions.hostname"],
    }
    ```
    Thus we were able to add additional transformation or aggregation pipeline to the same incoming
    monasca metric very easily.

  * **Step 6**: Update "pre_transform_spec" and "transform_spec"

    * Edit
      "/monasca-transform-source/monasca_transform/data_driven_specs/pre_transform_specs/pre_transform_specs.json"
      and add following line.

      ```
        {"event_processing_params":{"set_default_zone_to":"1","set_default_geolocation_to":"1","set_default_region_to":"W"},"event_type":"monasca.collection_time_sec","metric_id_list":["monasca_collection_host","monasca_collection_all"],"required_raw_fields_list":["creation_time"]}
      ```

      **Note:** Each line does not end with a comma (the file is not one big json document).

    * Edit
      "/monasca-transform-source/monasca_transform/data_driven_specs/transform_specs/transform_specs.json"
      and add following lines.

      ```
        {"aggregation_params_map":{"aggregation_pipeline":{"source":"streaming","usage":"fetch_quantity","setters":["set_aggregated_metric_name","set_aggregated_period"],"insert":["insert_data_pre_hourly"]},"aggregated_metric_name":"monasca.collection_time_sec_host_agg","aggregation_period":"hourly","aggregation_group_by_list":["host"],"usage_fetch_operation":"max","filter_by_list":[],"dimension_list":["aggregation_period","host"],"pre_hourly_operation":"max","pre_hourly_group_by_list":["default"]},"metric_group":"monasca_collection_host","metric_id":"monasca_collection_host"}
        {"aggregation_params_map":{"aggregation_pipeline":{"source":"streaming","usage":"fetch_quantity","setters":["set_aggregated_metric_name","set_aggregated_period"],"insert":["insert_data_pre_hourly"]},"aggregated_metric_name":"monasca.collection_time_sec_all_agg","aggregation_period":"hourly","aggregation_group_by_list":[],"usage_fetch_operation":"max","filter_by_list":[],"dimension_list":["aggregation_period"],"pre_hourly_operation":"max","pre_hourly_group_by_list":["default"]},"metric_group":"monasca_collection_all","metric_id":"monasca_collection_all"}
      ```

    * Run "refresh_monasca_transform.sh" script as documented in devstack
      [README](devstack/README.md) to refresh the specs in the database.
      ```
      vagrant@devstack:~$ cd /opt/stack/monasca-transform
      vagrant@devstack:/opt/stack/monasca-transform$ tools/vagrant/refresh_monasca_transform.sh
      ```

      If successful, you should see this message.
      ```
        ***********************************************
        *                                             *
        * SUCCESS!! refresh monasca transform done.   *
        *                                             *
        ***********************************************
      ```
  * **Step 7**: Verifying results

    To verify if new aggregated metrics are being produced you can look at the "metrics_pre_hourly"
    topic in kafka. By default, monasca-transform fires of a batch every 10 minutes so you should
    see metrics in intermediate "instance_usage" data format being published to that topic every 10
    minutes.
    ```
    /opt/kafka_2.11-0.9.0.1/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic metrics_pre_hourly

    {"usage_hour":"06","geolocation":"NA","record_count":40.0,"app":"NA","deployment":"NA","resource_uuid":"NA",
    "pod_name":"NA","usage_minute":"NA","service_group":"NA","lastrecord_timestamp_string":"2018-04-1106:29:49",
    "user_id":"NA","zone":"NA","namespace":"NA","usage_date":"2018-04-11","daemon_set":"NA","processing_meta":{
    "event_type":"NA","metric_id":"monasca_collection_all"},
    "firstrecord_timestamp_unix":1523427604.208577,"project_id":"NA","lastrecord_timestamp_unix":1523428189.718174,
    "aggregation_period":"hourly","host":"NA","container_name":"NA","interface":"NA",
    "aggregated_metric_name":"monasca.collection_time_sec_all_agg","tenant_id":"NA","region":"NA",
    "firstrecord_timestamp_string":"2018-04-11 06:20:04","quantity":0.0687000751}

    {"usage_hour":"06","geolocation":"NA","record_count":40.0,"app":"NA","deployment":"NA","resource_uuid":"NA",
    "pod_name":"NA","usage_minute":"NA","service_group":"NA","lastrecord_timestamp_string":"2018-04-11 06:29:49",
    "user_id":"NA","zone":"NA","namespace":"NA","usage_date":"2018-04-11","daemon_set":"NA","processing_meta":{
    "event_type":"NA","metric_id":"monasca_collection_host"},"firstrecord_timestamp_unix":1523427604.208577,
    "project_id":"NA","lastrecord_timestamp_unix":1523428189.718174,"aggregation_period":"hourly",
    "host":"devstack","container_name":"NA","interface":"NA",
    "aggregated_metric_name":"monasca.collection_time_sec_host_agg","tenant_id":"NA","region":"NA",
    "firstrecord_timestamp_string":"2018-04-11 06:20:04","quantity":0.0687000751}
    ```

    Similarly, to verify if final aggregated metrics are being published by pre hourly processor,
    you can look at "metrics" topic in kafka. By default pre hourly processor (which processes
    metrics from "metrics_pre_hourly" topic) runs 10 minutes past the top of the hour.
    ```
    /opt/kafka_2.11-0.9.0.1/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic metrics | grep "_agg"

    {"metric":{"timestamp":1523459468616,"value_meta":{"firstrecord_timestamp_string":"2018-04-11 14:00:13",
    "lastrecord_timestamp_string":"2018-04-11 14:59:46","record_count":239.0},"name":"monasca.collection_time_sec_host_agg",
    "value":0.1182248592,"dimensions":{"aggregation_period":"hourly","host":"devstack"}},
    "meta":{"region":"useast","tenantId":"df89c3db21954b08b0516b4b60b8baff"},"creation_time":1523459468}

    {"metric":{"timestamp":1523455872740,"value_meta":{"firstrecord_timestamp_string":"2018-04-11 13:00:10",
    "lastrecord_timestamp_string":"2018-04-11 13:59:58","record_count":240.0},"name":"monasca.collection_time_sec_all_agg",
    "value":0.0898442268,"dimensions":{"aggregation_period":"hourly"}},
    "meta":{"region":"useast","tenantId":"df89c3db21954b08b0516b4b60b8baff"},"creation_time":1523455872}
    ```

    As you can see monasca-transform created two new aggregated metrics with name
    "monasca.collection_time_sec_host_agg" and "monasca.collection_time_sec_all_agg". "value_meta"
    section has three fields "firstrecord_timestamp" and "lastrecord_timestamp" and
    "record_count". These fields are for informational purposes only. It shows timestamp of the first metric,
    timestamp of the last metric and number of metrics that went into the calculation of the aggregated
    metric.
