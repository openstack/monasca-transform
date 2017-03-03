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

from pyspark import SparkConf
from pyspark import SparkContext

from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition
from pyspark.streaming import StreamingContext

from pyspark.sql.functions import explode
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from pyspark.sql import SQLContext

import logging
from monasca_common.simport import simport
from oslo_config import cfg
import time

from monasca_transform.component.usage.fetch_quantity import \
    FetchQuantityException
from monasca_transform.component.usage.fetch_quantity_util import \
    FetchQuantityUtilException
from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.log_utils import LogUtils
from monasca_transform.transform.builder.generic_transform_builder \
    import GenericTransformBuilder

from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepo

from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepoFactory

from monasca_transform.processor.pre_hourly_processor import PreHourlyProcessor

from monasca_transform.transform import RddTransformContext
from monasca_transform.transform.storage_utils import \
    InvalidCacheStorageLevelException
from monasca_transform.transform.storage_utils import StorageUtils
from monasca_transform.transform.transform_utils import MonMetricUtils
from monasca_transform.transform import TransformContextUtils

ConfigInitializer.basic_config()
log = LogUtils.init_logger(__name__)


class MonMetricsKafkaProcessor(object):

    @staticmethod
    def log_debug(message):
        print(message)
        log.debug(message)

    @staticmethod
    def store_offset_ranges(batch_time, rdd):
        if rdd.isEmpty():
            MonMetricsKafkaProcessor.log_debug(
                "storeOffsetRanges: nothing to process...")
            return rdd
        else:
            my_offset_ranges = rdd.offsetRanges()
            transform_context = \
                TransformContextUtils.get_context(offset_info=my_offset_ranges,
                                                  batch_time_info=batch_time
                                                  )
            rdd_transform_context = \
                rdd.map(lambda x: RddTransformContext(x, transform_context))
            return rdd_transform_context

    @staticmethod
    def print_offset_ranges(my_offset_ranges):
        for o in my_offset_ranges:
            print("printOffSetRanges: %s %s %s %s" % (
                o.topic, o.partition, o.fromOffset, o.untilOffset))

    @staticmethod
    def get_kafka_stream(topic, streaming_context):
        offset_specifications = simport.load(cfg.CONF.repositories.offsets)()
        app_name = streaming_context.sparkContext.appName
        saved_offset_spec = offset_specifications.get_kafka_offsets(app_name)
        if len(saved_offset_spec) < 1:

            MonMetricsKafkaProcessor.log_debug(
                "No saved offsets available..."
                "connecting to kafka without specifying offsets")
            kvs = KafkaUtils.createDirectStream(
                streaming_context, [topic],
                {"metadata.broker.list": cfg.CONF.messaging.brokers})

            return kvs

        else:
            from_offsets = {}
            for key, value in saved_offset_spec.items():
                if key.startswith("%s_%s" % (app_name, topic)):
                    # spec_app_name = value.get_app_name()
                    spec_topic = value.get_topic()
                    spec_partition = int(value.get_partition())
                    # spec_from_offset = value.get_from_offset()
                    spec_until_offset = value.get_until_offset()
                    # composite_key = "%s_%s_%s" % (spec_app_name,
                    #                               spec_topic,
                    #                               spec_partition)
                    # partition = saved_offset_spec[composite_key]
                    from_offsets[
                        TopicAndPartition(spec_topic, spec_partition)
                    ] = long(spec_until_offset)

            MonMetricsKafkaProcessor.log_debug(
                "get_kafka_stream: calling createDirectStream :"
                " topic:{%s} : start " % topic)
            for key, value in from_offsets.items():
                MonMetricsKafkaProcessor.log_debug(
                    "get_kafka_stream: calling createDirectStream : "
                    "offsets : TopicAndPartition:{%s,%s}, value:{%s}" %
                    (str(key._topic), str(key._partition), str(value)))
            MonMetricsKafkaProcessor.log_debug(
                "get_kafka_stream: calling createDirectStream : "
                "topic:{%s} : done" % topic)

            kvs = KafkaUtils.createDirectStream(
                streaming_context, [topic],
                {"metadata.broker.list": cfg.CONF.messaging.brokers},
                from_offsets)
            return kvs

    @staticmethod
    def save_rdd_contents(rdd):
        file_name = "".join((
            "/vagrant_home/uniq_metrics",
            '-', time.strftime("%Y-%m-%d-%H-%M-%S"),
            '-', str(rdd.id),
            '.log'))
        rdd.saveAsTextFile(file_name)

    @staticmethod
    def save_kafka_offsets(current_offsets, app_name,
                           batch_time_info):
        """save current offsets to offset specification."""

        offset_specs = simport.load(cfg.CONF.repositories.offsets)()

        for o in current_offsets:
            MonMetricsKafkaProcessor.log_debug(
                "saving: OffSetRanges: %s %s %s %s, "
                "batch_time_info: %s" % (
                    o.topic, o.partition, o.fromOffset, o.untilOffset,
                    str(batch_time_info)))
        # add new offsets, update revision
        offset_specs.add_all_offsets(app_name,
                                     current_offsets,
                                     batch_time_info)

    @staticmethod
    def reset_kafka_offsets(app_name):
        """delete all offsets from the offset specification."""
        # get the offsets from global var
        offset_specs = simport.load(cfg.CONF.repositories.offsets)()
        offset_specs.delete_all_kafka_offsets(app_name)

    @staticmethod
    def _validate_raw_mon_metrics(row):

        required_fields = row.required_raw_fields_list

        invalid_list = []

        for required_field in required_fields:
            required_field_value = None

            # Look for the field in the first layer of the row
            try:
                required_field_value = eval(".".join(("row", required_field)))
            except Exception:
                pass

            if (required_field_value is None or required_field_value == "" and
                    row.metric is not None and
                    row.metric.dimensions is not None):
                # Look for the field in the dimensions layer of the row
                try:
                    required_field_value = eval(
                        ".".join(("row.metric.dimensions", required_field)))
                except Exception:
                    pass

            if (required_field_value is None or required_field_value == "" and
                    row.meta is not None):
                # Look for the field in the meta layer of the row
                try:
                    required_field_value = eval(
                        ".".join(("row.meta", required_field)))
                except Exception:
                    pass

            if required_field_value is None \
                    or required_field_value == "":
                invalid_list.append("invalid")

        if len(invalid_list) <= 0:
            return row
        else:
            print("_validate_raw_mon_metrics : found invalid : ** %s: %s" % (
                (".".join(("row", required_field))),
                required_field_value))

    @staticmethod
    def process_metric(transform_context, record_store_df):
        """process (aggregate) metric data from record_store data
        All the parameters to drive processing should be available
        in transform_spec_df dataframe.
        """

        # call processing chain
        return GenericTransformBuilder.do_transform(
            transform_context, record_store_df)

    @staticmethod
    def process_metrics(transform_context, record_store_df):
        """start processing (aggregating) metrics
        """
        #
        # look in record_store_df for list of metrics to be processed
        #
        metric_ids_df = record_store_df.select("metric_id").distinct()
        metric_ids_to_process = [row.metric_id
                                 for row in metric_ids_df.collect()]

        data_driven_specs_repo = DataDrivenSpecsRepoFactory.\
            get_data_driven_specs_repo()
        sqlc = SQLContext.getOrCreate(record_store_df.rdd.context)
        transform_specs_df = data_driven_specs_repo.get_data_driven_specs(
            sql_context=sqlc,
            data_driven_spec_type=DataDrivenSpecsRepo.transform_specs_type)

        for metric_id in metric_ids_to_process:
            transform_spec_df = transform_specs_df.select(
                ["aggregation_params_map", "metric_id"]
            ).where(transform_specs_df.metric_id == metric_id)
            source_record_store_df = record_store_df.select("*").where(
                record_store_df.metric_id == metric_id)

            # set transform_spec_df in TransformContext
            transform_context = \
                TransformContextUtils.get_context(
                    transform_context_info=transform_context,
                    transform_spec_df_info=transform_spec_df)

            try:
                agg_inst_usage_df = (
                    MonMetricsKafkaProcessor.process_metric(
                        transform_context, source_record_store_df))

                # if running in debug mode, write out the aggregated metric
                # name just processed (along with the count of how many of
                # these were aggregated) to the application log.
                if log.isEnabledFor(logging.DEBUG):
                    agg_inst_usage_collection = agg_inst_usage_df.collect()
                    collection_len = len(agg_inst_usage_collection)
                    if collection_len > 0:
                        agg_inst_usage_dict = (
                            agg_inst_usage_collection[0].asDict())
                        log.debug("Submitted pre-hourly aggregated metric: "
                                  "%s (%s)",
                                  agg_inst_usage_dict[
                                      "aggregated_metric_name"],
                                  str(collection_len))
            except FetchQuantityException:
                raise
            except FetchQuantityUtilException:
                raise
            except Exception as e:
                MonMetricsKafkaProcessor.log_debug(
                    "Exception raised in metric processing for metric: " +
                    str(metric_id) + ".  Error: " + str(e))

    @staticmethod
    def rdd_to_recordstore(rdd_transform_context_rdd):

        if rdd_transform_context_rdd.isEmpty():
            MonMetricsKafkaProcessor.log_debug(
                "rdd_to_recordstore: nothing to process...")
        else:

            sql_context = SQLContext.getOrCreate(
                rdd_transform_context_rdd.context)
            data_driven_specs_repo = DataDrivenSpecsRepoFactory.\
                get_data_driven_specs_repo()
            pre_transform_specs_df = data_driven_specs_repo.\
                get_data_driven_specs(
                    sql_context=sql_context,
                    data_driven_spec_type=DataDrivenSpecsRepo.
                    pre_transform_specs_type)

            #
            # extract second column containing raw metric data
            #
            raw_mon_metrics = rdd_transform_context_rdd.map(
                lambda nt: nt.rdd_info[1])

            #
            # convert raw metric data rdd to dataframe rdd
            #
            raw_mon_metrics_df = \
                MonMetricUtils.create_mon_metrics_df_from_json_rdd(
                    sql_context,
                    raw_mon_metrics)

            #
            # filter out unwanted metrics and keep metrics we are interested in
            #
            cond = [
                raw_mon_metrics_df.metric.name ==
                pre_transform_specs_df.event_type]
            filtered_metrics_df = raw_mon_metrics_df.join(
                pre_transform_specs_df, cond)

            #
            # validate filtered metrics to check if required fields
            # are present and not empty
            # In order to be able to apply filter function had to convert
            # data frame rdd to normal rdd. After validation the rdd is
            # converted back to dataframe rdd
            #
            # FIXME: find a way to apply filter function on dataframe rdd data
            validated_mon_metrics_rdd = filtered_metrics_df.rdd.filter(
                MonMetricsKafkaProcessor._validate_raw_mon_metrics)
            validated_mon_metrics_df = sql_context.createDataFrame(
                validated_mon_metrics_rdd, filtered_metrics_df.schema)

            #
            # record generator
            # generate a new intermediate metric record if a given metric
            # metric_id_list, in pre_transform_specs table has several
            # intermediate metrics defined.
            # intermediate metrics are used as a convenient way to
            # process (aggregated) metric in mutiple ways by making a copy
            # of the source data for each processing
            #
            gen_mon_metrics_df = validated_mon_metrics_df.select(
                validated_mon_metrics_df.meta,
                validated_mon_metrics_df.metric,
                validated_mon_metrics_df.event_processing_params,
                validated_mon_metrics_df.event_type,
                explode(validated_mon_metrics_df.metric_id_list).alias(
                    "this_metric_id"),
                validated_mon_metrics_df.service_id)

            #
            # transform metrics data to record_store format
            # record store format is the common format which will serve as
            # source to aggregation processing.
            # converting the metric to common standard format helps in writing
            # generic aggregation routines driven by configuration parameters
            #  and can be reused
            #
            record_store_df = gen_mon_metrics_df.select(
                (gen_mon_metrics_df.metric.timestamp / 1000).alias(
                    "event_timestamp_unix"),
                from_unixtime(
                    gen_mon_metrics_df.metric.timestamp / 1000).alias(
                    "event_timestamp_string"),
                gen_mon_metrics_df.event_type.alias("event_type"),
                gen_mon_metrics_df.event_type.alias("event_quantity_name"),
                (gen_mon_metrics_df.metric.value / 1.0).alias(
                    "event_quantity"),
                when(gen_mon_metrics_df.metric.dimensions.state != '',
                     gen_mon_metrics_df.metric.dimensions.state).otherwise(
                    'NA').alias("event_status"),
                lit('1.0').alias('event_version'),
                lit('metrics').alias("record_type"),

                # resource_uuid
                when(gen_mon_metrics_df.metric.dimensions.instanceId != '',
                     gen_mon_metrics_df.metric.dimensions.instanceId).when(
                    gen_mon_metrics_df.metric.dimensions.resource_id != '',
                    gen_mon_metrics_df.metric.dimensions.resource_id).
                otherwise('NA').alias("resource_uuid"),

                when(gen_mon_metrics_df.metric.dimensions.tenantId != '',
                     gen_mon_metrics_df.metric.dimensions.tenantId).when(
                    gen_mon_metrics_df.metric.dimensions.tenant_id != '',
                    gen_mon_metrics_df.metric.dimensions.tenant_id).when(
                    gen_mon_metrics_df.metric.dimensions.project_id != '',
                    gen_mon_metrics_df.metric.dimensions.project_id).otherwise(
                    'NA').alias("tenant_id"),

                when(gen_mon_metrics_df.metric.dimensions.mount != '',
                     gen_mon_metrics_df.metric.dimensions.mount).otherwise(
                    'NA').alias("mount"),

                when(gen_mon_metrics_df.metric.dimensions.device != '',
                     gen_mon_metrics_df.metric.dimensions.device).otherwise(
                    'NA').alias("device"),

                when(gen_mon_metrics_df.metric.dimensions.namespace != '',
                     gen_mon_metrics_df.metric.dimensions.namespace).otherwise(
                    'NA').alias("namespace"),

                when(gen_mon_metrics_df.metric.dimensions.pod_name != '',
                     gen_mon_metrics_df.metric.dimensions.pod_name).otherwise(
                    'NA').alias("pod_name"),

                when(gen_mon_metrics_df.metric.dimensions.container_name != '',
                     gen_mon_metrics_df.metric.dimensions
                     .container_name).otherwise('NA').alias("container_name"),

                when(gen_mon_metrics_df.metric.dimensions.app != '',
                     gen_mon_metrics_df.metric.dimensions.app).otherwise(
                    'NA').alias("app"),

                when(gen_mon_metrics_df.metric.dimensions.interface != '',
                     gen_mon_metrics_df.metric.dimensions.interface).otherwise(
                    'NA').alias("interface"),

                when(gen_mon_metrics_df.metric.dimensions.deployment != '',
                     gen_mon_metrics_df.metric.dimensions
                     .deployment).otherwise('NA').alias("deployment"),

                when(gen_mon_metrics_df.metric.dimensions.daemon_set != '',
                     gen_mon_metrics_df.metric.dimensions
                     .daemon_set).otherwise('NA').alias("daemon_set"),

                when(gen_mon_metrics_df.meta.userId != '',
                     gen_mon_metrics_df.meta.userId).otherwise('NA').alias(
                    "user_id"),

                when(gen_mon_metrics_df.meta.region != '',
                     gen_mon_metrics_df.meta.region).when(
                    gen_mon_metrics_df.event_processing_params
                    .set_default_region_to != '',
                    gen_mon_metrics_df.event_processing_params
                    .set_default_region_to).otherwise(
                    'NA').alias("region"),

                when(gen_mon_metrics_df.meta.zone != '',
                     gen_mon_metrics_df.meta.zone).when(
                    gen_mon_metrics_df.event_processing_params
                    .set_default_zone_to != '',
                    gen_mon_metrics_df.event_processing_params
                    .set_default_zone_to).otherwise(
                    'NA').alias("zone"),

                when(gen_mon_metrics_df.metric.dimensions.hostname != '',
                     gen_mon_metrics_df.metric.dimensions.hostname).when(
                    gen_mon_metrics_df.metric.value_meta.host != '',
                    gen_mon_metrics_df.metric.value_meta.host).otherwise(
                    'NA').alias("host"),

                when(gen_mon_metrics_df.service_id != '',
                     gen_mon_metrics_df.service_id).otherwise(
                    'NA').alias("service_group"),

                when(gen_mon_metrics_df.service_id != '',
                     gen_mon_metrics_df.service_id).otherwise(
                    'NA').alias("service_id"),

                from_unixtime(gen_mon_metrics_df.metric.timestamp / 1000,
                              'yyyy-MM-dd').alias("event_date"),
                from_unixtime(gen_mon_metrics_df.metric.timestamp / 1000,
                              'HH').alias("event_hour"),
                from_unixtime(gen_mon_metrics_df.metric.timestamp / 1000,
                              'mm').alias("event_minute"),
                from_unixtime(gen_mon_metrics_df.metric.timestamp / 1000,
                              'ss').alias("event_second"),
                gen_mon_metrics_df.this_metric_id.alias("metric_group"),
                gen_mon_metrics_df.this_metric_id.alias("metric_id"))

            #
            # get transform context
            #
            rdd_transform_context = rdd_transform_context_rdd.first()
            transform_context = rdd_transform_context.transform_context_info

            #
            # cache record store rdd
            #
            if cfg.CONF.service.enable_record_store_df_cache:
                storage_level_prop = \
                    cfg.CONF.service.record_store_df_cache_storage_level
                try:
                    storage_level = StorageUtils.get_storage_level(
                        storage_level_prop)
                except InvalidCacheStorageLevelException as storage_error:
                    storage_error.value += \
                        " (as specified in " \
                        "service.record_store_df_cache_storage_level)"
                    raise
                record_store_df.persist(storage_level)

            #
            # start processing metrics available in record_store data
            #
            MonMetricsKafkaProcessor.process_metrics(transform_context,
                                                     record_store_df)

            # remove df from cache
            if cfg.CONF.service.enable_record_store_df_cache:
                record_store_df.unpersist()

            #
            # extract kafka offsets and batch processing time
            # stored in transform_context and save offsets
            #
            offsets = transform_context.offset_info

            # batch time
            batch_time_info = \
                transform_context.batch_time_info

            MonMetricsKafkaProcessor.save_kafka_offsets(
                offsets, rdd_transform_context_rdd.context.appName,
                batch_time_info)

            # call pre hourly processor, if its time to run
            if (cfg.CONF.stage_processors.pre_hourly_processor_enabled and
                    PreHourlyProcessor.is_time_to_run(batch_time_info)):
                PreHourlyProcessor.run_processor(
                    record_store_df.rdd.context,
                    batch_time_info)

    @staticmethod
    def transform_to_recordstore(kvs):
        """Transform metrics data from kafka to record store format.
        extracts, validates, filters, generates data from kakfa to only keep
        data that has to be aggregated. Generate data generates multiple
        records for for the same incoming metric if the metric has multiple
        intermediate metrics defined, so that each of intermediate metrics can
        be potentially processed independently.
        """
        # save offsets in global var myOffsetRanges
        # http://spark.apache.org/docs/latest/streaming-kafka-integration.html
        # Note that the typecast to HasOffsetRanges will only succeed if it is
        #  done in the first method called on the directKafkaStream, not later
        #  down a chain of methods. You can use transform() instead of
        # foreachRDD() as your first method call in order to access offsets,
        #  then call further Spark methods. However, be aware that the
        # one-to-one mapping between RDD partition and Kafka partition does not
        #  remain after any methods that shuffle or repartition,
        # e.g. reduceByKey() or window()
        kvs.transform(
            MonMetricsKafkaProcessor.store_offset_ranges
        ).foreachRDD(MonMetricsKafkaProcessor.rdd_to_recordstore)


def invoke():
    # object to keep track of offsets
    ConfigInitializer.basic_config()

    # app name
    application_name = "mon_metrics_kafka"

    my_spark_conf = SparkConf().setAppName(application_name)

    spark_context = SparkContext(conf=my_spark_conf)

    # read at the configured interval
    spark_streaming_context = \
        StreamingContext(spark_context, cfg.CONF.service.stream_interval)

    kafka_stream = MonMetricsKafkaProcessor.get_kafka_stream(
        cfg.CONF.messaging.topic,
        spark_streaming_context)

    # transform to recordstore
    MonMetricsKafkaProcessor.transform_to_recordstore(kafka_stream)

    # catch interrupt, stop streaming context gracefully
    # signal.signal(signal.SIGINT, signal_handler)

    # start processing
    spark_streaming_context.start()

    # FIXME: stop spark context to relinquish resources

    # FIXME: specify cores, so as not to use all the resources on the cluster.

    # FIXME: HA deploy multiple masters, may be one on each control node

    try:
        # Wait for the Spark driver to "finish"
        spark_streaming_context.awaitTermination()
    except Exception as e:
        MonMetricsKafkaProcessor.log_debug(
            "Exception raised during Spark execution : " + str(e))
        # One exception that can occur here is the result of the saved
        # kafka offsets being obsolete/out of range.  Delete the saved
        # offsets to improve the chance of success on the next execution.

        # TODO(someone) prevent deleting all offsets for an application,
        # but just the latest revision
        MonMetricsKafkaProcessor.log_debug(
            "Deleting saved offsets for chance of success on next execution")

        MonMetricsKafkaProcessor.reset_kafka_offsets(application_name)

        # delete pre hourly processor offsets
        if cfg.CONF.stage_processors.pre_hourly_processor_enabled:
            PreHourlyProcessor.reset_kafka_offsets()

if __name__ == "__main__":
    invoke()
