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
from monasca_common.kafka_lib.client import KafkaClient
from monasca_common.kafka_lib.common import OffsetRequest

from pyspark.sql import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange

import datetime
import logging
from monasca_common.simport import simport
from oslo_config import cfg

from monasca_transform.component.insert.kafka_insert import KafkaInsert
from monasca_transform.component.setter.pre_hourly_calculate_rate import \
    PreHourlyCalculateRate
from monasca_transform.component.setter.rollup_quantity import RollupQuantity
from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepo
from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepoFactory
from monasca_transform.log_utils import LogUtils
from monasca_transform.processor import Processor
from monasca_transform.processor.processor_util import PreHourlyProcessorUtil
from monasca_transform.processor.processor_util import ProcessUtilDataProvider
from monasca_transform.transform.storage_utils import \
    InvalidCacheStorageLevelException
from monasca_transform.transform.storage_utils import StorageUtils
from monasca_transform.transform.transform_utils import InstanceUsageUtils
from monasca_transform.transform import TransformContextUtils

ConfigInitializer.basic_config()
log = LogUtils.init_logger(__name__)


class PreHourlyProcessorDataProvider(ProcessUtilDataProvider):

    def get_last_processed(self):
        offset_specifications = PreHourlyProcessor.get_offset_specs()
        app_name = PreHourlyProcessor.get_app_name()
        topic = PreHourlyProcessor.get_kafka_topic()
        most_recent_batch_time = (
            offset_specifications.get_most_recent_batch_time_from_offsets(
                app_name, topic))
        return most_recent_batch_time


class PreHourlyProcessor(Processor):
    """Processor to process usage data published to metrics_pre_hourly topic a
    and publish final rolled up metrics to metrics topic in kafka.
    """

    @staticmethod
    def save_kafka_offsets(current_offsets,
                           batch_time_info):
        """save current offsets to offset specification."""

        offset_specs = simport.load(cfg.CONF.repositories.offsets)()

        app_name = PreHourlyProcessor.get_app_name()

        for o in current_offsets:
            log.debug(
                "saving: OffSetRanges: %s %s %s %s, "
                "batch_time_info: %s" % (
                    o.topic, o.partition, o.fromOffset, o.untilOffset,
                    str(batch_time_info)))
        # add new offsets, update revision
        offset_specs.add_all_offsets(app_name,
                                     current_offsets,
                                     batch_time_info)

    @staticmethod
    def reset_kafka_offsets():
        """delete all offsets from the offset specification."""

        app_name = PreHourlyProcessor.get_app_name()

        # get the offsets from global var
        offset_specs = simport.load(cfg.CONF.repositories.offsets)()
        offset_specs.delete_all_kafka_offsets(app_name)

    @staticmethod
    def get_app_name():
        """get name of this application. Will be used to
        store offsets in database
        """
        return "mon_metrics_kafka_pre_hourly"

    @staticmethod
    def get_kafka_topic():
        """get name of kafka topic for transformation."""
        return "metrics_pre_hourly"

    @staticmethod
    def is_time_to_run(check_time):
        return PreHourlyProcessorUtil.is_time_to_run(check_time)

    @staticmethod
    def _get_offsets_from_kafka(brokers,
                                topic,
                                offset_time):
        """get dict representing kafka
        offsets.
        """
        # get client
        client = KafkaClient(brokers)

        # get partitions for a topic
        partitions = client.topic_partitions[topic]

        # https://cwiki.apache.org/confluence/display/KAFKA/
        # A+Guide+To+The+Kafka+Protocol#
        # AGuideToTheKafkaProtocol-OffsetRequest
        MAX_OFFSETS = 1
        offset_requests = [OffsetRequest(topic,
                                         part_name,
                                         offset_time,
                                         MAX_OFFSETS) for part_name
                           in partitions.keys()]

        offsets_responses = client.send_offset_request(offset_requests)

        offset_dict = {}
        for response in offsets_responses:
            key = "_".join((response.topic,
                            str(response.partition)))
            offset_dict[key] = response

        return offset_dict

    @staticmethod
    def _parse_saved_offsets(app_name, topic, saved_offset_spec):
        """get dict representing saved
        offsets.
        """
        offset_dict = {}
        for key, value in saved_offset_spec.items():
            if key.startswith("%s_%s" % (app_name, topic)):
                spec_app_name = value.get_app_name()
                spec_topic = value.get_topic()
                spec_partition = int(value.get_partition())
                spec_from_offset = value.get_from_offset()
                spec_until_offset = value.get_until_offset()
                key = "_".join((spec_topic,
                                str(spec_partition)))
                offset_dict[key] = (spec_app_name,
                                    spec_topic,
                                    spec_partition,
                                    spec_from_offset,
                                    spec_until_offset)
        return offset_dict

    @staticmethod
    def _get_new_offset_range_list(brokers, topic):
        """get offset range from earliest to latest."""
        offset_range_list = []

        # https://cwiki.apache.org/confluence/display/KAFKA/
        # A+Guide+To+The+Kafka+Protocol#
        # AGuideToTheKafkaProtocol-OffsetRequest
        GET_LATEST_OFFSETS = -1
        latest_dict = PreHourlyProcessor._get_offsets_from_kafka(
            brokers, topic, GET_LATEST_OFFSETS)

        GET_EARLIEST_OFFSETS = -2
        earliest_dict = PreHourlyProcessor._get_offsets_from_kafka(
            brokers, topic, GET_EARLIEST_OFFSETS)

        for item in latest_dict:
            until_offset = latest_dict[item].offsets[0]
            from_offset = earliest_dict[item].offsets[0]
            partition = latest_dict[item].partition
            topic = latest_dict[item].topic
            offset_range_list.append(OffsetRange(topic,
                                                 partition,
                                                 from_offset,
                                                 until_offset))

        return offset_range_list

    @staticmethod
    def _get_offset_range_list(brokers,
                               topic,
                               app_name,
                               saved_offset_spec):
        """get offset range from saved offset to latest.
        """
        offset_range_list = []

        # https://cwiki.apache.org/confluence/display/KAFKA/
        # A+Guide+To+The+Kafka+Protocol#
        # AGuideToTheKafkaProtocol-OffsetRequest
        GET_LATEST_OFFSETS = -1
        latest_dict = PreHourlyProcessor._get_offsets_from_kafka(
            brokers, topic, GET_LATEST_OFFSETS)

        GET_EARLIEST_OFFSETS = -2
        earliest_dict = PreHourlyProcessor._get_offsets_from_kafka(
            brokers, topic, GET_EARLIEST_OFFSETS)

        saved_dict = PreHourlyProcessor._parse_saved_offsets(
            app_name, topic, saved_offset_spec)

        for item in latest_dict:
            # saved spec
            (spec_app_name,
             spec_topic_name,
             spec_partition,
             spec_from_offset,
             spec_until_offset) = saved_dict[item]

            # until
            until_offset = latest_dict[item].offsets[0]

            # from
            if spec_until_offset is not None and int(spec_until_offset) >= 0:
                from_offset = spec_until_offset
            else:
                from_offset = earliest_dict[item].offsets[0]

            partition = latest_dict[item].partition
            topic = latest_dict[item].topic
            offset_range_list.append(OffsetRange(topic,
                                                 partition,
                                                 from_offset,
                                                 until_offset))

        return offset_range_list

    @staticmethod
    def get_processing_offset_range_list(processing_time):
        """get offset range to fetch data from. The
        range will last from the last saved offsets to current offsets
        available. If there are no last saved offsets available in the
        database the starting offsets will be set to the earliest
        available in kafka.
        """

        offset_specifications = PreHourlyProcessor.get_offset_specs()

        # get application name, will be used to get offsets from database
        app_name = PreHourlyProcessor.get_app_name()

        saved_offset_spec = offset_specifications.get_kafka_offsets(app_name)

        # get kafka topic to fetch data
        topic = PreHourlyProcessor.get_kafka_topic()

        if len(saved_offset_spec) < 1:

            log.debug(
                "No saved offsets available..."
                "connecting to kafka and fetching "
                "from earliest available offset ...")

            offset_range_list = PreHourlyProcessor._get_new_offset_range_list(
                cfg.CONF.messaging.brokers,
                topic)
        else:
            log.debug(
                "Saved offsets available..."
                "connecting to kafka and fetching from saved offset ...")

            offset_range_list = PreHourlyProcessor._get_offset_range_list(
                cfg.CONF.messaging.brokers,
                topic,
                app_name,
                saved_offset_spec)
        return offset_range_list

    @staticmethod
    def get_offset_specs():
        """get offset specifications.
        """
        return simport.load(cfg.CONF.repositories.offsets)()

    @staticmethod
    def get_effective_offset_range_list(offset_range_list):
        """get effective batch offset range.
        Effective batch offset range covers offsets starting
        from effective batch revision (defined by effective_batch_revision
        config property). By default this method will set the
        pyspark Offset.fromOffset for each partition
        to have value older than the latest revision
        (defaults to latest -1) so that prehourly processor has access
        to entire data for the hour. This will also account for and cover
        any early arriving data (data that arrives before the start hour).
        """

        offset_specifications = PreHourlyProcessor.get_offset_specs()

        app_name = PreHourlyProcessor.get_app_name()

        topic = PreHourlyProcessor.get_kafka_topic()

        # start offset revision
        effective_batch_revision = cfg.CONF.pre_hourly_processor.\
            effective_batch_revision

        effective_batch_spec = offset_specifications\
            .get_kafka_offsets_by_revision(app_name,
                                           effective_batch_revision)

        # get latest revision, if penultimate is unavailable
        if not effective_batch_spec:
            log.debug("effective batch spec: offsets: revision %s unavailable,"
                      " getting the latest revision instead..." % (
                          effective_batch_revision))
            # not available
            effective_batch_spec = offset_specifications.get_kafka_offsets(
                app_name)

        effective_batch_offsets = PreHourlyProcessor._parse_saved_offsets(
            app_name, topic,
            effective_batch_spec)

        # for debugging
        for effective_key in effective_batch_offsets.keys():
            effective_offset = effective_batch_offsets.get(effective_key,
                                                           None)
            (effect_app_name,
             effect_topic_name,
             effect_partition,
             effect_from_offset,
             effect_until_offset) = effective_offset
            log.debug(
                "effective batch offsets (from db):"
                " OffSetRanges: %s %s %s %s" % (
                    effect_topic_name, effect_partition,
                    effect_from_offset, effect_until_offset))

        # effective batch revision
        effective_offset_range_list = []
        for offset_range in offset_range_list:
            part_topic_key = "_".join((offset_range.topic,
                                       str(offset_range.partition)))
            effective_offset = effective_batch_offsets.get(part_topic_key,
                                                           None)
            if effective_offset:
                (effect_app_name,
                 effect_topic_name,
                 effect_partition,
                 effect_from_offset,
                 effect_until_offset) = effective_offset

                log.debug(
                    "Extending effective offset range:"
                    " OffSetRanges: %s %s %s-->%s %s" % (
                        effect_topic_name, effect_partition,
                        offset_range.fromOffset,
                        effect_from_offset,
                        effect_until_offset))

                effective_offset_range_list.append(
                    OffsetRange(offset_range.topic,
                                offset_range.partition,
                                effect_from_offset,
                                offset_range.untilOffset))
            else:
                effective_offset_range_list.append(
                    OffsetRange(offset_range.topic,
                                offset_range.partition,
                                offset_range.fromOffset,
                                offset_range.untilOffset))

        # return effective offset range list
        return effective_offset_range_list

    @staticmethod
    def fetch_pre_hourly_data(spark_context,
                              offset_range_list):
        """get metrics pre hourly data from offset range list."""

        for o in offset_range_list:
            log.debug(
                "fetch_pre_hourly: offset_range_list:"
                " OffSetRanges: %s %s %s %s" % (
                    o.topic, o.partition, o.fromOffset, o.untilOffset))

        effective_offset_list = PreHourlyProcessor.\
            get_effective_offset_range_list(offset_range_list)

        for o in effective_offset_list:
            log.debug(
                "fetch_pre_hourly: effective_offset_range_list:"
                " OffSetRanges: %s %s %s %s" % (
                    o.topic, o.partition, o.fromOffset, o.untilOffset))

        # get kafka stream over the same offsets
        pre_hourly_rdd = KafkaUtils.createRDD(spark_context,
                                              {"metadata.broker.list":
                                                  cfg.CONF.messaging.brokers},
                                              effective_offset_list)
        return pre_hourly_rdd

    @staticmethod
    def pre_hourly_to_instance_usage_df(pre_hourly_rdd):
        """convert raw pre hourly data into instance usage dataframe."""
        #
        # extract second column containing instance usage data
        #
        instance_usage_rdd = pre_hourly_rdd.map(
            lambda iud: iud[1])

        #
        # convert usage data rdd to instance usage df
        #
        sqlc = SQLContext.getOrCreate(pre_hourly_rdd.context)
        instance_usage_df = InstanceUsageUtils.create_df_from_json_rdd(
            sqlc, instance_usage_rdd)

        if cfg.CONF.pre_hourly_processor.enable_batch_time_filtering:
            instance_usage_df = (
                PreHourlyProcessor.filter_out_records_not_in_current_batch(
                    instance_usage_df))

        return instance_usage_df

    @staticmethod
    def filter_out_records_not_in_current_batch(instance_usage_df):
        """Filter out any records which don't pertain to the
        current batch (i.e., records before or after the
        batch currently being processed).
        """
        # get the most recent batch time from the stored offsets

        offset_specifications = PreHourlyProcessor.get_offset_specs()
        app_name = PreHourlyProcessor.get_app_name()
        topic = PreHourlyProcessor.get_kafka_topic()
        most_recent_batch_time = (
            offset_specifications.get_most_recent_batch_time_from_offsets(
                app_name, topic))

        if most_recent_batch_time:
            # batches can fire after late metrics slack time, not neccessarily
            # at the top of the hour
            most_recent_batch_time_truncated = most_recent_batch_time.replace(
                minute=0, second=0, microsecond=0)
            log.debug("filter out records before : %s" % (
                most_recent_batch_time_truncated.strftime(
                    '%Y-%m-%dT%H:%M:%S')))
            # filter out records before current batch
            instance_usage_df = instance_usage_df.filter(
                instance_usage_df.lastrecord_timestamp_string >=
                most_recent_batch_time_truncated)

        # determine the timestamp of the most recent top-of-the-hour (which
        # is the end of the current batch).
        current_time = datetime.datetime.now()
        truncated_timestamp_to_current_hour = current_time.replace(
            minute=0, second=0, microsecond=0)

        # filter out records after current batch
        log.debug("filter out records after : %s" % (
            truncated_timestamp_to_current_hour.strftime(
                '%Y-%m-%dT%H:%M:%S')))
        instance_usage_df = instance_usage_df.filter(
            instance_usage_df.firstrecord_timestamp_string <
            truncated_timestamp_to_current_hour)

        return instance_usage_df

    @staticmethod
    def process_instance_usage(transform_context, instance_usage_df):
        """second stage aggregation. Aggregate instance usage rdd
        data and write results to metrics topic in kafka.
        """

        transform_spec_df = transform_context.transform_spec_df_info

        #
        # do a rollup operation
        #
        agg_params = (transform_spec_df.select(
            "aggregation_params_map.pre_hourly_group_by_list")
            .collect()[0].asDict())
        pre_hourly_group_by_list = agg_params["pre_hourly_group_by_list"]

        if (len(pre_hourly_group_by_list) == 1 and
                pre_hourly_group_by_list[0] == "default"):
            pre_hourly_group_by_list = ["tenant_id", "user_id",
                                        "resource_uuid",
                                        "geolocation", "region", "zone",
                                        "host", "project_id",
                                        "aggregated_metric_name",
                                        "aggregation_period"]

        # get aggregation period
        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_period").collect()[0].asDict()
        aggregation_period = agg_params["aggregation_period"]

        # get 2stage operation
        agg_params = (transform_spec_df.select(
            "aggregation_params_map.pre_hourly_operation")
            .collect()[0].asDict())
        pre_hourly_operation = agg_params["pre_hourly_operation"]

        if pre_hourly_operation != "rate":
            instance_usage_df = RollupQuantity.do_rollup(
                pre_hourly_group_by_list, aggregation_period,
                pre_hourly_operation, instance_usage_df)
        else:
            instance_usage_df = PreHourlyCalculateRate.do_rate_calculation(
                instance_usage_df)

        # insert metrics
        instance_usage_df = KafkaInsert.insert(transform_context,
                                               instance_usage_df)
        return instance_usage_df

    @staticmethod
    def do_transform(instance_usage_df):
        """start processing (aggregating) metrics
        """
        #
        # look in instance_usage_df for list of metrics to be processed
        #
        metric_ids_df = instance_usage_df.select(
            "processing_meta.metric_id").distinct()

        metric_ids_to_process = [row.metric_id
                                 for row in metric_ids_df.collect()]

        data_driven_specs_repo = (
            DataDrivenSpecsRepoFactory.get_data_driven_specs_repo())
        sqlc = SQLContext.getOrCreate(instance_usage_df.rdd.context)
        transform_specs_df = data_driven_specs_repo.get_data_driven_specs(
            sql_context=sqlc,
            data_driven_spec_type=DataDrivenSpecsRepo.transform_specs_type)

        for metric_id in metric_ids_to_process:
            transform_spec_df = transform_specs_df.select(
                ["aggregation_params_map", "metric_id"]
            ).where(transform_specs_df.metric_id == metric_id)
            source_instance_usage_df = instance_usage_df.select("*").where(
                instance_usage_df.processing_meta.metric_id == metric_id)

            # set transform_spec_df in TransformContext
            transform_context = TransformContextUtils.get_context(
                transform_spec_df_info=transform_spec_df)

            agg_inst_usage_df = PreHourlyProcessor.process_instance_usage(
                transform_context, source_instance_usage_df)

            # if running in debug mode, write out the aggregated metric
            # name just processed (along with the count of how many of these
            # were aggregated) to the application log.
            if log.isEnabledFor(logging.DEBUG):
                agg_inst_usage_collection = agg_inst_usage_df.collect()
                collection_len = len(agg_inst_usage_collection)
                if collection_len > 0:
                    agg_inst_usage_dict = agg_inst_usage_collection[0].asDict()
                    log.debug("Submitted hourly aggregated metric: %s (%s)",
                              agg_inst_usage_dict["aggregated_metric_name"],
                              str(collection_len))

    @staticmethod
    def run_processor(spark_context, processing_time):
        """process data in metrics_pre_hourly queue, starting
           from the last saved offsets, else start from earliest
           offsets available
           """

        offset_range_list = (
            PreHourlyProcessor.get_processing_offset_range_list(
                processing_time))

        # get pre hourly data
        pre_hourly_rdd = PreHourlyProcessor.fetch_pre_hourly_data(
            spark_context, offset_range_list)

        # get instance usage df
        instance_usage_df = PreHourlyProcessor.pre_hourly_to_instance_usage_df(
            pre_hourly_rdd)

        #
        # cache instance usage df
        #
        if cfg.CONF.pre_hourly_processor.enable_instance_usage_df_cache:
            storage_level_prop = (
                cfg.CONF.pre_hourly_processor
                .instance_usage_df_cache_storage_level)
            try:
                storage_level = StorageUtils.get_storage_level(
                    storage_level_prop)
            except InvalidCacheStorageLevelException as storage_error:
                storage_error.value += (" (as specified in "
                                        "pre_hourly_processor"
                                        ".instance_usage_df_cache"
                                        "_storage_level)")
                raise
            instance_usage_df.persist(storage_level)

        # aggregate pre hourly data
        PreHourlyProcessor.do_transform(instance_usage_df)

        # remove cache
        if cfg.CONF.pre_hourly_processor.enable_instance_usage_df_cache:
            instance_usage_df.unpersist()

        # save latest metrics_pre_hourly offsets in the database
        PreHourlyProcessor.save_kafka_offsets(offset_range_list,
                                              processing_time)
