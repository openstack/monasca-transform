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
import datetime
import os
import random
import sys
import unittest
import uuid

from collections import defaultdict

import mock
from oslo_config import cfg
from oslo_utils import uuidutils
from pyspark.streaming.kafka import OffsetRange

from monasca_common.kafka_lib.common import OffsetResponse

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.mysql_offset_specs import MySQLOffsetSpecs
from monasca_transform.processor.pre_hourly_processor import PreHourlyProcessor

from tests.functional.component.insert.dummy_insert import DummyInsert
from tests.functional.json_offset_specs import JSONOffsetSpecs
from tests.functional.messaging.adapter import DummyAdapter
from tests.functional.spark_context_test import SparkContextTest
from tests.functional.test_resources.metrics_pre_hourly_data.data_provider \
    import DataProvider


class TestPreHourlyProcessorAgg(SparkContextTest):

    test_resources_path = 'tests/functional/test_resources'

    def setUp(self):
        super(TestPreHourlyProcessorAgg, self).setUp()
        # configure the system with a dummy messaging adapter
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/functional/test_resources/config/'
                'test_config_with_dummy_messaging_adapter.conf'])
        # reset metric_id list dummy adapter
        if not DummyAdapter.adapter_impl:
            DummyAdapter.init()
        DummyAdapter.adapter_impl.metric_list = []

        # get mysql offset specs
        self.kafka_offset_specs = MySQLOffsetSpecs()

    def add_offset_for_test(self, my_app, my_topic, my_partition,
                            my_from_offset, my_until_offset,
                            my_batch_time):
        """"utility method to populate mysql db with offsets."""
        self.kafka_offset_specs.add(topic=my_topic, partition=my_partition,
                                    app_name=my_app,
                                    from_offset=my_from_offset,
                                    until_offset=my_until_offset,
                                    batch_time_info=my_batch_time)

    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.get_app_name')
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.get_kafka_topic')
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor._get_offsets_from_kafka')
    def test_get_processing_offset_range_list(self,
                                              kafka_get_offsets,
                                              kafka_topic_name,
                                              app_name):

        # setup
        my_app = uuidutils.generate_uuid()
        my_topic = uuidutils.generate_uuid()

        # mock app_name, topic_name, partition
        app_name.return_value = my_app
        kafka_topic_name.return_value = my_topic
        my_partition = 1

        ret_offset_key = "_".join((my_topic, str(my_partition)))
        kafka_get_offsets.side_effect = [
            # mock latest offsets
            {ret_offset_key: OffsetResponse(topic=my_topic,
                                            partition=my_partition,
                                            error=None,
                                            offsets=[30])},
            # mock earliest offsets
            {ret_offset_key: OffsetResponse(topic=my_topic,
                                            partition=my_partition,
                                            error=None,
                                            offsets=[0])}
        ]

        # add offsets
        my_until_offset = 0
        my_from_offset = 10
        my_batch_time = datetime.datetime.strptime('2016-01-01 00:10:00',
                                                   '%Y-%m-%d %H:%M:%S')
        self.add_offset_for_test(my_app, my_topic,
                                 my_partition, my_until_offset,
                                 my_from_offset, my_batch_time)

        my_until_offset_2 = 10
        my_from_offset_2 = 20
        my_batch_time_2 = datetime.datetime.strptime('2016-01-01 01:10:00',
                                                     '%Y-%m-%d %H:%M:%S')
        self.add_offset_for_test(my_app, my_topic,
                                 my_partition, my_until_offset_2,
                                 my_from_offset_2, my_batch_time_2)

        # get latest offset spec as dict
        current_batch_time = datetime.datetime.strptime('2016-01-01 02:10:00',
                                                        '%Y-%m-%d %H:%M:%S')

        # use mysql offset repositories
        cfg.CONF.set_override(
            'offsets',
            'monasca_transform.mysql_offset_specs:MySQLOffsetSpecs',
            group='repositories')

        # list of pyspark.streaming.kafka.OffsetRange objects
        offset_range_list = PreHourlyProcessor.\
            get_processing_offset_range_list(
                current_batch_time)

        self.assertEqual(my_partition,
                         offset_range_list[0].partition)
        self.assertEqual(my_topic,
                         offset_range_list[0].topic)
        self.assertEqual(20,
                         offset_range_list[0].fromOffset)
        self.assertEqual(30,
                         offset_range_list[0].untilOffset)

    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.get_app_name')
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.get_kafka_topic')
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor._get_offsets_from_kafka')
    def test_get_effective_offset_range_list(self,
                                             kafka_get_offsets,
                                             kafka_topic_name,
                                             app_name):
        # setup
        my_app = uuidutils.generate_uuid()
        my_topic = uuidutils.generate_uuid()

        # mock app_name, topic_name, partition
        app_name.return_value = my_app
        kafka_topic_name.return_value = my_topic
        my_partition = 1

        ret_offset_key = "_".join((my_topic, str(my_partition)))
        kafka_get_offsets.side_effect = [
            # mock latest offsets in kafka
            {ret_offset_key: OffsetResponse(topic=my_topic,
                                            partition=my_partition,
                                            error=None,
                                            offsets=[3000])},
            # mock earliest offsets in kafka
            {ret_offset_key: OffsetResponse(topic=my_topic,
                                            partition=my_partition,
                                            error=None,
                                            offsets=[0])}
        ]

        # add offsets
        my_until_offset = 500
        my_from_offset = 1000
        my_batch_time = datetime.datetime.strptime('2016-01-01 00:10:00',
                                                   '%Y-%m-%d %H:%M:%S')
        self.add_offset_for_test(my_app, my_topic,
                                 my_partition, my_until_offset,
                                 my_from_offset, my_batch_time)

        my_until_offset_2 = 1000
        my_from_offset_2 = 2000
        my_batch_time_2 = datetime.datetime.strptime('2016-01-01 01:10:00',
                                                     '%Y-%m-%d %H:%M:%S')
        self.add_offset_for_test(my_app, my_topic,
                                 my_partition, my_until_offset_2,
                                 my_from_offset_2, my_batch_time_2)

        # get latest offset spec as dict
        current_batch_time = datetime.datetime.strptime('2016-01-01 02:10:00',
                                                        '%Y-%m-%d %H:%M:%S')

        # use mysql offset repositories
        cfg.CONF.set_override(
            'offsets',
            'monasca_transform.mysql_offset_specs:MySQLOffsetSpecs',
            group='repositories')

        # list of pyspark.streaming.kafka.OffsetRange objects
        offset_range_list = PreHourlyProcessor.\
            get_processing_offset_range_list(
                current_batch_time)

        # effective batch range list
        # should cover range of starting from  (latest - 1) offset version to
        # latest
        offset_range_list = PreHourlyProcessor.get_effective_offset_range_list(
            offset_range_list)

        self.assertEqual(my_partition,
                         offset_range_list[0].partition)
        self.assertEqual(my_topic,
                         offset_range_list[0].topic)
        self.assertEqual(500,
                         offset_range_list[0].fromOffset)
        self.assertEqual(3000,
                         offset_range_list[0].untilOffset)

    @mock.patch('monasca_transform.processor.pre_hourly_processor.KafkaInsert',
                DummyInsert)
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.get_offset_specs')
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.fetch_pre_hourly_data')
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.get_processing_offset_range_list')
    def test_pre_hourly_processor(self,
                                  offset_range_list,
                                  pre_hourly_data,
                                  offset_specs):

        # load components
        myOffsetRanges = [
            OffsetRange("metrics_pre_hourly", 0, 10, 20)]
        offset_range_list.return_value = myOffsetRanges

        filename = '%s.json' % str(uuid.uuid4())
        file_path = os.path.join(self.test_resources_path, filename)
        json_offset_specs = JSONOffsetSpecs(
            path=self.test_resources_path,
            filename=filename
        )
        app_name = "mon_metrics_kafka_pre_hourly"
        topic = "metrics_pre_hourly"
        partition = 0
        until_offset = random.randint(0, sys.maxsize)
        from_offset = random.randint(0, sys.maxsize)

        my_batch_time = self.get_dummy_batch_time()

        json_offset_specs.add(topic=topic, partition=partition,
                              app_name=app_name,
                              from_offset=from_offset,
                              until_offset=until_offset,
                              batch_time_info=my_batch_time)

        offset_specs.return_value = json_offset_specs

        # Create an RDD out of the mocked instance usage data
        with open(DataProvider.metrics_pre_hourly_data_path) as f:
            raw_lines = f.read().splitlines()
        raw_tuple_list = [eval(raw_line) for raw_line in raw_lines]
        pre_hourly_rdd_data = self.spark_context.parallelize(raw_tuple_list)
        pre_hourly_data.return_value = pre_hourly_rdd_data

        # Do something simple with the RDD
        result = self.simple_count_transform(pre_hourly_rdd_data)

        # run pre hourly processor
        PreHourlyProcessor.run_processor(
            self.spark_context, self.get_dummy_batch_time())

        # get the metrics that have been submitted to the dummy message adapter
        metrics = DummyAdapter.adapter_impl.metric_list

        # Verify count of instance usage data
        self.assertEqual(result, 57)

        # make a dictionary of all metrics so that they can be directly
        # referenced in the verifications that follow
        metrics_dict = defaultdict(list)
        for metric in metrics:
            metric_name = metric.get('metric').get('name')
            metrics_dict[metric_name].append(metric)

        # Verify mem.total_mb_agg metrics for all hosts
        metric_list = metrics_dict['mem.total_mb_agg']
        mem_total_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(mem_total_mb_agg_metric is not None)
        self.assertEqual(16049.0,
                         mem_total_mb_agg_metric
                         .get('metric').get('value'))
        # agg meta
        self.assertEqual("2016-06-20 11:49:44",
                         mem_total_mb_agg_metric
                         .get("metric")
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual("2016-06-20 11:24:59",
                         mem_total_mb_agg_metric
                         .get("metric")
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(60.0,
                         mem_total_mb_agg_metric
                         .get("metric")
                         .get('value_meta').get('record_count'))

        # Verify mem.usable_mb_agg metrics for all hosts
        metric_list = metrics_dict['mem.usable_mb_agg']
        mem_usable_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(mem_total_mb_agg_metric is not None)
        self.assertEqual(10283.1,
                         mem_usable_mb_agg_metric
                         .get('metric').get('value'))
        # agg meta
        self.assertEqual("2016-06-20 11:49:44",
                         mem_usable_mb_agg_metric
                         .get("metric")
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual("2016-06-20 11:24:59",
                         mem_usable_mb_agg_metric
                         .get("metric")
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(60.0,
                         mem_usable_mb_agg_metric
                         .get("metric")
                         .get('value_meta').get('record_count'))

        # Verify swiftlm.diskusage.rate_agg metrics
        swift_disk_rate_agg_metric = metrics_dict[
            'swiftlm.diskusage.rate_agg'][0]

        self.assertTrue(swift_disk_rate_agg_metric is not None)
        self.assertEqual(37.25140584281991,
                         swift_disk_rate_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-10 20:27:02',
                         swift_disk_rate_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         swift_disk_rate_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(68.0,
                         swift_disk_rate_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         swift_disk_rate_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         swift_disk_rate_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         swift_disk_rate_agg_metric.get('metric')
                         .get('dimensions').get('host'))
        self.assertEqual('all',
                         swift_disk_rate_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         swift_disk_rate_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total pod.net.in_bytes_sec_agg metrics
        metric_list = metrics_dict['pod.net.in_bytes_sec_agg']
        pod_net_in_bytes_sec_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('app') ==
            'all'][0]

        self.assertTrue(pod_net_in_bytes_sec_agg_metric is not None)
        self.assertEqual(75.0,
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(6.0,
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_net_in_bytes_sec_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_in_bytes_sec_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('deployment'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('interface'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('namespace'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('pod_name'))
        self.assertEqual('hourly',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify pod.net.in_bytes_sec_agg metrics for wordpress app
        metric_list = metrics_dict['pod.net.in_bytes_sec_agg']
        pod_net_in_bytes_sec_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('app') ==
            'wordpress'][0]

        self.assertTrue(pod_net_in_bytes_sec_agg_metric is not None)
        self.assertEqual(175.0,
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(14.0,
                         pod_net_in_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_net_in_bytes_sec_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_in_bytes_sec_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('deployment'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('interface'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('namespace'))
        self.assertEqual('all',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('pod_name'))
        self.assertEqual('hourly',
                         pod_net_in_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total pod.cpu.total_time_agg metrics
        metric_list = metrics_dict['pod.cpu.total_time_agg']
        pod_cpu_total_time_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all'][0]

        self.assertTrue(pod_cpu_total_time_agg_metric is not None)
        self.assertEqual(275.0,
                         pod_cpu_total_time_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(22.0,
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_cpu_total_time_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_cpu_total_time_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         pod_cpu_total_time_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify for pod.cpu.total_time_agg metrics for first_namespace
        metric_list = metrics_dict['pod.cpu.total_time_agg']
        pod_cpu_total_time_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'first_namespace'][0]

        self.assertTrue(pod_cpu_total_time_agg_metric is not None)
        self.assertEqual(375.0,
                         pod_cpu_total_time_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(30.0,
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_cpu_total_time_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_cpu_total_time_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         pod_cpu_total_time_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify pod.cpu.total_time_agg metrics second_namespace
        metric_list = metrics_dict['pod.cpu.total_time_agg']
        pod_cpu_total_time_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'second_namespace'][0]

        self.assertTrue(pod_cpu_total_time_agg_metric is not None)
        self.assertEqual(475.0,
                         pod_cpu_total_time_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(38.0,
                         pod_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_cpu_total_time_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_cpu_total_time_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         pod_cpu_total_time_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total pod.net.out_bytes_sec_agg metrics
        metric_list = metrics_dict['pod.net.out_bytes_sec_agg']
        pod_net_out_bytes_sec_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all'][0]

        self.assertTrue(pod_net_out_bytes_sec_agg_metric is not None)
        self.assertEqual(775.0,
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(62.0,
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_net_out_bytes_sec_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_out_bytes_sec_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_net_out_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('interface'))
        self.assertEqual('all',
                         pod_net_out_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('pod_name'))
        self.assertEqual('hourly',
                         pod_net_out_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify pod.net.out_bytes_sec_agg metrics for first_namespace
        metric_list = metrics_dict['pod.net.out_bytes_sec_agg']
        pod_net_out_bytes_sec_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'first_namespace'][0]

        self.assertTrue(pod_net_out_bytes_sec_agg_metric is not None)
        self.assertEqual(875.0,
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(70.0,
                         pod_net_out_bytes_sec_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_net_out_bytes_sec_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_out_bytes_sec_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_net_out_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('interface'))
        self.assertEqual('all',
                         pod_net_out_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('pod_name'))
        self.assertEqual('hourly',
                         pod_net_out_bytes_sec_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total pod.mem.used_bytes_agg metrics
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_mem_used_bytes_agg_metric is not None)
        self.assertEqual(975.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(82.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('deployment'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify pod.mem.used_bytes_agg metrics for first_namespace
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'first_namespace' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_mem_used_bytes_agg_metric is not None)
        self.assertEqual(1075.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(90.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('deployment'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify pod.mem.used_bytes_agg metrics for second_namespace
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'second_namespace' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_mem_used_bytes_agg_metric is not None)
        self.assertEqual(1175.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(98.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('deployment'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify pod.mem.used_bytes_agg metrics for first_pod
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'first_pod'][0]

        self.assertTrue(pod_mem_used_bytes_agg_metric is not None)
        self.assertEqual(1275.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(106.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('deployment'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify pod.mem.used_bytes_agg metrics for second_pod
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'second_pod'][0]

        self.assertTrue(pod_mem_used_bytes_agg_metric is not None)
        self.assertEqual(1375.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(114.0,
                         pod_mem_used_bytes_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('deployment'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total container.cpu.total_time_agg metrics
        metric_list = metrics_dict['container.cpu.total_time_agg']
        container_cpu_total_time_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('container_name') ==
            'all'][0]

        self.assertTrue(container_cpu_total_time_agg_metric is not None)
        self.assertEqual(275.0,
                         container_cpu_total_time_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         container_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         container_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(22.0,
                         container_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         container_cpu_total_time_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         container_cpu_total_time_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         container_cpu_total_time_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify container.cpu.total_time_agg metrics by container_name
        metric_list = metrics_dict['container.cpu.total_time_agg']
        container_cpu_total_time_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('container_name') ==
            'container_1'][0]

        self.assertTrue(container_cpu_total_time_agg_metric is not None)
        self.assertEqual(400.0,
                         container_cpu_total_time_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:29:44',
                         container_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         container_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(32.0,
                         container_cpu_total_time_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         container_cpu_total_time_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         container_cpu_total_time_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         container_cpu_total_time_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total kubernetes.node.capacity.cpu_agg metrics
        metric_list = metrics_dict['kubernetes.node.capacity.cpu_agg']
        kubernetes_node_capacity_cpu_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(kubernetes_node_capacity_cpu_agg_metric is not None)
        self.assertEqual(275.0,
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(22.0,
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         kubernetes_node_capacity_cpu_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_capacity_cpu_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_capacity_cpu_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify kubernetes.node.capacity.cpu_agg metrics by host
        metric_list = metrics_dict['kubernetes.node.capacity.cpu_agg']
        kubernetes_node_capacity_cpu_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'host1'][0]

        self.assertTrue(kubernetes_node_capacity_cpu_agg_metric is not None)
        self.assertEqual(400.0,
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:29:44',
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(32.0,
                         kubernetes_node_capacity_cpu_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         kubernetes_node_capacity_cpu_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_capacity_cpu_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_capacity_cpu_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total cpu.total_time_sec_agg metrics
        metric_list = metrics_dict['cpu.total_time_sec_agg']
        cpu_total_time_sec_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(cpu_total_time_sec_agg_metric is not None)
        self.assertEqual(275.0,
                         cpu_total_time_sec_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         cpu_total_time_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         cpu_total_time_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(22.0,
                         cpu_total_time_sec_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         cpu_total_time_sec_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         cpu_total_time_sec_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         cpu_total_time_sec_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify cpu.total_time_sec_agg metrics by host
        metric_list = metrics_dict['cpu.total_time_sec_agg']
        cpu_total_time_sec_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'host1'][0]

        self.assertTrue(cpu_total_time_sec_agg_metric is not None)
        self.assertEqual(400.0,
                         cpu_total_time_sec_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:29:44',
                         cpu_total_time_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         cpu_total_time_sec_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(32.0,
                         cpu_total_time_sec_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         cpu_total_time_sec_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         cpu_total_time_sec_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         cpu_total_time_sec_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify total kubernetes.node.allocatable.cpu_agg metrics
        metric_list = metrics_dict['kubernetes.node.allocatable.cpu_agg']
        kubernetes_node_allocatable_cpu_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(kubernetes_node_allocatable_cpu_agg_metric is not None)
        self.assertEqual(275.0,
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:39:44',
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(22.0,
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         kubernetes_node_allocatable_cpu_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_allocatable_cpu_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('dimensions').get('aggregation_period'))

        # Verify kubernetes.node.allocatable.cpu_agg metrics by host
        metric_list = metrics_dict['kubernetes.node.allocatable.cpu_agg']
        kubernetes_node_allocatable_cpu_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'host1'][0]

        self.assertTrue(kubernetes_node_allocatable_cpu_agg_metric is not None)
        self.assertEqual(400.0,
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('2016-06-20 11:29:44',
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))
        self.assertEqual('2016-06-20 11:24:59',
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual(32.0,
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('useast',
                         kubernetes_node_allocatable_cpu_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_allocatable_cpu_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_allocatable_cpu_agg_metric
                         .get('metric')
                         .get('dimensions').get('aggregation_period'))

        os.remove(file_path)

    def simple_count_transform(self, rdd):
        return rdd.count()


if __name__ == "__main__":
    print("PATH *************************************************************")
    print(sys.path)
    print("PATH==============================================================")
    unittest.main()
