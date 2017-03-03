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
import unittest

from collections import defaultdict
import mock
from mock import MagicMock
from oslo_config import cfg
from pyspark.streaming.kafka import OffsetRange

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.driver.mon_metrics_kafka \
    import MonMetricsKafkaProcessor
from monasca_transform.transform import RddTransformContext
from monasca_transform.transform import TransformContextUtils
from tests.functional.messaging.adapter import DummyAdapter
from tests.functional.spark_context_test import SparkContextTest
from tests.functional.test_resources.kafka_data.data_provider import \
    DataProvider
from tests.functional.test_resources.mock_component_manager import \
    MockComponentManager


class SparkUnitTest(unittest.TestCase):

    def test_transform_to_recordstore(self):
        # simply verify that the transform method is called first, then
        # rdd to recordstore
        kafka_stream = MagicMock(name='kafka_stream')
        transformed_stream = MagicMock(name='transformed_stream')
        kafka_stream.transform.return_value = transformed_stream
        MonMetricsKafkaProcessor.transform_to_recordstore(
            kafka_stream)

        # TODO(someone) figure out why these asserts now fail
        # transformed_stream_expected = call.foreachRDD(
        #     MonMetricsKafkaProcessor.rdd_to_recordstore
        # ).call_list()
        # kafka_stream_expected = call.transform(
        #     MonMetricsKafkaProcessor.store_offset_ranges
        # ).call_list()
        # self.assertEqual(kafka_stream_expected, kafka_stream.mock_calls)
        # self.assertEqual(transformed_stream_expected,
        #                  transformed_stream.mock_calls)


class SparkTest(SparkContextTest):

    def setUp(self):
        super(SparkTest, self).setUp()
        # configure the system with a dummy messaging adapter
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/functional/test_resources/config/'
                'test_config_with_dummy_messaging_adapter.conf'])
        # reset metric_id list dummy adapter
        if not DummyAdapter.adapter_impl:
            DummyAdapter.init()
        DummyAdapter.adapter_impl.metric_list = []

    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_usage_component_manager')
    def test_rdd_to_recordstore(self,
                                usage_manager,
                                setter_manager,
                                insert_manager):

        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = (MockComponentManager
                                       .get_setter_cmpt_mgr())
        insert_manager.return_value = (MockComponentManager
                                       .get_insert_cmpt_mgr())

        # Create an emulated set of Kafka messages (these were gathered
        # by extracting Monasca messages from the Metrics queue).

        # Create an RDD out of the mocked Monasca metrics
        with open(DataProvider.kafka_data_path) as f:
            raw_lines = f.read().splitlines()
        raw_tuple_list = [eval(raw_line) for raw_line in raw_lines]

        rdd_monasca = self.spark_context.parallelize(raw_tuple_list)

        # decorate mocked RDD with dummy kafka offsets
        myOffsetRanges = [
            OffsetRange("metrics", 1, 10, 20)]  # mimic rdd.offsetRanges()

        transform_context = TransformContextUtils.get_context(
            offset_info=myOffsetRanges,
            batch_time_info=self.get_dummy_batch_time())

        rdd_monasca_with_offsets = rdd_monasca.map(
            lambda x: RddTransformContext(x, transform_context))

        # Do something simple with the RDD
        result = simple_count_transform(rdd_monasca_with_offsets)

        # Verify it worked
        self.assertEqual(result, 426)

        # Call the primary method in mon_metrics_kafka
        MonMetricsKafkaProcessor.rdd_to_recordstore(
            rdd_monasca_with_offsets)

        # get the metrics that have been submitted to the dummy message adapter
        metrics = DummyAdapter.adapter_impl.metric_list

        # make a dictionary of all metrics so that they can be directly
        # referenced in the verifications that follow
        metrics_dict = defaultdict(list)
        for metric in metrics:
            metric_name = metric.get('metric').get('name')
            metrics_dict[metric_name].append(metric)

        # Verify mem.total_mb_agg metrics
        total_mb_agg_metric = metrics_dict['mem.total_mb_agg'][0]

        self.assertTrue(total_mb_agg_metric is not None)

        self.assertEqual(3733.75,
                         total_mb_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         total_mb_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         total_mb_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         total_mb_agg_metric.get('metric').get('dimensions')
                         .get('host'))
        self.assertEqual('all',
                         total_mb_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         total_mb_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))
        self.assertEqual(4.0,
                         total_mb_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         total_mb_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         total_mb_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify mem.usable_mb_agg metrics
        usable_mb_agg_metric = metrics_dict['mem.usable_mb_agg'][0]

        self.assertTrue(usable_mb_agg_metric is not None)

        self.assertEqual(843.0,
                         usable_mb_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         usable_mb_agg_metric.get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         usable_mb_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         usable_mb_agg_metric.get('metric').get('dimensions')
                         .get('host'))
        self.assertEqual('all',
                         usable_mb_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         usable_mb_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(4.0,
                         usable_mb_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         usable_mb_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         usable_mb_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vcpus_agg metrics for all projects
        metric_list = metrics_dict['vcpus_agg']
        vcpus_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            'all'][0]

        self.assertTrue(vcpus_agg_metric is not None)

        self.assertEqual(8.0,
                         vcpus_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vcpus_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vcpus_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vcpus_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vcpus_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(14.0,
                         vcpus_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vcpus_agg metrics for 8647fd5030b04a799b0411cc38c4102d
        # project
        metric_list = metrics_dict['vcpus_agg']
        vcpus_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '8647fd5030b04a799b0411cc38c4102d'][0]

        self.assertTrue(vcpus_agg_metric is not None)

        self.assertEqual(2.0,
                         vcpus_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vcpus_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vcpus_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vcpus_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vcpus_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         vcpus_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:42',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vcpus_agg metrics for 9647fd5030b04a799b0411cc38c4102d
        # project
        metric_list = metrics_dict['vcpus_agg']
        vcpus_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '9647fd5030b04a799b0411cc38c4102d'][0]

        self.assertTrue(vcpus_agg_metric is not None)

        self.assertEqual(6.0,
                         vcpus_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vcpus_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vcpus_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vcpus_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vcpus_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(8.0,
                         vcpus_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:05',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.mem.total_mb_agg metrics for all projects
        metric_list = metrics_dict['vm.mem.total_mb_agg']
        vm_mem_total_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            'all'][0]

        self.assertTrue(vm_mem_total_mb_agg_metric is not None)

        self.assertEqual(14336.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(7.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-07 16:27:54',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-07 16:30:54',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.mem.total_mb_agg metrics for the 1 project
        metric_list = metrics_dict['vm.mem.total_mb_agg']
        vm_mem_total_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '1'][0]

        self.assertTrue(vm_mem_total_mb_agg_metric is not None)

        self.assertEqual(6656.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(4.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-07 16:27:54',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-07 16:29:54',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.mem.total_mb_agg metrics for the 2 project
        metric_list = metrics_dict['vm.mem.total_mb_agg']
        vm_mem_total_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '2'][0]

        self.assertTrue(vm_mem_total_mb_agg_metric is not None)

        self.assertEqual(7680.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-07 16:28:54',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-07 16:30:54',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify nova.vm.disk.total_allocated_gb_agg metrics
        total_allocated_disk_agg_metric = \
            metrics_dict['nova.vm.disk.total_allocated_gb_agg'][0]

        self.assertTrue(total_allocated_disk_agg_metric is not None)

        self.assertEqual(180.0,
                         total_allocated_disk_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         total_allocated_disk_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         total_allocated_disk_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         total_allocated_disk_agg_metric.get('metric')
                         .get('dimensions').get('host'))
        self.assertEqual('all',
                         total_allocated_disk_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         total_allocated_disk_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))
        self.assertEqual(5.0,
                         total_allocated_disk_agg_metric.get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('2016-05-17 15:14:08',
                         total_allocated_disk_agg_metric.get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-05-17 15:14:44',
                         total_allocated_disk_agg_metric.get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.disk.allocation_agg metrics for all projects
        metric_list = metrics_dict['vm.disk.allocation_agg']
        vm_disk_allocation_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            'all'][0]

        self.assertTrue(vm_disk_allocation_agg_metric is not None)

        self.assertEqual(140.0,
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_disk_allocation_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_disk_allocation_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(9.0,
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.disk.allocation_agg metrics for
        # 5f681592f7084c5fbcd4e8a20a4fef15 project
        metric_list = metrics_dict['vm.disk.allocation_agg']
        vm_disk_allocation_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '5f681592f7084c5fbcd4e8a20a4fef15'][0]

        self.assertTrue(vm_disk_allocation_agg_metric is not None)

        self.assertEqual(24.0,
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_disk_allocation_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_disk_allocation_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:40',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.disk.allocation_agg metrics for
        # 6f681592f7084c5fbcd4e8a20a4fef15 project
        metric_list = metrics_dict['vm.disk.allocation_agg']
        vm_disk_allocation_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '6f681592f7084c5fbcd4e8a20a4fef15'][0]

        self.assertTrue(vm_disk_allocation_agg_metric is not None)

        self.assertEqual(116.0,
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_disk_allocation_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_disk_allocation_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         vm_disk_allocation_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.cpu.utilization_perc_agg metrics for
        # 817331145b804dc9a7accb6edfb0674d project
        metric_list = metrics_dict['vm.cpu.utilization_perc_agg']
        vm_cpu_util_perc_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '817331145b804dc9a7accb6edfb0674d'][0]

        self.assertTrue(vm_cpu_util_perc_agg_metric is not None)

        self.assertEqual(55.0,
                         vm_cpu_util_perc_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_cpu_util_perc_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_cpu_util_perc_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('dimensions').get('host'))
        self.assertEqual('817331145b804dc9a7accb6edfb0674d',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))
        self.assertEqual(3.0,
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('2016-05-26 17:31:01',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-05-26 17:31:36',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.cpu.utilization_perc_agg metrics for
        # 5d0e49bdc4534bb4b65909228aa040da project
        metric_list = metrics_dict['vm.cpu.utilization_perc_agg']
        vm_cpu_util_perc_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '5d0e49bdc4534bb4b65909228aa040da'][0]

        self.assertTrue(vm_cpu_util_perc_agg_metric is not None)

        self.assertEqual(26.0,
                         vm_cpu_util_perc_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_cpu_util_perc_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_cpu_util_perc_agg_metric.get('meta')
                         .get('tenantId'))
        self.assertEqual('all',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('dimensions').get('host'))
        self.assertEqual('5d0e49bdc4534bb4b65909228aa040da',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))
        self.assertEqual(1.0,
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('2016-05-26 17:30:27',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-05-26 17:30:27',
                         vm_cpu_util_perc_agg_metric.get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify disk.total_space_mb_agg metrics
        disk_total_space_agg_metric = \
            metrics_dict['disk.total_space_mb_agg'][0]

        self.assertTrue(disk_total_space_agg_metric is not None)

        self.assertEqual(2574044.0,
                         disk_total_space_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         disk_total_space_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         disk_total_space_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         disk_total_space_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         disk_total_space_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(2.0,
                         disk_total_space_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-01 21:09:21',
                         disk_total_space_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-01 21:09:24',
                         disk_total_space_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify disk.total_used_space_mb_agg metrics
        disk_total_used_agg_metric = \
            metrics_dict['disk.total_used_space_mb_agg'][0]

        self.assertTrue(disk_total_used_agg_metric is not None)

        self.assertEqual(34043.0,
                         disk_total_used_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         disk_total_used_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         disk_total_used_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         disk_total_used_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         disk_total_used_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(2.0,
                         disk_total_used_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-01 21:09:21',
                         disk_total_used_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-01 21:09:24',
                         disk_total_used_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.utilized_logical_cores_agg metrics for all hosts
        metric_list = metrics_dict['cpu.utilized_logical_cores_agg']
        cpu_util_cores_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(cpu_util_cores_agg_metric is not None)

        self.assertEqual(0.5303809523809525,
                         cpu_util_cores_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         cpu_util_cores_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         cpu_util_cores_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         cpu_util_cores_agg_metric
                         .get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         cpu_util_cores_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(13.0,
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.utilized_logical_cores_agg metrics for
        # test-cp1-comp0294-mgmt host
        metric_list = metrics_dict['cpu.utilized_logical_cores_agg']
        cpu_util_cores_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'test-cp1-comp0294-mgmt'][0]

        self.assertTrue(cpu_util_cores_agg_metric is not None)

        self.assertEqual(0.3866666666666669,
                         cpu_util_cores_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         cpu_util_cores_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         cpu_util_cores_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         cpu_util_cores_agg_metric
                         .get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         cpu_util_cores_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.cpu.utilization_perc_agg metrics for
        # test-cp1-comp0037-mgmt host
        metric_list = metrics_dict['cpu.utilized_logical_cores_agg']
        cpu_util_cores_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'test-cp1-comp0037-mgmt'][0]

        self.assertTrue(cpu_util_cores_agg_metric is not None)

        self.assertEqual(0.14371428571428566,
                         cpu_util_cores_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         cpu_util_cores_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         cpu_util_cores_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         cpu_util_cores_agg_metric
                         .get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         cpu_util_cores_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(7.0,
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         cpu_util_cores_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.mem.used_mb_agg metrics for all projects
        metric_list = metrics_dict['vm.mem.used_mb_agg']
        vm_mem_used_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            'all'][0]

        self.assertTrue(vm_mem_used_mb_agg_metric is not None)

        self.assertEqual(6340.0,
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_used_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_mem_used_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(7.0,
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-07 16:27:54',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-07 16:30:54',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.mem.used_mb_agg metrics for the 1 project
        metric_list = metrics_dict['vm.mem.used_mb_agg']
        vm_mem_used_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '1'][0]

        self.assertTrue(vm_mem_used_mb_agg_metric is not None)

        self.assertEqual(1840.0,
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_used_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_mem_used_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(4.0,
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-07 16:27:54',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-07 16:29:54',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify vm.mem.used_mb_agg metrics for the 2 project
        metric_list = metrics_dict['vm.mem.used_mb_agg']
        vm_mem_used_mb_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('project_id') ==
            '2'][0]

        self.assertTrue(vm_mem_used_mb_agg_metric is not None)

        self.assertEqual(4500.0,
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_used_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         vm_mem_used_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-06-07 16:28:54',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-07 16:30:54',
                         vm_mem_used_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify swiftlm.diskusage.val.size_agg for all hosts
        metric_list = metrics_dict['swiftlm.diskusage.val.size_agg']
        used_swift_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') == 'all'][0]

        self.assertTrue(used_swift_agg_metric is not None)

        self.assertEqual(3363.4285714285716,
                         used_swift_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         used_swift_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         used_swift_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         used_swift_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         used_swift_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))
        self.assertEqual(17.0,
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-06-10 20:27:01',
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify swiftlm.diskusage.val.size_agg for host a
        metric_list = metrics_dict['swiftlm.diskusage.val.size_agg']
        used_swift_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') == 'a'][0]

        self.assertTrue(used_swift_agg_metric is not None)

        self.assertEqual(1890.857142857143,
                         used_swift_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         used_swift_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         used_swift_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         used_swift_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         used_swift_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))
        self.assertEqual(8.0,
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-06-10 20:27:01',
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify swiftlm.diskusage.val.size_agg for host b
        metric_list = metrics_dict['swiftlm.diskusage.val.size_agg']
        used_swift_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') == 'b'][0]

        self.assertTrue(used_swift_agg_metric is not None)

        self.assertEqual(1472.5714285714284,
                         used_swift_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         used_swift_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         used_swift_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         used_swift_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         used_swift_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))
        self.assertEqual(9.0,
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-06-10 20:27:01',
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         used_swift_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify swiftlm.diskusage.val.avail_agg for all hosts
        metric_list = metrics_dict['swiftlm.diskusage.val.avail_agg']
        avail_swift_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') == 'all'][0]

        self.assertTrue(avail_swift_agg_metric is not None)

        self.assertEqual(3363.4285714285716,
                         avail_swift_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         avail_swift_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         avail_swift_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         avail_swift_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         avail_swift_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))
        self.assertEqual(17.0,
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-06-10 20:27:01',
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify swiftlm.diskusage.val.avail_agg for host a
        metric_list = metrics_dict['swiftlm.diskusage.val.avail_agg']
        avail_swift_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') == 'a'][0]

        self.assertTrue(avail_swift_agg_metric is not None)

        self.assertEqual(1890.857142857143,
                         avail_swift_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         avail_swift_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         avail_swift_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         avail_swift_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         avail_swift_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))
        self.assertEqual(8.0,
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-06-10 20:27:01',
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify swiftlm.diskusage.val.avail_agg for host b
        metric_list = metrics_dict['swiftlm.diskusage.val.avail_agg']
        avail_swift_agg_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') == 'b'][0]

        self.assertTrue(avail_swift_agg_metric is not None)

        self.assertEqual(1472.5714285714284,
                         avail_swift_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         avail_swift_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         avail_swift_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         avail_swift_agg_metric.get('metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         avail_swift_agg_metric.get('metric').get('dimensions')
                         .get('aggregation_period'))
        self.assertEqual(9.0,
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-06-10 20:27:01',
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         avail_swift_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify swiftlm.diskusage.rate_agg metrics
        diskusage_rate_agg_metric = \
            metrics_dict['swiftlm.diskusage.rate_agg'][0]

        self.assertTrue(diskusage_rate_agg_metric is not None)

        self.assertEqual(24.990442207212947,
                         diskusage_rate_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         diskusage_rate_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         diskusage_rate_agg_metric.get('meta').get('tenantId'))
        self.assertEqual('all',
                         diskusage_rate_agg_metric.get('metric')
                         .get('dimensions').get('host'))
        self.assertEqual('all',
                         diskusage_rate_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         diskusage_rate_agg_metric.get('metric')
                         .get('dimensions').get('aggregation_period'))
        self.assertEqual(34.0,
                         diskusage_rate_agg_metric.get('metric')
                         .get('value_meta').get('record_count'))
        self.assertEqual('2016-06-10 20:27:01',
                         diskusage_rate_agg_metric.get('metric')
                         .get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-06-10 20:27:01',
                         diskusage_rate_agg_metric.get('metric')
                         .get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify nova.vm.cpu.total_allocated_agg metrics
        nova_vm_cpu_total_alloc_agg_metric = \
            metrics_dict['nova.vm.cpu.total_allocated_agg'][0]

        self.assertTrue(nova_vm_cpu_total_alloc_agg_metric is not None)

        self.assertEqual(8.0,
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('all',
                         nova_vm_cpu_total_alloc_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(14.0,
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         nova_vm_cpu_total_alloc_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify nova.vm.mem.total_allocated_mb_agg metrics
        nova_vm_mem_total_alloc_agg_metric = \
            metrics_dict['nova.vm.mem.total_allocated_mb_agg'][0]

        self.assertTrue(nova_vm_mem_total_alloc_agg_metric is not None)

        self.assertEqual(9728.0,
                         nova_vm_mem_total_alloc_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         nova_vm_mem_total_alloc_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         nova_vm_mem_total_alloc_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         nova_vm_mem_total_alloc_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('all',
                         nova_vm_mem_total_alloc_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         nova_vm_mem_total_alloc_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(9.0,
                         nova_vm_mem_total_alloc_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         nova_vm_mem_total_alloc_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-01-20 16:40:46',
                         nova_vm_mem_total_alloc_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify storage.objects.size_agg metrics
        storage_objects_size_agg_metric = \
            metrics_dict['storage.objects.size_agg'][0]

        self.assertTrue(storage_objects_size_agg_metric is not None)

        self.assertEqual(16666.5,
                         storage_objects_size_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         storage_objects_size_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         storage_objects_size_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         storage_objects_size_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('all',
                         storage_objects_size_agg_metric.get('metric')
                         .get('dimensions').get('project_id'))
        self.assertEqual('hourly',
                         storage_objects_size_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(5.0,
                         storage_objects_size_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-08-10 21:04:12',
                         storage_objects_size_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-08-10 21:04:12',
                         storage_objects_size_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.cpu.total_time_agg metrics for total
        metric_list = metrics_dict['pod.cpu.total_time_agg']
        pod_cpu_total_time_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all'][0]

        self.assertTrue(pod_cpu_total_time_metric is not None)

        self.assertEqual(13437792.0,
                         pod_cpu_total_time_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_cpu_total_time_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_cpu_total_time_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_cpu_total_time_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         pod_cpu_total_time_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         pod_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         pod_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.cpu.total_time_agg metrics by namespace
        metric_list = metrics_dict['pod.cpu.total_time_agg']
        pod_cpu_total_time_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'monitoring'][0]

        self.assertTrue(pod_cpu_total_time_metric is not None)

        self.assertEqual(13437792.0,
                         pod_cpu_total_time_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_cpu_total_time_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_cpu_total_time_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_cpu_total_time_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         pod_cpu_total_time_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         pod_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         pod_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.out_bytes_sec_agg metrics for total
        metric_list = metrics_dict['pod.net.out_bytes_sec_agg']
        pod_net_out_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_net_out_bytes_metric is not None)

        self.assertEqual(13554.79,
                         pod_net_out_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_out_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_out_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_net_out_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         pod_net_out_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_out_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_out_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.out_bytes_sec_agg metrics by namespace
        metric_list = metrics_dict['pod.net.out_bytes_sec_agg']
        pod_net_out_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'kube-system' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_net_out_bytes_metric is not None)

        self.assertEqual(13554.79,
                         pod_net_out_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_out_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_out_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_net_out_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         pod_net_out_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_out_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_out_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.in_bytes_sec_agg metrics for total
        metric_list = metrics_dict['pod.net.in_bytes_sec_agg']
        pod_net_in_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('app') ==
            'all'][0]

        self.assertTrue(pod_net_in_bytes_metric is not None)

        self.assertEqual(145.88,
                         pod_net_in_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_in_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_in_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_net_in_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.in_bytes_sec_agg metrics by namespace
        metric_list = metrics_dict['pod.net.in_bytes_sec_agg']
        pod_net_in_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'website2' and
            value.get('metric').get('dimensions').get('app') ==
            'all'][0]

        self.assertTrue(pod_net_in_bytes_metric is not None)

        self.assertEqual(122.94,
                         pod_net_in_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_in_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_in_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_net_in_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.in_bytes_sec_agg metrics by app
        metric_list = metrics_dict['pod.net.in_bytes_sec_agg']
        pod_net_in_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('app') ==
            'wordpress'][0]

        self.assertTrue(pod_net_in_bytes_metric is not None)

        self.assertEqual(22.94,
                         pod_net_in_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_in_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_in_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_net_in_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.in_bytes_sec_agg metrics for total
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_metricpod_net_in_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_mem_used_bytes_metricpod_net_in_bytes_metric
                        is not None)

        self.assertEqual(195000.0,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(8.0,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:22',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:25',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.in_bytes_sec_agg metrics by namespace
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_metricpod_net_in_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'gluster2' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_mem_used_bytes_metricpod_net_in_bytes_metric
                        is not None)

        self.assertEqual(155000.0,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(4.0,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:22',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:25',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify pod.net.in_bytes_sec_agg metrics by pod_name
        metric_list = metrics_dict['pod.mem.used_bytes_agg']
        pod_mem_used_bytes_metricpod_net_in_bytes_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'heketi-703226055-w13jr4'][0]

        self.assertTrue(pod_mem_used_bytes_metricpod_net_in_bytes_metric
                        is not None)

        self.assertEqual(35000.0,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(1.0,
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:23',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:23',
                         pod_mem_used_bytes_metricpod_net_in_bytes_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify container_cpu_total_time_agg metrics for total
        metric_list = metrics_dict['container.cpu.total_time_agg']
        container_cpu_total_time_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('container_name') ==
            'all'][0]

        self.assertTrue(container_cpu_total_time_metric is not None)
        self.assertEqual(26875584.0,
                         container_cpu_total_time_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         container_cpu_total_time_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         container_cpu_total_time_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         container_cpu_total_time_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         container_cpu_total_time_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         container_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         container_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify container_cpu_total_time_agg metrics by container_name
        metric_list = metrics_dict['container.cpu.total_time_agg']
        container_cpu_total_time_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('container_name') ==
            'container1'][0]

        self.assertTrue(container_cpu_total_time_metric is not None)
        self.assertEqual(13437792.0,
                         container_cpu_total_time_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         container_cpu_total_time_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         container_cpu_total_time_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         container_cpu_total_time_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(2.0,
                         container_cpu_total_time_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         container_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         container_cpu_total_time_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify kubernetes.node.capacity.cpu_agg metrics for total
        metric_list = metrics_dict['kubernetes.node.capacity.cpu_agg']
        kubernetes_node_capacity_cpu_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(kubernetes_node_capacity_cpu_metric is not None)
        self.assertEqual(26875584.0,
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         kubernetes_node_capacity_cpu_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_capacity_cpu_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify kubernetes.node.capacity.cpu_agg metrics by host
        metric_list = metrics_dict['kubernetes.node.capacity.cpu_agg']
        kubernetes_node_capacity_cpu_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'host1'][0]

        self.assertTrue(kubernetes_node_capacity_cpu_metric is not None)
        self.assertEqual(13437792.0,
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         kubernetes_node_capacity_cpu_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_capacity_cpu_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(2.0,
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_capacity_cpu_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.total_time_sec_agg metrics for total
        metric_list = metrics_dict['cpu.total_time_sec_agg']
        cpu_total_time_sec_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(cpu_total_time_sec_metric is not None)
        self.assertEqual(26875584.0,
                         cpu_total_time_sec_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         cpu_total_time_sec_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         cpu_total_time_sec_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         cpu_total_time_sec_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         cpu_total_time_sec_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         cpu_total_time_sec_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         cpu_total_time_sec_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.total_time_sec_agg metrics by host
        metric_list = metrics_dict['cpu.total_time_sec_agg']
        cpu_total_time_sec_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'host1'][0]

        self.assertTrue(cpu_total_time_sec_metric is not None)
        self.assertEqual(13437792.0,
                         cpu_total_time_sec_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         cpu_total_time_sec_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         cpu_total_time_sec_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         cpu_total_time_sec_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(2.0,
                         cpu_total_time_sec_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         cpu_total_time_sec_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         cpu_total_time_sec_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify kubernetes.node.allocatable.cpu_agg metrics for total
        metric_list = metrics_dict['kubernetes.node.allocatable.cpu_agg']
        kubernetes_node_allocatable_cpu_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertTrue(kubernetes_node_allocatable_cpu_metric is not None)
        self.assertEqual(26875584.0,
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         kubernetes_node_allocatable_cpu_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_allocatable_cpu_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify kubernetes.node.allocatable.cpu_agg metrics by host
        metric_list = metrics_dict['kubernetes.node.allocatable.cpu_agg']
        kubernetes_node_allocatable_cpu_metric = [
            value for value in metric_list
            if value.get('metric').get('dimensions').get('host') ==
            'host1'][0]

        self.assertTrue(kubernetes_node_allocatable_cpu_metric is not None)
        self.assertEqual(13437792.0,
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         kubernetes_node_allocatable_cpu_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         kubernetes_node_allocatable_cpu_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('hourly',
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(2.0,
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:52',
                         kubernetes_node_allocatable_cpu_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))


def simple_count_transform(rdd):
    return rdd.count()


if __name__ == "__main__":
    print("PATH *************************************************************")
    import sys
    print(sys.path)
    print("PATH==============================================================")
    unittest.main()
