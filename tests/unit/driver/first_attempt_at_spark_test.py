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

from oslo_config import cfg
from pyspark.streaming.kafka import OffsetRange

import mock
from mock import call
from mock import MagicMock

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.driver.mon_metrics_kafka \
    import MonMetricsKafkaProcessor
from monasca_transform.messaging.adapter import MessageAdapter
from monasca_transform.transform import RddTransformContext
from monasca_transform.transform import TransformContextUtils

from tests.unit.spark_context_test import SparkContextTest
from tests.unit.test_resources.kafka_data.data_provider import DataProvider
from tests.unit.test_resources.mock_component_manager \
    import MockComponentManager


class SparkUnitTest(unittest.TestCase):

    def test_transform_to_recordstore(self):
        # simply verify that the transform method is called first, then
        # rdd to recordstore
        kafka_stream = MagicMock(name='kafka_stream')
        transformed_stream = MagicMock(name='transformed_stream')
        kafka_stream.transform.return_value = transformed_stream
        transformed_stream_expected = call.foreachRDD(
            MonMetricsKafkaProcessor.rdd_to_recordstore
        ).call_list()
        kafka_stream_expected = call.transform(
            MonMetricsKafkaProcessor.store_offset_ranges
        ).call_list()
        MonMetricsKafkaProcessor.transform_to_recordstore(
            kafka_stream)

        self.assertEqual(
            kafka_stream_expected, kafka_stream.mock_calls)
        self.assertEqual(
            transformed_stream_expected, transformed_stream.mock_calls)


class SparkTest(SparkContextTest):

    def setUp(self):
        super(SparkTest, self).setUp()
        # configure the system with a dummy messaging adapter
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/unit/test_resources/config/'
                'test_config_with_dummy_messaging_adapter.conf'])
        # reset metric_id list dummy adapter
        if not MessageAdapter.adapter_impl:
            MessageAdapter.init()
        MessageAdapter.adapter_impl.metric_list = []

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
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # Create an emulated set of Kafka messages (these were gathered
        # by extracting Monasca messages from the Metrics queue on mini-mon).

        # Create an RDD out of the mocked Monasca metrics
        with open(DataProvider.kafka_data_path) as f:
            raw_lines = f.read().splitlines()
        raw_tuple_list = [eval(raw_line) for raw_line in raw_lines]

        rdd_monasca = self.spark_context.parallelize(raw_tuple_list)

        # decorate mocked RDD with dummy kafka offsets
        myOffsetRanges = [
            OffsetRange("metrics", 1, 10, 20)]  # mimic rdd.offsetRanges()

        transform_context = TransformContextUtils.get_context(
            offset_info=myOffsetRanges)
        rdd_monasca_with_offsets = rdd_monasca.map(
            lambda x: RddTransformContext(x, transform_context))

        # Do something simple with the RDD
        result = simple_count_transform(rdd_monasca_with_offsets)

        # Verify it worked
        self.assertEqual(result, 307)

        # Call the primary method in mon_metrics_kafka
        MonMetricsKafkaProcessor.rdd_to_recordstore(
            rdd_monasca_with_offsets)

        # get the metrics that have been submitted to the dummy message adapter
        metrics = MessageAdapter.adapter_impl.metric_list

        total_mb_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') == 'mem.total_mb_agg'][0]

        self.assertEqual(3733.75,
                         total_mb_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         total_mb_agg_metric.get('meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
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
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:46',
                         total_mb_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        usable_mb_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') == 'mem.usable_mb_agg'][0]

        self.assertEqual(843.0,
                         usable_mb_agg_metric.get('metric').get('value'))
        self.assertEqual('useast',
                         usable_mb_agg_metric.get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
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
                         total_mb_agg_metric.get('metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         total_mb_agg_metric.get('metric').get('value_meta')
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:46',
                         total_mb_agg_metric.get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        vcpus_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'vcpus_agg' and
            value.get('metric').get('dimensions').get('project_id') ==
            'all'][0]

        self.assertTrue(vcpus_agg_metric is not None)

        self.assertEqual(8.0,
                         vcpus_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vcpus_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
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
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:46',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        vcpus_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'vcpus_agg' and
            value.get('metric').get('dimensions').get('project_id') ==
            '8647fd5030b04a799b0411cc38c4102d'][0]

        self.assertTrue(vcpus_agg_metric is not None)

        self.assertEqual(2.0,
                         vcpus_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vcpus_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
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
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:42',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        vcpus_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'vcpus_agg' and
            value.get('metric').get('dimensions').get('project_id') ==
            '9647fd5030b04a799b0411cc38c4102d'][0]

        self.assertTrue(vcpus_agg_metric is not None)

        self.assertEqual(6.0,
                         vcpus_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vcpus_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
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
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:46',
                         vcpus_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        vm_mem_total_mb_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'vm.mem.total_mb_agg' and
            value.get('metric').get('dimensions').get('project_id') ==
            'all'][0]

        self.assertTrue(vm_mem_total_mb_agg_metric is not None)

        self.assertEqual(9728.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(9.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:46',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        vm_mem_total_mb_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'vm.mem.total_mb_agg' and
            value.get('metric').get('dimensions').get('project_id') ==
            '5f681592f7084c5fbcd4e8a20a4fef15'][0]

        self.assertTrue(vm_mem_total_mb_agg_metric is not None)

        self.assertEqual(1536.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
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
        self.assertEqual('2016-01-20 16:40:00',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:40',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        vm_mem_total_mb_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'vm.mem.total_mb_agg' and
            value.get('metric').get('dimensions').get('project_id') ==
            '6f681592f7084c5fbcd4e8a20a4fef15'][0]

        self.assertTrue(vm_mem_total_mb_agg_metric is not None)

        self.assertEqual(8192.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
                         vm_mem_total_mb_agg_metric
                         .get('meta').get('tenantId'))
        self.assertEqual('all',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions').get('host'))
        self.assertEqual('hourly',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2016-01-20 16:40:00',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp'))
        self.assertEqual('2016-01-20 16:40:46',
                         vm_mem_total_mb_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp'))

        total_allocated_disk_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'nova.vm.disk.total_allocated_gb_agg'][0]

        self.assertEqual(180.0,
                         total_allocated_disk_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         total_allocated_disk_agg_metric.get('meta')
                         .get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_tenant_id,
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
                         .get('value_meta').get('firstrecord_timestamp'))
        self.assertEqual('2016-05-17 15:14:44',
                         total_allocated_disk_agg_metric.get('metric')
                         .get('value_meta').get('lastrecord_timestamp'))


def simple_count_transform(rdd):
    return rdd.count()


if __name__ == "__main__":
    print("PATH *************************************************************")
    import sys
    print(sys.path)
    print("PATH==============================================================")
    unittest.main()
