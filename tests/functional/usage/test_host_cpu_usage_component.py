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

import mock
from oslo_config import cfg
from pyspark.streaming.kafka import OffsetRange

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.driver.mon_metrics_kafka \
    import MonMetricsKafkaProcessor
from monasca_transform.transform import RddTransformContext
from monasca_transform.transform import TransformContextUtils
from tests.functional.messaging.adapter import DummyAdapter
from tests.functional.spark_context_test import SparkContextTest
from tests.functional.test_resources.cpu_kafka_data.data_provider \
    import DataProvider
from tests.functional.test_resources.mock_component_manager import \
    MockComponentManager


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
            offset_info=myOffsetRanges,
            batch_time_info=self.get_dummy_batch_time())

        rdd_monasca_with_offsets = rdd_monasca.map(
            lambda x: RddTransformContext(x, transform_context))

        # Call the primary method in mon_metrics_kafka
        MonMetricsKafkaProcessor.rdd_to_recordstore(
            rdd_monasca_with_offsets)

        # get the metrics that have been submitted to the dummy message adapter
        metrics = DummyAdapter.adapter_impl.metric_list

        # Verify cpu.total_logical_cores_agg for all hosts
        total_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'cpu.total_logical_cores_agg' and
            value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertEqual(15.0,
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value'))
        self.assertEqual('useast',
                         total_cpu_logical_agg_metric.get(
                             'meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         total_cpu_logical_agg_metric.get(
                             'meta').get('tenantId'))
        self.assertEqual('all',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(13.0,
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.total_logical_cores_agg for test-cp1-comp0333-mgmt host
        total_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'cpu.total_logical_cores_agg' and
            value.get('metric').get('dimensions').get('host') ==
            'test-cp1-comp0333-mgmt'][0]

        self.assertEqual(9.0,
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value'))
        self.assertEqual('useast',
                         total_cpu_logical_agg_metric.get(
                             'meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         total_cpu_logical_agg_metric.get(
                             'meta').get('tenantId'))
        self.assertEqual('all',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.total_logical_cores_agg for test-cp1-comp0027-mgmt host
        total_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'cpu.total_logical_cores_agg' and
            value.get('metric').get('dimensions').get('host') ==
            'test-cp1-comp0027-mgmt'][0]

        self.assertEqual(6.0,
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value'))
        self.assertEqual('useast',
                         total_cpu_logical_agg_metric.get(
                             'meta').get('region'))
        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         total_cpu_logical_agg_metric.get(
                             'meta').get('tenantId'))
        self.assertEqual('all',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(7.0,
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         total_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.utilized_logical_cores_agg for all hosts
        utilized_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'cpu.utilized_logical_cores_agg' and
            value.get('metric').get('dimensions').get('host') ==
            'all'][0]

        self.assertEqual(7.134214285714285,
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value'))
        self.assertEqual('useast',
                         utilized_cpu_logical_agg_metric.get(
                             'meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         utilized_cpu_logical_agg_metric.get(
                             'meta').get('tenantId'))
        self.assertEqual('all',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(13.0,
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.utilized_logical_cores_agg for the
        # test-cp1-comp0333-mgmt host
        utilized_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'cpu.utilized_logical_cores_agg' and
            value.get('metric').get('dimensions').get('host') ==
            'test-cp1-comp0333-mgmt'][0]

        self.assertEqual(4.9665,
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value'))
        self.assertEqual('useast',
                         utilized_cpu_logical_agg_metric.get(
                             'meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         utilized_cpu_logical_agg_metric.get(
                             'meta').get('tenantId'))
        self.assertEqual('all',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

        # Verify cpu.utilized_logical_cores_agg for the
        # test-cp1-comp0027-mgmt host
        utilized_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'cpu.utilized_logical_cores_agg' and
            value.get('metric').get('dimensions').get('host') ==
            'test-cp1-comp0027-mgmt'][0]

        self.assertEqual(2.1677142857142853,
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value'))
        self.assertEqual('useast',
                         utilized_cpu_logical_agg_metric.get(
                             'meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         utilized_cpu_logical_agg_metric.get(
                             'meta').get('tenantId'))
        self.assertEqual('all',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('project_id'))
        self.assertEqual('hourly',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(7.0,
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('record_count'))
        self.assertEqual('2016-03-07 16:09:23',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2016-03-07 16:10:38',
                         utilized_cpu_logical_agg_metric.get(
                             'metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))


def simple_count_transform(rdd):
    return rdd.count()


if __name__ == "__main__":
    print("PATH *************************************************************")
    import sys
    print(sys.path)
    print("PATH==============================================================")
    unittest.main()
