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
import mock
import unittest

from pyspark.streaming.kafka import OffsetRange

from monasca_transform.component.insert.dummy_insert import DummyInsert
from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.processor.pre_hourly_processor import PreHourlyProcessor

from tests.unit.messaging.adapter import DummyAdapter
from tests.unit.spark_context_test import SparkContextTest
from tests.unit.test_resources.metrics_pre_hourly_data.data_provider \
    import DataProvider


class TestPreHourlyProcessorAgg(SparkContextTest):

    def setUp(self):
        super(TestPreHourlyProcessorAgg, self).setUp()
        # configure the system with a dummy messaging adapter
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/unit/test_resources/config/'
                'test_config_with_dummy_messaging_adapter.conf'])
        # reset metric_id list dummy adapter
        if not DummyAdapter.adapter_impl:
            DummyAdapter.init()
        DummyAdapter.adapter_impl.metric_list = []

    @mock.patch('monasca_transform.processor.pre_hourly_processor.KafkaInsert',
                DummyInsert)
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.fetch_pre_hourly_data')
    @mock.patch('monasca_transform.processor.pre_hourly_processor.'
                'PreHourlyProcessor.get_processing_offset_range_list')
    def test_pre_hourly_processor(self,
                                  offset_range_list,
                                  pre_hourly_data):

        # load components
        myOffsetRanges = [
            OffsetRange("metrics_pre_hourly", 1, 10, 20)]
        offset_range_list.return_value = myOffsetRanges

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
        self.assertEqual(result, 6)

        # check aggregation result
        mem_total_mb_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'mem.total_mb_agg' and
            value.get('metric').get('dimensions').get('host') ==
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

        mem_usable_mb_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'mem.usable_mb_agg' and
            value.get('metric').get('dimensions').get('host') ==
            'all'][0]
        self.assertTrue(mem_usable_mb_agg_metric is not None)
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

    def simple_count_transform(self, rdd):
        return rdd.count()


if __name__ == "__main__":
    print("PATH *************************************************************")
    import sys
    print(sys.path)
    print("PATH==============================================================")
    unittest.main()
