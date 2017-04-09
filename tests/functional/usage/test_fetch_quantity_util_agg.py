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
import json
import mock
from oslo_config import cfg
import unittest


from pyspark.streaming.kafka import OffsetRange

from tests.functional.messaging.adapter import DummyAdapter
from tests.functional.spark_context_test import SparkContextTest
from tests.functional.test_resources.cpu_kafka_data.data_provider \
    import DataProvider
from tests.functional.test_resources.mock_component_manager \
    import MockComponentManager
from tests.functional.test_resources.mock_data_driven_specs_repo \
    import MockDataDrivenSpecsRepo

from monasca_transform.component.usage.fetch_quantity_util import \
    FetchQuantityUtilException
from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.driver.mon_metrics_kafka \
    import MonMetricsKafkaProcessor
from monasca_transform.transform import RddTransformContext
from monasca_transform.transform import TransformContextUtils


class TestFetchQuantityUtilAgg(SparkContextTest):

    def setUp(self):
        super(TestFetchQuantityUtilAgg, self).setUp()
        # configure the system with a dummy messaging adapter
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/functional/test_resources/config/'
                'test_config_with_dummy_messaging_adapter.conf'])
        # reset metric_id list dummy adapter
        if not DummyAdapter.adapter_impl:
            DummyAdapter.init()
        DummyAdapter.adapter_impl.metric_list = []

    def get_pre_transform_specs_json(self):
        """get pre_transform_specs driver table info."""
        pre_transform_specs = ["""
        {"event_processing_params":{"set_default_zone_to":"1",
                                    "set_default_geolocation_to":"1",
                                    "set_default_region_to":"W"},
         "event_type":"cpu.total_logical_cores",
         "metric_id_list":["cpu_util_all"],
         "required_raw_fields_list":["creation_time"],
         "service_id":"host_metrics"}""", """
        {"event_processing_params":{"set_default_zone_to":"1",
                                    "set_default_geolocation_to":"1",
                                    "set_default_region_to":"W"},
         "event_type":"cpu.idle_perc",
         "metric_id_list":["cpu_util_all"],
         "required_raw_fields_list":["creation_time"],
         "service_id":"host_metrics"}"""]
        pre_transform_specs_json_list = \
            [json.loads(pre_transform_spec)
             for pre_transform_spec in pre_transform_specs]

        return pre_transform_specs_json_list

    def get_transform_specs_json_by_operation(self,
                                              usage_fetch_operation):
        """get transform_specs driver table info."""
        transform_specs = ["""
        {"aggregation_params_map":{
               "aggregation_pipeline":{"source":"streaming",
                                       "usage":"fetch_quantity_util",
                                       "setters":["rollup_quantity",
                                                  "set_aggregated_metric_name",
                                                  "set_aggregated_period"],
                                       "insert":["prepare_data",
                                                 "insert_data"]},
               "aggregated_metric_name": "cpu.utilized_logical_cores_agg",
               "aggregation_period": "hourly",
               "aggregation_group_by_list": ["event_type", "host"],
               "usage_fetch_operation": "%s",
               "usage_fetch_util_quantity_event_type":
                    "cpu.total_logical_cores",
               "usage_fetch_util_idle_perc_event_type":
                    "cpu.idle_perc",
               "setter_rollup_group_by_list": [],
               "setter_rollup_operation": "sum",
               "dimension_list":["aggregation_period",
                                 "host",
                                 "project_id"]
         },
         "metric_group":"cpu_util_all",
         "metric_id":"cpu_util_all"}"""]

        transform_specs_json_list = []

        for transform_spec in transform_specs:
            transform_spec_json_operation = \
                transform_spec % usage_fetch_operation
            transform_spec_json = json.loads(
                transform_spec_json_operation)
            transform_specs_json_list.append(transform_spec_json)

        return transform_specs_json_list

    @mock.patch('monasca_transform.data_driven_specs.data_driven_specs_repo.'
                'DataDrivenSpecsRepoFactory.get_data_driven_specs_repo')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_usage_component_manager')
    def test_fetch_quantity_latest(self,
                                   usage_manager,
                                   setter_manager,
                                   insert_manager,
                                   data_driven_specs_repo):

        # test operation
        test_operation = "latest"

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json(),
                                    self.get_transform_specs_json_by_operation(
                                        test_operation))

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

        utilized_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get(
                'name') == 'cpu.utilized_logical_cores_agg'][0]

        self.assertEqual(7.7700000000000005,
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
                         .get('host'))
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

    @mock.patch('monasca_transform.data_driven_specs.data_driven_specs_repo.'
                'DataDrivenSpecsRepoFactory.get_data_driven_specs_repo')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_usage_component_manager')
    def test_fetch_quantity_oldest(self,
                                   usage_manager,
                                   setter_manager,
                                   insert_manager,
                                   data_driven_specs_repo):

        # test operation
        test_operation = "oldest"

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json(),
                                    self.get_transform_specs_json_by_operation(
                                        test_operation))

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

        utilized_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get(
                'name') == 'cpu.utilized_logical_cores_agg'][0]

        self.assertEqual(9.52,
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
                         .get('host'))
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

    @mock.patch('monasca_transform.data_driven_specs.data_driven_specs_repo.'
                'DataDrivenSpecsRepoFactory.get_data_driven_specs_repo')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_usage_component_manager')
    def test_fetch_quantity_avg(self,
                                usage_manager,
                                setter_manager,
                                insert_manager,
                                data_driven_specs_repo):

        # test operation
        test_operation = "avg"

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json(),
                                    self.get_transform_specs_json_by_operation(
                                        test_operation))

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

        utilized_cpu_logical_agg_metric = [
            value for value in metrics
            if value.get('metric').get(
                'name') == 'cpu.utilized_logical_cores_agg'][0]

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
                         .get('host'))
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

    @mock.patch('monasca_transform.data_driven_specs.data_driven_specs_repo.'
                'DataDrivenSpecsRepoFactory.get_data_driven_specs_repo')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_usage_component_manager')
    def test_fetch_quantity_max(self,
                                usage_manager,
                                setter_manager,
                                insert_manager,
                                data_driven_specs_repo):

        # test operation
        test_operation = "max"

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json(),
                                    self.get_transform_specs_json_by_operation(
                                        test_operation))

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

        try:
            # Call the primary method in mon_metrics_kafka
            MonMetricsKafkaProcessor.rdd_to_recordstore(
                rdd_monasca_with_offsets)
            self.assertTrue(False)
        except FetchQuantityUtilException as e:
            self.assertTrue("Operation max is not supported" in
                            e.value)

    @mock.patch('monasca_transform.data_driven_specs.data_driven_specs_repo.'
                'DataDrivenSpecsRepoFactory.get_data_driven_specs_repo')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_usage_component_manager')
    def test_fetch_quantity_min(self,
                                usage_manager,
                                setter_manager,
                                insert_manager,
                                data_driven_specs_repo):

        # test operation
        test_operation = "min"

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json(),
                                    self.get_transform_specs_json_by_operation(
                                        test_operation))

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

        try:
            # Call the primary method in mon_metrics_kafka
            MonMetricsKafkaProcessor.rdd_to_recordstore(
                rdd_monasca_with_offsets)
            self.assertTrue(False)
        except FetchQuantityUtilException as e:
            self.assertTrue("Operation min is not supported" in
                            e.value)

    @mock.patch('monasca_transform.data_driven_specs.data_driven_specs_repo.'
                'DataDrivenSpecsRepoFactory.get_data_driven_specs_repo')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.'
                'generic_transform_builder.GenericTransformBuilder.'
                '_get_usage_component_manager')
    def test_fetch_quantity_sum(self,
                                usage_manager,
                                setter_manager,
                                insert_manager,
                                data_driven_specs_repo):

        # test operation
        test_operation = "sum"

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json(),
                                    self.get_transform_specs_json_by_operation(
                                        test_operation))

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

        try:
            # Call the primary method in mon_metrics_kafka
            MonMetricsKafkaProcessor.rdd_to_recordstore(
                rdd_monasca_with_offsets)
            self.assertTrue(False)
        except FetchQuantityUtilException as e:
            self.assertTrue("Operation sum is not supported" in
                            e.value)

if __name__ == "__main__":
    print("PATH *************************************************************")
    import sys
    print(sys.path)
    print("PATH==============================================================")
    unittest.main()
