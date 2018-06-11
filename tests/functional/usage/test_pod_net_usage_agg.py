# (c) Copyright 2017 SUSE LLC
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
import unittest

import mock
from oslo_config import cfg
from pyspark.streaming.kafka import OffsetRange
from tests.functional.spark_context_test import SparkContextTest
from tests.functional.test_resources.fetch_quantity_data.data_provider \
    import DataProvider
from tests.functional.test_resources.mock_component_manager \
    import MockComponentManager
from tests.functional.test_resources.mock_data_driven_specs_repo \
    import MockDataDrivenSpecsRepo

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.driver.mon_metrics_kafka \
    import MonMetricsKafkaProcessor
from monasca_transform.transform import RddTransformContext
from monasca_transform.transform import TransformContextUtils
from tests.functional.messaging.adapter import DummyAdapter


class TestPodNetUsageAgg(SparkContextTest):

    def setUp(self):
        super(TestPodNetUsageAgg, self).setUp()
        # configure the system with a dummy messaging adapter
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/functional/test_resources/config/'
                'test_config_with_dummy_messaging_adapter.conf'])
        # reset metric_id list dummy adapter
        if not DummyAdapter.adapter_impl:
            DummyAdapter.init()
        DummyAdapter.adapter_impl.metric_list = []

    def get_pre_transform_specs_json_all(self):
        """get pre_transform_specs driver table info."""
        pre_transform_spec_json = """
        {"event_processing_params":{"set_default_zone_to":"1",
                                    "set_default_geolocation_to":"1",
                                    "set_default_region_to":"W"},
         "event_type":"pod.net.in_bytes_sec",
         "metric_id_list":["pod_net_in_b_per_sec_total_all"],
         "required_raw_fields_list":["creation_time",
                                     "meta#tenantId",
                                     "dimensions#namespace",
                                     "dimensions#pod_name",
                                     "dimensions#app"],
         "service_id":"host_metrics"}"""
        return [json.loads(pre_transform_spec_json)]

    def get_transform_specs_json_all(self):
        """get transform_specs driver table info."""
        transform_spec_json_all = """
        {"aggregation_params_map":{
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
                                             "dimensions#pod_name",
                                             "dimensions#interface",
                                             "dimensions#deployment"],
               "usage_fetch_operation": "avg",
               "filter_by_list": [],
               "setter_rollup_group_by_list":[],
               "setter_rollup_operation": "sum",
               "dimension_list":["aggregation_period",
                                 "dimensions#app",
                                 "dimensions#namespace",
                                 "dimensions#pod_name"],
               "pre_hourly_operation":"avg",
               "pre_hourly_group_by_list":["default"]},
         "metric_group":"pod_net_in_b_per_sec_total_all",
         "metric_id":"pod_net_in_b_per_sec_total_all"}"""
        return [json.loads(transform_spec_json_all)]

    def get_pre_transform_specs_json_namespace(self):
        """get pre_transform_specs driver table info."""
        pre_transform_spec_json = """
        {"event_processing_params":{"set_default_zone_to":"1",
                                    "set_default_geolocation_to":"1",
                                    "set_default_region_to":"W"},
         "event_type":"pod.net.in_bytes_sec",
         "metric_id_list":["pod_net_in_b_per_sec_per_namespace"],
         "required_raw_fields_list":["creation_time",
                                     "meta#tenantId",
                                     "dimensions#namespace",
                                     "dimensions#pod_name",
                                     "dimensions#app"],
         "service_id":"host_metrics"}"""
        return [json.loads(pre_transform_spec_json)]

    def get_transform_specs_json_namespace(self):
        """get transform_specs driver table info."""
        transform_spec_json_namespace = """
        {"aggregation_params_map":{
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
         "metric_id":"pod_net_in_b_per_sec_per_namespace"}"""
        return [json.loads(transform_spec_json_namespace)]

    def get_pre_transform_specs_json_app(self):
        """get pre_transform_specs driver table info."""
        pre_transform_spec_json = """
        {"event_processing_params":{"set_default_zone_to":"1",
                                    "set_default_geolocation_to":"1",
                                    "set_default_region_to":"W"},
         "event_type":"pod.net.in_bytes_sec",
         "metric_id_list":["pod_net_in_b_per_sec_per_app"],
         "required_raw_fields_list":["creation_time",
                                     "meta#tenantId",
                                     "dimensions#namespace",
                                     "dimensions#pod_name",
                                     "dimensions#app"],
         "service_id":"host_metrics"}"""
        return [json.loads(pre_transform_spec_json)]

    def get_transform_specs_json_app(self):
        """get transform_specs driver table info."""
        transform_spec_json_app = """
        {"aggregation_params_map":{
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
               "setter_rollup_group_by_list":["dimensions#app"],
               "setter_rollup_operation": "sum",
               "dimension_list":["aggregation_period",
                                 "dimensions#app",
                                 "dimensions#namespace",
                                 "dimensions#pod_name"],
               "pre_hourly_operation":"avg",
               "pre_hourly_group_by_list":["geolocation",
                                           "region",
                                           "zone",
                                           "aggregated_metric_name",
                                           "aggregation_period",
                                           "dimensions#app"]},
         "metric_group":"pod_net_in_b_per_sec_per_app",
         "metric_id":"pod_net_in_b_per_sec_per_app"}"""
        return [json.loads(transform_spec_json_app)]

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
    def test_pod_net_in_usage_all(self,
                                  usage_manager,
                                  setter_manager,
                                  insert_manager,
                                  data_driven_specs_repo):

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json_all(),
                                    self.get_transform_specs_json_all())

        # Create an emulated set of Kafka messages (these were gathered
        # by extracting Monasca messages from the Metrics queue on mini-mon).

        # Create an RDD out of the mocked Monasca metrics
        with open(DataProvider.fetch_quantity_data_path) as f:
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

        pod_net_usage_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'pod.net.in_bytes_sec_agg' and
            value.get('metric').get('dimensions').get('app') ==
            'all' and
            value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_net_usage_agg_metric is not None)

        self.assertEqual('pod.net.in_bytes_sec_agg',
                         pod_net_usage_agg_metric
                         .get('metric').get('name'))

        self.assertEqual('all',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('app'))

        self.assertEqual('all',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('namespace'))

        self.assertEqual('all',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('pod_name'))

        self.assertEqual(145.88,
                         pod_net_usage_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_usage_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_usage_agg_metric
                         .get('meta').get('tenantId'))

        self.assertEqual('hourly',
                         pod_net_usage_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(6.0,
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:14:47',
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta')
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
    def test_pod_net_in_usage_namespace(self,
                                        usage_manager,
                                        setter_manager,
                                        insert_manager,
                                        data_driven_specs_repo):

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json_namespace(),
                                    self.get_transform_specs_json_namespace())

        # Create an emulated set of Kafka messages (these were gathered
        # by extracting Monasca messages from the Metrics queue on mini-mon).

        # Create an RDD out of the mocked Monasca metrics
        with open(DataProvider.fetch_quantity_data_path) as f:
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

        pod_net_usage_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'pod.net.in_bytes_sec_agg' and
            value.get('metric').get('dimensions').get('app') ==
            'all' and
            value.get('metric').get('dimensions').get('namespace') ==
            'website' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_net_usage_agg_metric is not None)

        self.assertEqual('pod.net.in_bytes_sec_agg',
                         pod_net_usage_agg_metric
                         .get('metric').get('name'))

        self.assertEqual('all',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('app'))

        self.assertEqual('website',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('namespace'))

        self.assertEqual('all',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('pod_name'))

        self.assertEqual(22.94,
                         pod_net_usage_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_usage_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_usage_agg_metric
                         .get('meta').get('tenantId'))

        self.assertEqual('hourly',
                         pod_net_usage_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:14:47',
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta')
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
    def test_pod_net_in_usage_app(self,
                                  usage_manager,
                                  setter_manager,
                                  insert_manager,
                                  data_driven_specs_repo):

        # load components
        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        # init mock driver tables
        data_driven_specs_repo.return_value = \
            MockDataDrivenSpecsRepo(self.spark_context,
                                    self.get_pre_transform_specs_json_app(),
                                    self.get_transform_specs_json_app())

        # Create an emulated set of Kafka messages (these were gathered
        # by extracting Monasca messages from the Metrics queue on mini-mon).

        # Create an RDD out of the mocked Monasca metrics
        with open(DataProvider.fetch_quantity_data_path) as f:
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

        pod_net_usage_agg_metric = [
            value for value in metrics
            if value.get('metric').get('name') ==
            'pod.net.in_bytes_sec_agg' and
            value.get('metric').get('dimensions').get('app') ==
            'junk' and
            value.get('metric').get('dimensions').get('namespace') ==
            'all' and
            value.get('metric').get('dimensions').get('pod_name') ==
            'all'][0]

        self.assertTrue(pod_net_usage_agg_metric is not None)

        self.assertEqual('pod.net.in_bytes_sec_agg',
                         pod_net_usage_agg_metric
                         .get('metric').get('name'))

        self.assertEqual('junk',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('app'))

        self.assertEqual('all',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('namespace'))

        self.assertEqual('all',
                         pod_net_usage_agg_metric
                         .get("metric").get('dimensions').get('pod_name'))

        self.assertEqual(122.94,
                         pod_net_usage_agg_metric
                         .get('metric').get('value'))
        self.assertEqual('useast',
                         pod_net_usage_agg_metric
                         .get('meta').get('region'))

        self.assertEqual(cfg.CONF.messaging.publish_kafka_project_id,
                         pod_net_usage_agg_metric
                         .get('meta').get('tenantId'))

        self.assertEqual('hourly',
                         pod_net_usage_agg_metric
                         .get('metric').get('dimensions')
                         .get('aggregation_period'))

        self.assertEqual(3.0,
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta').get('record_count'))
        self.assertEqual('2017-01-24 20:14:47',
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta')
                         .get('firstrecord_timestamp_string'))
        self.assertEqual('2017-01-24 20:15:47',
                         pod_net_usage_agg_metric
                         .get('metric').get('value_meta')
                         .get('lastrecord_timestamp_string'))

if __name__ == "__main__":
    print("PATH *************************************************************")
    import sys
    print(sys.path)
    print("PATH==============================================================")
    unittest.main()
