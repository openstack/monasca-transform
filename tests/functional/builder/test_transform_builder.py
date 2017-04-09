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

from pyspark.sql import SQLContext

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.transform.builder.generic_transform_builder \
    import GenericTransformBuilder
from monasca_transform.transform.transform_utils import RecordStoreUtils
from monasca_transform.transform.transform_utils import TransformSpecsUtils
from monasca_transform.transform import TransformContextUtils

from tests.functional.spark_context_test import SparkContextTest
from tests.functional.test_resources.mem_total_all.data_provider \
    import DataProvider
from tests.functional.test_resources.mock_component_manager \
    import MockComponentManager


class TransformBuilderTest(SparkContextTest):

    def setUp(self):
        super(TransformBuilderTest, self).setUp()
        # configure the system with a dummy messaging adapter
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/functional/test_resources/config/test_config.conf'])

    @mock.patch('monasca_transform.transform.builder.generic_transform_builder'
                '.GenericTransformBuilder._get_insert_component_manager')
    @mock.patch('monasca_transform.transform.builder.generic_transform_builder'
                '.GenericTransformBuilder._get_setter_component_manager')
    @mock.patch('monasca_transform.transform.builder.generic_transform_builder'
                '.GenericTransformBuilder._get_usage_component_manager')
    def test_transform_builder(self,
                               usage_manager,
                               setter_manager,
                               insert_manager):

        usage_manager.return_value = MockComponentManager.get_usage_cmpt_mgr()
        setter_manager.return_value = \
            MockComponentManager.get_setter_cmpt_mgr()
        insert_manager.return_value = \
            MockComponentManager.get_insert_cmpt_mgr()

        record_store_json_path = DataProvider.record_store_path

        metric_proc_json_path = DataProvider.transform_spec_path

        sql_context = SQLContext.getOrCreate(self.spark_context)
        record_store_df = \
            RecordStoreUtils.create_df_from_json(sql_context,
                                                 record_store_json_path)

        transform_spec_df = TransformSpecsUtils.create_df_from_json(
            sql_context, metric_proc_json_path)

        transform_context = TransformContextUtils.get_context(
            transform_spec_df_info=transform_spec_df,
            batch_time_info=self.get_dummy_batch_time())

        # invoke the generic transformation builder
        instance_usage_df = GenericTransformBuilder.do_transform(
            transform_context, record_store_df)

        result_list = [(row.usage_date, row.usage_hour,
                        row.tenant_id, row.host, row.quantity,
                        row.aggregated_metric_name)
                       for row in instance_usage_df.rdd.collect()]

        expected_result = [('2016-02-08', '18', 'all',
                            'all', 12946.0,
                            'mem.total_mb_agg')]

        self.assertItemsEqual(result_list, expected_result)
