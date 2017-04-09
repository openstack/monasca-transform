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
from pyspark.sql import SQLContext

from monasca_transform.component.setter.rollup_quantity \
    import RollupQuantity
from monasca_transform.component.setter.set_aggregated_metric_name \
    import SetAggregatedMetricName
from monasca_transform.component.usage.fetch_quantity \
    import FetchQuantity
from monasca_transform.transform.transform_utils import RecordStoreUtils
from monasca_transform.transform.transform_utils import TransformSpecsUtils
from monasca_transform.transform import TransformContextUtils

from tests.functional.spark_context_test import SparkContextTest
from tests.functional.test_resources.mem_total_all.data_provider \
    import DataProvider


class SetAggregatedMetricNameTest(SparkContextTest):

    def setUp(self):
        super(SetAggregatedMetricNameTest, self).setUp()
        self.sql_context = SQLContext(self.spark_context)

    def test_set_aggregated_metric_name(self):

        record_store_df = RecordStoreUtils.create_df_from_json(
            self.sql_context,
            DataProvider.record_store_path)

        transform_spec_df = TransformSpecsUtils.create_df_from_json(
            self.sql_context,
            DataProvider.transform_spec_path)

        transform_context = TransformContextUtils.get_context(
            transform_spec_df_info=transform_spec_df,
            batch_time_info=self.get_dummy_batch_time())

        instance_usage_df = FetchQuantity.usage(
            transform_context, record_store_df)

        instance_usage_df_1 = RollupQuantity.setter(
            transform_context, instance_usage_df)

        instance_usage_df_2 = SetAggregatedMetricName.setter(
            transform_context, instance_usage_df_1)

        result_list = [(row.usage_date, row.usage_hour,
                        row.tenant_id, row.host, row.quantity,
                        row.aggregated_metric_name)
                       for row in instance_usage_df_2.rdd.collect()]

        expected_result = [
            ('2016-02-08', '18', 'all', 'all', 12946.0, 'mem.total_mb_agg')]

        self.assertItemsEqual(result_list, expected_result)
