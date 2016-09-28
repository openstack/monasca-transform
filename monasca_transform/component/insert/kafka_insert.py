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

from monasca_transform.component.insert import InsertComponent
from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.messaging.adapter import KafkaMessageAdapter


class KafkaInsert(InsertComponent):
    """Insert component that writes instance usage data
    to kafka queue
    """

    @staticmethod
    def insert(transform_context, instance_usage_df):
        """write instance usage data to kafka"""

        # object to init config
        ConfigInitializer.basic_config()

        transform_spec_df = transform_context.transform_spec_df_info

        agg_params = transform_spec_df.select(
            "aggregation_params_map.dimension_list").collect()[0].asDict()

        # Approach # 1
        # using foreachPartition to iterate through elements in an
        # RDD is the recommended approach so as to not overwhelm kafka with the
        # zillion connections (but in our case the MessageAdapter does
        # store the adapter_impl so we should not create many producers)

        # using foreachpartitions was causing some serialization/cpickle
        # problems where few libs like kafka.SimpleProducer and oslo_config.cfg
        # were not available in foreachPartition method
        #
        # removing _write_metrics_from_partition for now in favor of
        # Approach # 2
        #

        # instance_usage_df_agg_params = instance_usage_df.rdd.map(
        #    lambda x: InstanceUsageDataAggParams(x,
        #                                        agg_params))
        # instance_usage_df_agg_params.foreachPartition(
        #     DummyInsert._write_metrics_from_partition)

        # Approach # 2
        # using collect() to fetch all elements of an RDD and write to
        # kafka

        for instance_usage_row in instance_usage_df.collect():
            metric = InsertComponent._get_metric(
                instance_usage_row, agg_params)
            # validate metric part
            if InsertComponent._validate_metric(metric):
                KafkaMessageAdapter.send_metric(metric)
        return instance_usage_df
