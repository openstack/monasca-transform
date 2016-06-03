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
from monasca_transform.messaging.adapter import KafkaMessageAdapterPreHourly


class KafkaInsertPreHourly(InsertComponent):
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
            "metric_id").\
            collect()[0].asDict()
        metric_id = agg_params["metric_id"]

        for instance_usage_row in instance_usage_df.collect():
            instance_usage_dict = \
                InsertComponent._get_instance_usage_pre_hourly(
                    instance_usage_row,
                    metric_id)
            KafkaMessageAdapterPreHourly.send_metric(instance_usage_dict)

        return instance_usage_df
