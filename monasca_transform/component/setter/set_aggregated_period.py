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

from monasca_transform.component import InstanceUsageDataAggParams
from monasca_transform.component.setter import SetterComponent
from monasca_transform.transform.transform_utils import InstanceUsageUtils

import json


class SetAggregatedPeriod(SetterComponent):
    """setter component that sets final aggregated metric name.
    aggregated metric name is available as a parameter 'aggregated_metric_name'
    in aggregation_params in metric processing driver table.
    """
    @staticmethod
    def _set_aggregated_period(instance_usage_agg_params):

        row = instance_usage_agg_params.instance_usage_data

        agg_params = instance_usage_agg_params.agg_params

        try:
            processing_meta = row.processing_meta
        except AttributeError:
            processing_meta = {}

        instance_usage_dict = {"tenant_id": row.tenant_id,
                               "user_id": row.user_id,
                               "resource_uuid": row.resource_uuid,
                               "namespace": row.namespace,
                               "pod_name": row.pod_name,
                               "app": row.app,
                               "container_name": row.container_name,
                               "interface": row.interface,
                               "deployment": row.deployment,
                               "daemon_set": row.daemon_set,
                               "geolocation": row.geolocation,
                               "region": row.region,
                               "zone": row.zone,
                               "host": row.host,
                               "project_id": row.project_id,
                               "aggregated_metric_name":
                                   row.aggregated_metric_name,
                               "quantity": row.quantity,
                               "firstrecord_timestamp_unix":
                                   row.firstrecord_timestamp_unix,
                               "firstrecord_timestamp_string":
                                   row.firstrecord_timestamp_string,
                               "lastrecord_timestamp_unix":
                                   row.lastrecord_timestamp_unix,
                               "lastrecord_timestamp_string":
                                   row.lastrecord_timestamp_string,
                               "record_count": row.record_count,
                               "service_group": row.service_group,
                               "service_id": row.service_id,
                               "usage_date": row.usage_date,
                               "usage_hour": row.usage_hour,
                               "usage_minute": row.usage_minute,
                               "aggregation_period":
                                   agg_params["aggregation_period"],
                               "processing_meta": processing_meta}

        instance_usage_data_json = json.dumps(instance_usage_dict)

        return instance_usage_data_json

    @staticmethod
    def setter(transform_context, instance_usage_df):
        """set the aggregated metric name field for elements in instance usage
        rdd
        """

        transform_spec_df = transform_context.transform_spec_df_info

        agg_params = transform_spec_df.select(
            "aggregation_params_map.aggregation_period").collect()[0].asDict()

        instance_usage_df_agg_params = instance_usage_df.rdd.map(
            lambda x: InstanceUsageDataAggParams(x, agg_params))

        instance_usage_json_rdd = instance_usage_df_agg_params.map(
            SetAggregatedPeriod._set_aggregated_period)

        sql_context = SQLContext.getOrCreate(instance_usage_df.rdd.context)

        instance_usage_trans_df = InstanceUsageUtils.create_df_from_json_rdd(
            sql_context,
            instance_usage_json_rdd)
        return instance_usage_trans_df
