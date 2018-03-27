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
import abc
import json
import time

from monasca_common.validation import metrics as metric_validator
from monasca_transform.component import Component
from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.log_utils import LogUtils

from oslo_config import cfg

ConfigInitializer.basic_config()
log = LogUtils.init_logger(__name__)


class InsertComponent(Component):

    @abc.abstractmethod
    def insert(transform_context, instance_usage_df):
        raise NotImplementedError(
            "Class %s doesn't implement setter(instance_usage_df,"
            " transform_spec_df)"
            % __name__)

    @staticmethod
    def get_component_type():
        return Component.INSERT_COMPONENT_TYPE

    @staticmethod
    def _validate_metric(metric):
        """validate monasca metric.
        """
        try:
            # validate metric part, without the wrapper
            metric_validator.validate(metric["metric"])
        except Exception as e:
            log.info("Metric %s is invalid: Exception : %s"
                     % (json.dumps(metric), str(e)))
            return False
        return True

    @staticmethod
    def _prepare_metric(instance_usage_dict, agg_params):
        """transform instance usage rdd to a monasca metric.

        example metric:

        {"metric":{"name":"host_alive_status",
                   "dimensions":{"hostname":"mini-mon",
                                 "observer_host":"devstack",
                                 "test_type":"ssh"},
                   "timestamp":1456858016000,
                   "value":1.0,
                   "value_meta":{"error":
                                 "Unable to open socket to host mini-mon"}
                   },
         "meta":{"tenantId":"8eadcf71fc5441d8956cb9cbb691704e",
                 "region":"useast"},
         "creation_time":1456858034
         }

        """

        current_epoch_seconds = time.time()
        current_epoch_milliseconds = current_epoch_seconds * 1000

        dimension_list = agg_params["dimension_list"]
        # build dimensions dynamically
        dimensions_part = {}
        for dim in dimension_list:
            dimensions_part[dim] = \
                instance_usage_dict.get(dim,
                                        Component.DEFAULT_UNAVAILABLE_VALUE)

        meta_part = {}

        # TODO(someone) determine the appropriate tenant ID to use.  For now,
        # what works is to use the same tenant ID as other metrics specify in
        # their kafka messages (and this appears to change each time mini-mon
        # is re-installed).  The long term solution is to have HLM provide
        # a usable tenant ID to us in a configurable way.  BTW, without a
        # proper/valid tenant ID, aggregated metrics don't get persisted
        # to the Monasca DB.
        meta_part["tenantId"] = cfg.CONF.messaging.publish_kafka_project_id
        meta_part["region"] = cfg.CONF.messaging.publish_region

        value_meta_part = {"record_count": instance_usage_dict.get(
                           "record_count", 0),
                           "firstrecord_timestamp_string":
                           instance_usage_dict.get(
                           "firstrecord_timestamp_string",
                               Component.DEFAULT_UNAVAILABLE_VALUE),
                           "lastrecord_timestamp_string":
                           instance_usage_dict.get(
                           "lastrecord_timestamp_string",
                               Component.DEFAULT_UNAVAILABLE_VALUE)}

        metric_part = {"name": instance_usage_dict.get(
                       "aggregated_metric_name"),
                       "dimensions": dimensions_part,
                       "timestamp": int(current_epoch_milliseconds),
                       "value": instance_usage_dict.get(
                       "quantity", 0.0),
                       "value_meta": value_meta_part}

        metric = {"metric": metric_part,
                  "meta": meta_part,
                  "creation_time": int(current_epoch_seconds)}

        return metric

    @staticmethod
    def _get_metric(row, agg_params):
        """write data to kafka. extracts and formats
        metric data and write s the data to kafka
        """
        instance_usage_dict = {"tenant_id": row.tenant_id,
                               "user_id": row.user_id,
                               "resource_uuid": row.resource_uuid,
                               "geolocation": row.geolocation,
                               "region": row.region,
                               "zone": row.zone,
                               "host": row.host,
                               "project_id": row.project_id,
                               "aggregated_metric_name":
                                   row.aggregated_metric_name,
                               "quantity": row.quantity,
                               "firstrecord_timestamp_string":
                                   row.firstrecord_timestamp_string,
                               "lastrecord_timestamp_string":
                                   row.lastrecord_timestamp_string,
                               "record_count": row.record_count,
                               "service_group": row.service_group,
                               "service_id": row.service_id,
                               "usage_date": row.usage_date,
                               "usage_hour": row.usage_hour,
                                   "usage_minute": row.usage_minute,
                               "aggregation_period":
                                   row.aggregation_period,
                               "namespace":
                                   row.namespace,
                               "pod_name":
                                   row.pod_name,
                               "app":
                                   row.app,
                               "container_name":
                                   row.container_name,
                               "interface":
                                   row.interface,
                               "deployment":
                                   row.deployment,
                               "daemon_set":
                                   row.daemon_set}
        metric = InsertComponent._prepare_metric(instance_usage_dict,
                                                 agg_params)
        return metric

    @staticmethod
    def _get_instance_usage_pre_hourly(row,
                                       metric_id):
        """write data to kafka. extracts and formats
        metric data and writes the data to kafka
        """
        # retrieve the processing meta from the row
        processing_meta = row.processing_meta
        # add transform spec metric id to the processing meta
        if processing_meta:
                processing_meta["metric_id"] = metric_id
        else:
                processing_meta = {"metric_id": metric_id}

        instance_usage_dict = {"tenant_id": row.tenant_id,
                               "user_id": row.user_id,
                               "resource_uuid": row.resource_uuid,
                               "geolocation": row.geolocation,
                               "region": row.region,
                               "zone": row.zone,
                               "host": row.host,
                               "project_id": row.project_id,
                               "aggregated_metric_name":
                                   row.aggregated_metric_name,
                               "quantity": row.quantity,
                               "firstrecord_timestamp_string":
                                   row.firstrecord_timestamp_string,
                               "lastrecord_timestamp_string":
                                   row.lastrecord_timestamp_string,
                               "firstrecord_timestamp_unix":
                                   row.firstrecord_timestamp_unix,
                               "lastrecord_timestamp_unix":
                                   row.lastrecord_timestamp_unix,
                               "record_count": row.record_count,
                               "service_group": row.service_group,
                               "service_id": row.service_id,
                               "usage_date": row.usage_date,
                               "usage_hour": row.usage_hour,
                               "usage_minute": row.usage_minute,
                               "aggregation_period":
                                   row.aggregation_period,
                               "processing_meta": processing_meta,
                               "namespace":
                                   row.namespace,
                               "pod_name":
                                   row.pod_name,
                               "app":
                                   row.app,
                               "container_name":
                                   row.container_name,
                               "interface":
                                   row.interface,
                               "deployment":
                                   row.deployment,
                               "daemon_set":
                                   row.daemon_set}
        return instance_usage_dict

    @staticmethod
    def _write_metrics_from_partition(partlistiter):
        """iterate through all rdd elements in partition
           and write metrics to kafka
           """
        for part in partlistiter:
            agg_params = part.agg_params
            row = part.instance_usage_data
            InsertComponent._write_metric(row, agg_params)
