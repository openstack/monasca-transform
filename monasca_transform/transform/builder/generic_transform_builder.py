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
from monasca_transform.log_utils import LogUtils

from stevedore import extension


class GenericTransformBuilder (object):
    """Build transformation pipeline based on
    aggregation_pipeline spec in metric processing
    configuration
    """

    _MONASCA_TRANSFORM_USAGE_NAMESPACE = 'monasca_transform.usage'
    _MONASCA_TRANSFORM_SETTER_NAMESPACE = 'monasca_transform.setter'
    _MONASCA_TRANSFORM_INSERT_NAMESPACE = 'monasca_transform.insert'

    @staticmethod
    def log_load_extension_error(manager, entry_point, error):
        LogUtils.log_debug("GenericTransformBuilder: "
                           "log load extension error: manager: {%s},"
                           "entry_point: {%s}, error: {%s}"
                           % (str(manager),
                              str(entry_point),
                              str(error)))

    @staticmethod
    def _get_usage_component_manager():
        """stevedore extension manager for usage components."""
        return extension.ExtensionManager(
            namespace=GenericTransformBuilder
            ._MONASCA_TRANSFORM_USAGE_NAMESPACE,
            on_load_failure_callback=GenericTransformBuilder.
            log_load_extension_error,
            invoke_on_load=False)

    @staticmethod
    def _get_setter_component_manager():
        """stevedore extension manager for setter components."""
        return extension.ExtensionManager(
            namespace=GenericTransformBuilder.
            _MONASCA_TRANSFORM_SETTER_NAMESPACE,
            on_load_failure_callback=GenericTransformBuilder.
            log_load_extension_error,
            invoke_on_load=False)

    @staticmethod
    def _get_insert_component_manager():
        """stevedore extension manager for insert components."""
        return extension.ExtensionManager(
            namespace=GenericTransformBuilder.
            _MONASCA_TRANSFORM_INSERT_NAMESPACE,
            on_load_failure_callback=GenericTransformBuilder.
            log_load_extension_error,
            invoke_on_load=False)

    @staticmethod
    def _parse_transform_pipeline(transform_spec_df):
        """parse aggregation pipeline from metric
        processing configuration
        """
        # get aggregation pipeline df
        aggregation_pipeline_df = transform_spec_df\
            .select("aggregation_params_map.aggregation_pipeline")

        # call components
        source_row = aggregation_pipeline_df\
            .select("aggregation_pipeline.source").collect()[0]
        source = source_row.source

        usage_row = aggregation_pipeline_df\
            .select("aggregation_pipeline.usage").collect()[0]
        usage = usage_row.usage

        setter_row_list = aggregation_pipeline_df\
            .select("aggregation_pipeline.setters").collect()
        setter_list = [setter_row.setters for setter_row in setter_row_list]

        insert_row_list = aggregation_pipeline_df\
            .select("aggregation_pipeline.insert").collect()
        insert_list = [insert_row.insert for insert_row in insert_row_list]
        return (source, usage, setter_list[0], insert_list[0])

    @staticmethod
    def do_transform(transform_context,
                     record_store_df):
        """Build a dynamic aggregation pipeline and call components to
        process record store dataframe
        """
        transform_spec_df = transform_context.transform_spec_df_info
        (source,
         usage,
         setter_list,
         insert_list) = GenericTransformBuilder.\
            _parse_transform_pipeline(transform_spec_df)

        # FIXME: source is a placeholder for non-streaming source
        # in the future?

        usage_component = GenericTransformBuilder.\
            _get_usage_component_manager()[usage].plugin

        instance_usage_df = usage_component.usage(transform_context,
                                                  record_store_df)

        for setter in setter_list:
            setter_component = GenericTransformBuilder.\
                _get_setter_component_manager()[setter].plugin
            instance_usage_df = setter_component.setter(transform_context,
                                                        instance_usage_df)

        for insert in insert_list:
            insert_component = GenericTransformBuilder.\
                _get_insert_component_manager()[insert].plugin
            instance_usage_df = insert_component.insert(transform_context,
                                                        instance_usage_df)

        return instance_usage_df
