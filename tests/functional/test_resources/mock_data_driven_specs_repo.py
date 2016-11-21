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

from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepo
from monasca_transform.transform.transform_utils import PreTransformSpecsUtils
from monasca_transform.transform.transform_utils import TransformSpecsUtils


class MockDataDrivenSpecsRepo(DataDrivenSpecsRepo):

    def __init__(self,
                 spark_context,
                 pre_transform_specs_json_list=None,
                 transform_specs_json_list=None):
        self.spark_context = spark_context
        self.pre_transform_specs_json_rdd \
            = self.spark_context.parallelize(pre_transform_specs_json_list)
        self.transform_specs_json_rdd \
            = self.spark_context.parallelize(transform_specs_json_list)

    def get_data_driven_specs(self, sql_context=None,
                              data_driven_spec_type=None):
        if data_driven_spec_type == self.pre_transform_specs_type:
            return PreTransformSpecsUtils.pre_transform_specs_rdd_to_df(
                self.pre_transform_specs_json_rdd)
        elif data_driven_spec_type == self.transform_specs_type:
            return TransformSpecsUtils.transform_specs_rdd_to_df(
                self.transform_specs_json_rdd)
