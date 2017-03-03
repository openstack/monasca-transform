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

import os

from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepo
from monasca_transform.transform.transform_utils import PreTransformSpecsUtils
from monasca_transform.transform.transform_utils import TransformSpecsUtils


class JSONDataDrivenSpecsRepo(DataDrivenSpecsRepo):

    def __init__(self, common_file_system_stub_path=None):
        self._common_file_system_stub_path = common_file_system_stub_path or ''

    def get_data_driven_specs(self, sql_context=None,
                              data_driven_spec_type=None):
        path = None
        if data_driven_spec_type == self.transform_specs_type:
            path = (os.path.join(
                    self._common_file_system_stub_path,
                    "tests/functional/data_driven_specs/"
                    "transform_specs/transform_specs.json"
                    ))
            if os.path.exists(path):
                # read file to json
                return TransformSpecsUtils.create_df_from_json(
                    sql_context, path)
        elif data_driven_spec_type == self.pre_transform_specs_type:
            path = (os.path.join(
                    self._common_file_system_stub_path,
                    "tests/functional/data_driven_specs/"
                    "pre_transform_specs/pre_transform_specs.json"
                    ))
            if os.path.exists(path):
                # read file to json
                return PreTransformSpecsUtils.create_df_from_json(
                    sql_context, path)
