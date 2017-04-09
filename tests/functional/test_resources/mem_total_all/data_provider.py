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


class DataProvider(object):

    _resource_path = 'tests/functional/test_resources/mem_total_all/'

    record_store_path = os.path.join(_resource_path,
                                     "record_store_df.txt")

    transform_spec_path = os.path.join(_resource_path,
                                       "transform_spec_df.txt")
