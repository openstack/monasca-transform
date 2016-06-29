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
from monasca_common.simport import simport
from oslo_config import cfg
import six


class DataDrivenSpecsRepoFactory(object):

    data_driven_specs_repo = None

    @staticmethod
    def get_data_driven_specs_repo():
        if not DataDrivenSpecsRepoFactory.data_driven_specs_repo:
            DataDrivenSpecsRepoFactory.data_driven_specs_repo = simport.load(
                cfg.CONF.repositories.data_driven_specs)()
        return DataDrivenSpecsRepoFactory.data_driven_specs_repo


@six.add_metaclass(abc.ABCMeta)
class DataDrivenSpecsRepo(object):

    transform_specs_type = 'transform_specs'
    pre_transform_specs_type = 'pre_transform_specs'

    @abc.abstractmethod
    def get_data_driven_specs(self, sql_context=None, type=None):
        raise NotImplementedError(
            "Class %s doesn't implement get_data_driven_specs(self, type=None)"
            % self.__class__.__name__)
