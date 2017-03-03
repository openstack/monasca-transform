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

from stevedore.extension import Extension
from stevedore.extension import ExtensionManager

from monasca_transform.component.insert.prepare_data import PrepareData
from monasca_transform.component.setter.rollup_quantity \
    import RollupQuantity
from monasca_transform.component.setter.set_aggregated_metric_name \
    import SetAggregatedMetricName
from monasca_transform.component.setter.set_aggregated_period \
    import SetAggregatedPeriod
from monasca_transform.component.usage.calculate_rate \
    import CalculateRate
from monasca_transform.component.usage.fetch_quantity \
    import FetchQuantity
from monasca_transform.component.usage.fetch_quantity_util \
    import FetchQuantityUtil
from tests.functional.component.insert.dummy_insert import DummyInsert
from tests.functional.component.insert.dummy_insert_pre_hourly import \
    DummyInsertPreHourly


class MockComponentManager(object):

    @staticmethod
    def get_usage_cmpt_mgr():
        return ExtensionManager.make_test_instance([Extension(
            'fetch_quantity',
            'monasca_transform.component.usage.'
            'fetch_quantity:'
            'FetchQuantity',
            FetchQuantity(),
            None),
            Extension(
            'fetch_quantity_util',
            'monasca_transform.component.usage.'
            'fetch_quantity_util:'
            'FetchQuantityUtil',
            FetchQuantityUtil(),
            None),
            Extension(
                'calculate_rate',
                'monasca_transform.component.usage.'
                'calculate_rate:'
                'CalculateRate',
                CalculateRate(),
                None),
        ])

    @staticmethod
    def get_setter_cmpt_mgr():
        return ExtensionManager.make_test_instance([Extension(
            'set_aggregated_metric_name',
            'monasca_transform.component.setter.'
            'set_aggregated_metric_name:SetAggregatedMetricName',
            SetAggregatedMetricName(),
            None),
            Extension('set_aggregated_period',
                      'monasca_transform.component.setter.'
                      'set_aggregated_period:SetAggregatedPeriod',
                      SetAggregatedPeriod(),
                      None),
            Extension('rollup_quantity',
                      'monasca_transform.component.setter.'
                      'rollup_quantity:RollupQuantity',
                      RollupQuantity(),
                      None)
        ])

    @staticmethod
    def get_insert_cmpt_mgr():
        return ExtensionManager.make_test_instance([Extension(
            'prepare_data',
            'monasca_transform.component.insert.prepare_data:PrepareData',
            PrepareData(),
            None),
            Extension('insert_data',
                      'tests.functional.component.insert.dummy_insert:'
                      'DummyInsert',
                      DummyInsert(),
                      None),
            Extension('insert_data_pre_hourly',
                      'tests.functional.component.insert.dummy_insert:'
                      'DummyInsert',
                      DummyInsert(),
                      None),
        ])

    @staticmethod
    def get_insert_pre_hourly_cmpt_mgr():
        return ExtensionManager.make_test_instance([Extension(
            'prepare_data',
            'monasca_transform.component.insert.prepare_data:PrepareData',
            PrepareData(),
            None),
            Extension('insert_data',
                      'tests.functional.component.insert.dummy_insert:'
                      'DummyInsert',
                      DummyInsert(),
                      None),
            Extension('insert_data_pre_hourly',
                      'tests.functional.component.insert.'
                      'dummy_insert_pre_hourly:'
                      'DummyInsertPreHourly',
                      DummyInsertPreHourly(),
                      None),
        ])
