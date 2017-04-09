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
from collections import Counter

from pyspark import SQLContext

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepo
from monasca_transform.data_driven_specs.mysql_data_driven_specs_repo \
    import MySQLDataDrivenSpecsRepo
from tests.functional.data_driven_specs.json_data_driven_specs_repo \
    import JSONDataDrivenSpecsRepo
from tests.functional.spark_context_test import SparkContextTest


class TestDataDrivenSpecsRepo(SparkContextTest):

    def setUp(self):
        super(TestDataDrivenSpecsRepo, self).setUp()
        if type(self) is not TestDataDrivenSpecsRepo:
            self.sql_context = SQLContext(self.spark_context)

    def check_transform_specs_data_frame(self, transform_specs_data_frame):
        self.check_metric(
            metric_id='mem_total_all',
            expected_agg_metric_name='mem.total_mb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='mem_usable_all',
            expected_agg_metric_name='mem.usable_mb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='disk_total_all',
            expected_agg_metric_name='disk.total_space_mb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='disk_usable_all',
            expected_agg_metric_name='disk.total_used_space_mb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='nova_vm_cpu_total_all',
            expected_agg_metric_name='nova.vm.cpu.total_allocated_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='nova_vm_mem_total_all',
            expected_agg_metric_name='nova.vm.mem.total_allocated_mb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='vcpus_all',
            expected_agg_metric_name='vcpus_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='vm_mem_total_mb_all',
            expected_agg_metric_name='vm.mem.total_mb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='vm_mem_used_mb_all',
            expected_agg_metric_name='vm.mem.used_mb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='nova_disk_total_allocated_gb_all',
            expected_agg_metric_name='nova.vm.disk.total_allocated_gb_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='vm_disk_allocation_all',
            expected_agg_metric_name='vm.disk.allocation_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='vm_cpu_util_perc_project',
            expected_agg_metric_name='vm.cpu.utilization_perc_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='swift_total_all',
            expected_agg_metric_name='swiftlm.diskusage.val.size_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='swift_avail_all',
            expected_agg_metric_name='swiftlm.diskusage.val.avail_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='swift_usage_rate',
            expected_agg_metric_name='swiftlm.diskusage.rate_agg',
            transform_specs_dataframe=transform_specs_data_frame)
        self.check_metric(
            metric_id='storage_objects_size_all',
            expected_agg_metric_name='storage.objects.size_agg',
            transform_specs_dataframe=transform_specs_data_frame)

    def check_metric(self, metric_id=None, expected_agg_metric_name=None,
                     transform_specs_dataframe=None):

        transform_specs_data_frame = transform_specs_dataframe.select(
            ["aggregation_params_map",
             "metric_id"]
        ).where(
            transform_specs_dataframe.metric_id == metric_id)
        agg_params_json = (transform_specs_data_frame.select(
            "aggregation_params_map.aggregated_metric_name").collect()[0]
            .asDict())
        self.assertEqual(expected_agg_metric_name,
                         agg_params_json["aggregated_metric_name"])

    def check_pre_transform_specs_data_frame(
            self, pre_transform_specs_data_frame, is_json_specs=False):

        if is_json_specs:
            # gather the references and uses here
            self.assertEqual(Counter([u'container.cpu.total_time',
                                      u'cpu.idle_perc',
                                      u'cpu.total_logical_cores',
                                      u'cpu.total_time_sec',
                                      u'disk.total_space_mb',
                                      u'disk.total_used_space_mb',
                                      u'kubernetes.node.allocatable.cpu',
                                      u'kubernetes.node.capacity.cpu',
                                      u'mem.total_mb',
                                      u'mem.usable_mb',
                                      u'nova.vm.cpu.total_allocated',
                                      u'nova.vm.disk.total_allocated_gb',
                                      u'nova.vm.mem.total_allocated_mb',
                                      u'pod.cpu.total_time',
                                      u'pod.mem.used_bytes',
                                      u'pod.net.in_bytes_sec',
                                      u'pod.net.out_bytes_sec',
                                      u'storage.objects.size',
                                      u'swiftlm.diskusage.host.val.avail',
                                      u'swiftlm.diskusage.host.val.size',
                                      u'vcpus',
                                      u'vm.cpu.utilization_perc',
                                      u'vm.disk.allocation',
                                      u'vm.mem.total_mb',
                                      u'vm.mem.used_mb']),
                             Counter(
                                 [row.event_type for row in
                                  pre_transform_specs_data_frame.collect()]))
        else:
            # gather the references and uses here
            self.assertEqual(Counter([u'cpu.idle_perc',
                                      u'cpu.total_logical_cores',
                                      u'disk.total_space_mb',
                                      u'disk.total_used_space_mb',
                                      u'mem.total_mb',
                                      u'mem.usable_mb',
                                      u'nova.vm.cpu.total_allocated',
                                      u'nova.vm.disk.total_allocated_gb',
                                      u'nova.vm.mem.total_allocated_mb',
                                      u'storage.objects.size',
                                      u'swiftlm.diskusage.host.val.avail',
                                      u'swiftlm.diskusage.host.val.size',
                                      u'vcpus',
                                      u'vm.cpu.utilization_perc',
                                      u'vm.disk.allocation',
                                      u'vm.mem.total_mb',
                                      u'vm.mem.used_mb']),
                             Counter(
                                 [row.event_type for row in
                                  pre_transform_specs_data_frame.collect()]))

        # mem.usable_mb
        event_type = 'mem.usable_mb'
        mem_usable_mb_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=mem_usable_mb_row,
            field_name='metric_id_list',
            expected_list=['mem_usable_all']
        )
        self.check_list_field_for_row(
            row=mem_usable_mb_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time'])
        self.check_dict_field_for_row(
            row=mem_usable_mb_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=mem_usable_mb_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # mem.total_mb
        event_type = 'mem.total_mb'
        mem_total_mb_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=mem_total_mb_row,
            field_name='metric_id_list',
            expected_list=['mem_total_all']
        )
        self.check_list_field_for_row(
            row=mem_total_mb_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time'],
        )
        self.check_dict_field_for_row(
            row=mem_total_mb_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=mem_total_mb_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # vcpus
        event_type = 'vcpus'
        vcpus_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=vcpus_all_row,
            field_name='metric_id_list',
            expected_list=['vcpus_all',
                           'vcpus_project']
        )
        self.check_list_field_for_row(
            row=vcpus_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'project_id', 'resource_id'],
        )
        self.check_dict_field_for_row(
            row=vcpus_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=vcpus_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # vm.mem.total_mb
        event_type = 'vm.mem.total_mb'
        vm_mem_total_mb_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='metric_id_list',
            expected_list=['vm_mem_total_mb_all',
                           'vm_mem_total_mb_project']
        )
        self.check_list_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'tenantId', 'resource_id'],
        )
        self.check_dict_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # vm.mem.used_mb
        event_type = 'vm.mem.used_mb'
        vm_mem_total_mb_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='metric_id_list',
            expected_list=['vm_mem_used_mb_all',
                           'vm_mem_used_mb_project']
        )
        self.check_list_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'tenantId', 'resource_id'],
        )
        self.check_dict_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=vm_mem_total_mb_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # nova.vm.disk.total_allocated_gb
        event_type = 'nova.vm.disk.total_allocated_gb'
        disk_total_alloc_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=disk_total_alloc_row,
            field_name='metric_id_list',
            expected_list=['nova_disk_total_allocated_gb_all']
        )
        self.check_list_field_for_row(
            row=disk_total_alloc_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time'],
        )
        self.check_dict_field_for_row(
            row=disk_total_alloc_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=disk_total_alloc_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # vm.disk.allocation
        event_type = 'vm.disk.allocation'
        vm_disk_allocation_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=vm_disk_allocation_all_row,
            field_name='metric_id_list',
            expected_list=['vm_disk_allocation_all',
                           'vm_disk_allocation_project']
        )
        self.check_list_field_for_row(
            row=vm_disk_allocation_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'tenant_id', 'resource_id'],
        )
        self.check_dict_field_for_row(
            row=vm_disk_allocation_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=vm_disk_allocation_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # vm.cpu.utilization_perc
        event_type = 'vm.cpu.utilization_perc'
        vm_cpu_util_perc_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=vm_cpu_util_perc_row,
            field_name='metric_id_list',
            expected_list=['vm_cpu_util_perc_project']
        )
        self.check_list_field_for_row(
            row=vm_cpu_util_perc_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'tenant_id', 'resource_id'],
        )
        self.check_dict_field_for_row(
            row=vm_cpu_util_perc_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=vm_cpu_util_perc_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # swiftlm.diskusage.host.val.size
        event_type = 'swiftlm.diskusage.host.val.size'
        swiftlm_diskusage_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=swiftlm_diskusage_all_row,
            field_name='metric_id_list',
            expected_list=['swift_total_all', 'swift_total_host']
        )
        self.check_list_field_for_row(
            row=swiftlm_diskusage_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'hostname', 'mount'],
        )
        self.check_dict_field_for_row(
            row=swiftlm_diskusage_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=swiftlm_diskusage_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # swiftlm.diskusage.host.val.avail
        event_type = 'swiftlm.diskusage.host.val.avail'
        swiftlm_diskavail_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=swiftlm_diskavail_all_row,
            field_name='metric_id_list',
            expected_list=['swift_avail_all', 'swift_avail_host',
                           'swift_usage_rate']
        )
        self.check_list_field_for_row(
            row=swiftlm_diskavail_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'hostname', 'mount'],
        )
        self.check_dict_field_for_row(
            row=swiftlm_diskavail_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=swiftlm_diskavail_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # nova.vm.cpu.total_allocated
        event_type = 'nova.vm.cpu.total_allocated'
        nova_vm_cpu_total_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=nova_vm_cpu_total_all_row,
            field_name='metric_id_list',
            expected_list=['nova_vm_cpu_total_all']
        )
        self.check_list_field_for_row(
            row=nova_vm_cpu_total_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time'],
        )
        self.check_dict_field_for_row(
            row=nova_vm_cpu_total_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=nova_vm_cpu_total_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # nova.vm.mem.total_allocated_mb
        event_type = 'nova.vm.mem.total_allocated_mb'
        nova_vm_mem_total_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=nova_vm_mem_total_all_row,
            field_name='metric_id_list',
            expected_list=['nova_vm_mem_total_all']
        )
        self.check_list_field_for_row(
            row=nova_vm_mem_total_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time'],
        )
        self.check_dict_field_for_row(
            row=nova_vm_mem_total_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=nova_vm_mem_total_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

        # storage.objects.size
        event_type = 'storage.objects.size'
        storage_objects_size_all_row = self.get_row_for_event_type(
            event_type=event_type,
            pre_transform_specs_data_frame=pre_transform_specs_data_frame)
        self.check_list_field_for_row(
            row=storage_objects_size_all_row,
            field_name='metric_id_list',
            expected_list=['storage_objects_size_all']
        )
        self.check_list_field_for_row(
            row=storage_objects_size_all_row,
            field_name='required_raw_fields_list',
            expected_list=['creation_time', 'project_id'],
        )
        self.check_dict_field_for_row(
            row=storage_objects_size_all_row,
            field_name='event_processing_params',
            expected_dict={
                "set_default_zone_to": "1",
                "set_default_geolocation_to": "1",
                "set_default_region_to": "W"})
        self.check_value_field_for_row(
            row=storage_objects_size_all_row,
            field_name='service_id',
            expected_value='host_metrics'
        )

    def get_row_for_event_type(self,
                               event_type=None,
                               pre_transform_specs_data_frame=None):
        """get row for event type
        :rtype: Row
        """
        rows = pre_transform_specs_data_frame.filter(
            pre_transform_specs_data_frame.event_type == event_type
        ).collect()
        self.assertEqual(
            1, len(rows),
            'There should be only one row for event_type %s' % event_type)
        return rows[0]

    def check_dict_field_for_row(
            self, row=None, field_name=None, expected_dict=None):
        field = getattr(row, field_name)
        values = field.asDict()
        self.assertEqual(expected_dict, values)

    def check_list_field_for_row(
            self, row=None, field_name=None, expected_list=None):
        found_list = getattr(row, field_name)
        self.assertEqual(Counter(expected_list), Counter(found_list))

    def check_value_field_for_row(
            self, row=None, field_name=None, expected_value=None):
        found_value = getattr(row, field_name)
        self.assertEqual(expected_value, found_value)

    @abc.abstractmethod
    def test_transform_specs_data_frame(self):
        pass

    @abc.abstractmethod
    def test_pre_transform_specs_data_frame(self):
        pass


class TestMySQLDataDrivenSpecsRepo(TestDataDrivenSpecsRepo):

    def setUp(self):
        ConfigInitializer.basic_config()
        super(TestMySQLDataDrivenSpecsRepo, self).setUp()
        self.data_driven_specs_repo = MySQLDataDrivenSpecsRepo()

    def tearDown(self):
        super(TestMySQLDataDrivenSpecsRepo, self).tearDown()

    def test_transform_specs_data_frame(self):

        db_transform_specs_data_frame = (
            self.data_driven_specs_repo.get_data_driven_specs(
                sql_context=self.sql_context,
                data_driven_spec_type=DataDrivenSpecsRepo.
                transform_specs_type))

        self.check_transform_specs_data_frame(db_transform_specs_data_frame)

    def test_pre_transform_specs_data_frame(self):

        db_pre_transform_specs_data_frame = (
            self.data_driven_specs_repo.get_data_driven_specs(
                sql_context=self.sql_context,
                data_driven_spec_type=DataDrivenSpecsRepo.
                pre_transform_specs_type))

        self.check_pre_transform_specs_data_frame(
            db_pre_transform_specs_data_frame)


class TestJSONDataDrivenSpecsRepo(TestDataDrivenSpecsRepo):

    def setUp(self):
        super(TestJSONDataDrivenSpecsRepo, self).setUp()
        self.data_driven_specs_repo = JSONDataDrivenSpecsRepo()

    def tearDown(self):
        super(TestJSONDataDrivenSpecsRepo, self).tearDown()

    def test_transform_specs_data_frame(self):

        json_transform_specs_data_frame = (
            self.data_driven_specs_repo.get_data_driven_specs(
                sql_context=self.sql_context,
                data_driven_spec_type=DataDrivenSpecsRepo
                .transform_specs_type))

        self.check_transform_specs_data_frame(json_transform_specs_data_frame)

    def test_pre_transform_specs_data_frame(self):

        json_pre_transform_specs_data_frame = (
            self.data_driven_specs_repo.get_data_driven_specs(
                sql_context=self.sql_context,
                data_driven_spec_type=DataDrivenSpecsRepo.
                pre_transform_specs_type))

        self.check_pre_transform_specs_data_frame(
            json_pre_transform_specs_data_frame,
            is_json_specs=True)
