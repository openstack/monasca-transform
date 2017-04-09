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

from oslo_config import cfg
import unittest

from monasca_transform.config.config_initializer import ConfigInitializer


class TestConfigInitializer(unittest.TestCase):

    def test_use_specific_config_file(self):

        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/functional/test_resources/config/test_config.conf'
            ])
        self.assertEqual('test_offsets_repo_class',
                         cfg.CONF.repositories.offsets)
        self.assertEqual('test_data_driven_specs_repo_class',
                         cfg.CONF.repositories.data_driven_specs)
        self.assertEqual('test_server_type',
                         cfg.CONF.database.server_type)
        self.assertEqual('test_host_name',
                         cfg.CONF.database.host)
        self.assertEqual('test_database_name',
                         cfg.CONF.database.database_name)
        self.assertEqual('test_database_user_name',
                         cfg.CONF.database.username)
        self.assertEqual('test_database_password',
                         cfg.CONF.database.password)
        self.assertEqual('test_ca_file_path',
                         cfg.CONF.database.ca_file)
        self.assertTrue(cfg.CONF.database.use_ssl)

    def test_use_default_config_file(self):

        ConfigInitializer.basic_config(default_config_files=[])

        self.assertEqual(
            'monasca_transform.mysql_offset_specs:MySQLOffsetSpecs',
            cfg.CONF.repositories.offsets)

        self.assertEqual(
            'monasca_transform.data_driven_specs.'
            'mysql_data_driven_specs_repo:MySQLDataDrivenSpecsRepo',
            cfg.CONF.repositories.data_driven_specs)

        self.assertEqual('mysql:thin',
                         cfg.CONF.database.server_type)
        self.assertEqual('localhost',
                         cfg.CONF.database.host)
        self.assertEqual('monasca_transform',
                         cfg.CONF.database.database_name)
        self.assertEqual('m-transform',
                         cfg.CONF.database.username)
        self.assertEqual('password',
                         cfg.CONF.database.password)
        self.assertIsNone(cfg.CONF.database.ca_file)
        self.assertFalse(cfg.CONF.database.use_ssl)
