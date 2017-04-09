# (c) Copyright 2016 Hewlett Packard Enterprise Development LP
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
from oslo_config.fixture import Config
import unittest

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.db.db_utils import DbUtil


class TestDBUtil(unittest.TestCase):

    def setUp(self):
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/functional/test_resources/config/test_config.conf'
            ])
        self.config = Config()
        self.config.config(
            group='database',
            use_ssl=True,
            host='test_ssl_hostname',
            server_type='jdbc_driver',
            database_name='db_name',
            username='test_user',
            password='pwd',
            ca_file='ca_file')
        self.config.setUp()

    def tearDown(self):
        self.config.cleanUp()

    def test_get_java_db_connection_string_with_ssl(self):
        self.assertEqual(
            'jdbc:jdbc_driver://test_ssl_hostname/db_name?user=test_user'
            '&password=pwd&useSSL=True&requireSSL=True',
            DbUtil.get_java_db_connection_string(cfg.CONF))

    def test_get_python_db_connection_string_with_ssl(self):
        self.assertEqual(
            'mysql+pymysql://test_user:pwd@test_ssl_hostname/db_name?'
            'ssl_ca=ca_file',
            DbUtil.get_python_db_connection_string(cfg.CONF))

    def test_get_java_db_connection_string_without_ssl(self):
        self.config.config(group='database', use_ssl=False)
        self.assertEqual(
            'jdbc:jdbc_driver://test_ssl_hostname/db_name?'
            'user=test_user&password=pwd',
            DbUtil.get_java_db_connection_string())

    def test_get_python_db_connection_string_without_ssl(self):
        self.config.config(group='database', use_ssl=False)
        self.assertEqual(
            'mysql+pymysql://test_user:pwd@test_ssl_hostname/db_name',
            DbUtil.get_python_db_connection_string())
