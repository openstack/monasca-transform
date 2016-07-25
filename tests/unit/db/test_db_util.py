from oslo_config import cfg
from oslo_config.fixture import Config
import unittest

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.db.db_utils import DbUtil


class TestDBUtil(unittest.TestCase):

    def setUp(self):
        ConfigInitializer.basic_config(
            default_config_files=[
                'tests/unit/test_resources/config/test_config.conf'
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
