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


class ConfigInitializer(object):

    @staticmethod
    def basic_config(default_config_files=None):
        cfg.CONF.reset()
        ConfigInitializer.load_repositories_options()
        ConfigInitializer.load_database_options()
        ConfigInitializer.load_messaging_options()
        ConfigInitializer.load_service_options()
        ConfigInitializer.load_stage_processors_options()
        ConfigInitializer.load_pre_hourly_processor_options()
        if not default_config_files:
            default_config_files = ['/etc/monasca-transform.conf',
                                    'etc/monasca-transform.conf']
        cfg.CONF(args=[],
                 project='monasca_transform',
                 default_config_files=default_config_files)

    @staticmethod
    def load_repositories_options():
        repo_opts = [
            cfg.StrOpt(
                'offsets',
                default='monasca_transform.offset_specs:JSONOffsetSpecs',
                help='Repository for offset persistence'
            ),
            cfg.StrOpt(
                'data_driven_specs',
                default='monasca_transform.data_driven_specs.'
                        'json_data_driven_specs_repo:JSONDataDrivenSpecsRepo',
                help='Repository for metric and event data_driven_specs'
            ),
            cfg.IntOpt('offsets_max_revisions', default=10,
                       help="Max revisions of offsets for each application")
        ]
        repo_group = cfg.OptGroup(name='repositories', title='repositories')
        cfg.CONF.register_group(repo_group)
        cfg.CONF.register_opts(repo_opts, group=repo_group)

    @staticmethod
    def load_database_options():
        db_opts = [
            cfg.StrOpt('server_type'),
            cfg.StrOpt('host'),
            cfg.StrOpt('database_name'),
            cfg.StrOpt('username'),
            cfg.StrOpt('password'),
            cfg.BoolOpt('use_ssl', default=False),
            cfg.StrOpt('ca_file')
        ]
        mysql_group = cfg.OptGroup(name='database', title='database')
        cfg.CONF.register_group(mysql_group)
        cfg.CONF.register_opts(db_opts, group=mysql_group)

    @staticmethod
    def load_messaging_options():
        messaging_options = [
            cfg.StrOpt('adapter',
                       default='monasca_transform.messaging.adapter:'
                       'KafkaMessageAdapter',
                       help='Message adapter implementation'),
            cfg.StrOpt('topic', default='metrics',
                       help='Messaging topic'),
            cfg.StrOpt('brokers',
                       default='192.168.10.4:9092',
                       help='Messaging brokers'),
            cfg.StrOpt('publish_kafka_project_id',
                       default='111111',
                       help='publish aggregated metrics tenant'),
            cfg.StrOpt('publish_region',
                       default='useast',
                       help='publish aggregated metrics region'),
            cfg.StrOpt('adapter_pre_hourly',
                       default='monasca_transform.messaging.adapter:'
                       'KafkaMessageAdapterPreHourly',
                       help='Message adapter implementation'),
            cfg.StrOpt('topic_pre_hourly', default='metrics_pre_hourly',
                       help='Messaging topic pre hourly')
        ]
        messaging_group = cfg.OptGroup(name='messaging', title='messaging')
        cfg.CONF.register_group(messaging_group)
        cfg.CONF.register_opts(messaging_options, group=messaging_group)

    @staticmethod
    def load_service_options():
        service_opts = [
            cfg.StrOpt('coordinator_address'),
            cfg.StrOpt('coordinator_group'),
            cfg.FloatOpt('election_polling_frequency'),
            cfg.BoolOpt('enable_debug_log_entries', default='false'),
            cfg.StrOpt('setup_file'),
            cfg.StrOpt('setup_target'),
            cfg.StrOpt('spark_driver'),
            cfg.StrOpt('service_log_path'),
            cfg.StrOpt('service_log_filename',
                       default='monasca-transform.log'),
            cfg.StrOpt('spark_event_logging_dest'),
            cfg.StrOpt('spark_event_logging_enabled'),
            cfg.StrOpt('spark_jars_list'),
            cfg.StrOpt('spark_master_list'),
            cfg.StrOpt('spark_python_files'),
            cfg.IntOpt('stream_interval'),
            cfg.StrOpt('work_dir'),
            cfg.StrOpt('spark_home'),
            cfg.BoolOpt('enable_record_store_df_cache'),
            cfg.StrOpt('record_store_df_cache_storage_level')
        ]
        service_group = cfg.OptGroup(name='service', title='service')
        cfg.CONF.register_group(service_group)
        cfg.CONF.register_opts(service_opts, group=service_group)

    @staticmethod
    def load_stage_processors_options():
        app_opts = [
            cfg.BoolOpt('pre_hourly_processor_enabled'),
        ]
        app_group = cfg.OptGroup(name='stage_processors',
                                 title='stage_processors')
        cfg.CONF.register_group(app_group)
        cfg.CONF.register_opts(app_opts, group=app_group)

    @staticmethod
    def load_pre_hourly_processor_options():
        app_opts = [
            cfg.IntOpt('late_metric_slack_time', default=600),
            cfg.StrOpt('data_provider',
                       default='monasca_transform.processor.'
                               'pre_hourly_processor:'
                               'PreHourlyProcessorDataProvider'),
            cfg.BoolOpt('enable_instance_usage_df_cache'),
            cfg.StrOpt('instance_usage_df_cache_storage_level'),
            cfg.BoolOpt('enable_batch_time_filtering'),
            cfg.IntOpt('effective_batch_revision', default=2)
        ]
        app_group = cfg.OptGroup(name='pre_hourly_processor',
                                 title='pre_hourly_processor')
        cfg.CONF.register_group(app_group)
        cfg.CONF.register_opts(app_opts, group=app_group)
