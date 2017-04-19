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

import datetime
import random
import sys
import unittest

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.mysql_offset_specs import MySQLOffsetSpecs

from oslo_utils import uuidutils


class TestMySQLOffsetSpecs(unittest.TestCase):

    def setUp(self):
        ConfigInitializer.basic_config()
        self.kafka_offset_specs = MySQLOffsetSpecs()

    def tearDown(self):
        pass

    def get_dummy_batch_time(self):
        """get a batch time for all tests."""
        my_batch_time = datetime.datetime.strptime('2016-01-01 00:00:00',
                                                   '%Y-%m-%d %H:%M:%S')
        return my_batch_time

    def test_add_offset(self):

        topic_1 = uuidutils.generate_uuid()
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = uuidutils.generate_uuid()
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)

        my_batch_time = self.get_dummy_batch_time()

        used_values = {}
        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1,
                                    batch_time_info=my_batch_time)
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }

        kafka_offset_specs = self.kafka_offset_specs.get_kafka_offsets(
            app_name_1)
        offset_value_1 = kafka_offset_specs.get(offset_key_1)
        self.assertions_on_offset(used_value=used_values.get(offset_key_1),
                                  offset_value=offset_value_1)

    def test_add_another_offset(self):
        topic_1 = uuidutils.generate_uuid()
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = uuidutils.generate_uuid()
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)
        my_batch_time = self.get_dummy_batch_time()

        used_values = {}
        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1,
                                    batch_time_info=my_batch_time)
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }

        kafka_offset_specs = self.kafka_offset_specs.get_kafka_offsets(
            app_name_1)
        offset_value_1 = kafka_offset_specs.get(offset_key_1)
        self.assertions_on_offset(used_value=used_values.get(offset_key_1),
                                  offset_value=offset_value_1)
        self.assertEqual(1,
                         len(self.kafka_offset_specs.get_kafka_offsets(
                             app_name_1)))

    def test_update_offset_values(self):
        topic_1 = uuidutils.generate_uuid()
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = uuidutils.generate_uuid()
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)

        my_batch_time = self.get_dummy_batch_time()

        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1,
                                    batch_time_info=my_batch_time)

        until_offset_2 = random.randint(0, sys.maxsize)
        while until_offset_2 == until_offset_1:
            until_offset_2 = random.randint(0, sys.maxsize)

        from_offset_2 = random.randint(0, sys.maxsize)
        while from_offset_2 == from_offset_1:
            from_offset_2 = random.randint(0, sys.maxsize)

        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_2,
                                    until_offset=until_offset_2,
                                    batch_time_info=my_batch_time)

        kafka_offset_specs = self.kafka_offset_specs.get_kafka_offsets(
            app_name_1)
        updated_offset_value = kafka_offset_specs.get(offset_key_1)
        self.assertEqual(from_offset_2, updated_offset_value.get_from_offset())
        self.assertEqual(until_offset_2,
                         updated_offset_value.get_until_offset())

    def test_get_most_recent_batch_time(self):
        topic_1 = uuidutils.generate_uuid()
        partition_1 = 0
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = uuidutils.generate_uuid()

        my_batch_time = self.get_dummy_batch_time()

        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1,
                                    batch_time_info=my_batch_time)

        most_recent_batch_time = (
            self.kafka_offset_specs.get_most_recent_batch_time_from_offsets(
                app_name_1, topic_1))

        self.assertEqual(most_recent_batch_time, my_batch_time)

    def assertions_on_offset(self, used_value=None, offset_value=None):
        self.assertEqual(used_value.get('topic'),
                         offset_value.get_topic())
        self.assertEqual(used_value.get('partition'),
                         int(offset_value.get_partition()))
        self.assertEqual(used_value.get('until_offset'),
                         int(offset_value.get_until_offset()))
        self.assertEqual(used_value.get('from_offset'),
                         int(offset_value.get_from_offset()))
        self.assertEqual(used_value.get('app_name'),
                         offset_value.get_app_name())

    def test_get_offset_by_revision(self):
        topic_1 = uuidutils.generate_uuid()
        partition_1 = 0
        until_offset_1 = 10
        from_offset_1 = 0
        app_name_1 = uuidutils.generate_uuid()

        my_batch_time = datetime.datetime.strptime('2016-01-01 00:10:00',
                                                   '%Y-%m-%d %H:%M:%S')

        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1,
                                    batch_time_info=my_batch_time)

        until_offset_2 = 20
        from_offset_2 = 10
        my_batch_time2 = datetime.datetime.strptime('2016-01-01 01:10:00',
                                                    '%Y-%m-%d %H:%M:%S')

        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_2,
                                    until_offset=until_offset_2,
                                    batch_time_info=my_batch_time2)

        # get penultimate revision
        penultimate_revision = 2
        kafka_offset_specs = self.kafka_offset_specs\
            .get_kafka_offsets_by_revision(app_name_1,
                                           penultimate_revision)

        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)
        offset_value_1 = kafka_offset_specs.get(offset_key_1)

        used_values = {}
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }

        self.assertions_on_offset(used_value=used_values.get(offset_key_1),
                                  offset_value=offset_value_1)
