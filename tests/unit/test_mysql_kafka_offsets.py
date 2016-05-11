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

import random
import sys
import unittest
import uuid

from monasca_transform.config.config_initializer import ConfigInitializer
from monasca_transform.mysql_offset_specs import MySQLOffsetSpecs


class TestMySQLOffsetSpecs(unittest.TestCase):

    def setUp(self):
        ConfigInitializer.basic_config()
        self.kafka_offset_specs = MySQLOffsetSpecs()

    def tearDown(self):
        pass

    def test_add_offset(self):

        topic_1 = str(uuid.uuid4())
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = str(uuid.uuid4())
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)

        used_values = {}
        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1)
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }

        kafka_offset_specs = self.kafka_offset_specs.get_kafka_offsets()
        offset_value_1 = kafka_offset_specs.get(offset_key_1)
        self.assertions_on_offset(used_value=used_values.get(offset_key_1),
                                  offset_value=offset_value_1)

    def test_add_another_offset(self):
        offset_specs_at_outset = self.kafka_offset_specs.get_kafka_offsets()
        offset_count = len(offset_specs_at_outset)
        topic_1 = str(uuid.uuid4())
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = str(uuid.uuid4())
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)

        used_values = {}
        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1)
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }

        kafka_offset_specs = self.kafka_offset_specs.get_kafka_offsets()
        offset_value_1 = kafka_offset_specs.get(offset_key_1)
        self.assertions_on_offset(used_value=used_values.get(offset_key_1),
                                  offset_value=offset_value_1)
        self.assertEqual(offset_count + 1,
                         len(self.kafka_offset_specs.get_kafka_offsets()))

    def test_update_offset_values(self):
        topic_1 = str(uuid.uuid4())
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = str(uuid.uuid4())
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)

        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_1,
                                    until_offset=until_offset_1)

        until_offset_2 = random.randint(0, sys.maxsize)
        while until_offset_2 == until_offset_1:
            until_offset_2 = random.randint(0, sys.maxsize)

        from_offset_2 = random.randint(0, sys.maxsize)
        while from_offset_2 == from_offset_1:
            from_offset_2 = random.randint(0, sys.maxsize)

        self.kafka_offset_specs.add(topic=topic_1, partition=partition_1,
                                    app_name=app_name_1,
                                    from_offset=from_offset_2,
                                    until_offset=until_offset_2)

        kafka_offset_specs = self.kafka_offset_specs.get_kafka_offsets()
        updated_offset_value = kafka_offset_specs.get(offset_key_1)
        self.assertEqual(from_offset_2, updated_offset_value.get_from_offset())
        self.assertEqual(until_offset_2,
                         updated_offset_value.get_until_offset())

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
