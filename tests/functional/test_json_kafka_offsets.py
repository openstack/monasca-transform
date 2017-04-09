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
import json
import os
import random
import sys
import unittest
import uuid

from monasca_transform.offset_specs import OffsetSpec

from tests.functional.json_offset_specs import JSONOffsetSpecs


class TestJSONOffsetSpecs(unittest.TestCase):

    test_resources_path = 'tests/functional/test_resources'

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def get_dummy_batch_time(self):
        """get a batch time for all tests."""
        my_batch_time = datetime.datetime.strptime('2016-01-01 00:00:00',
                                                   '%Y-%m-%d %H:%M:%S')
        return my_batch_time

    def test_read_offsets_on_start(self):
        json_offset_specs = JSONOffsetSpecs(
            path=self.test_resources_path,
            filename='test_read_offsets_on_start.json')
        app_name = "mon_metrics_kafka"
        kafka_offsets = json_offset_specs.get_kafka_offsets(app_name)
        self.assertEqual(1, len(kafka_offsets))
        offset_key_0 = kafka_offsets.iterkeys().next()
        self.assertEqual('mon_metrics_kafka_metrics_0', offset_key_0)
        offset_value_0 = kafka_offsets.get(offset_key_0)
        self.assertEqual('metrics', offset_value_0.get_topic())
        self.assertEqual(85081, offset_value_0.get_until_offset())
        self.assertEqual(0, offset_value_0.get_partition())
        self.assertEqual(app_name, offset_value_0.get_app_name())
        self.assertEqual(84790, offset_value_0.get_from_offset())

    def test_write_offsets_each_add(self):
        filename = '%s.json' % str(uuid.uuid4())
        file_path = os.path.join(self.test_resources_path, filename)
        json_offset_specs = JSONOffsetSpecs(
            path=self.test_resources_path,
            filename=filename
        )
        topic_1 = str(uuid.uuid4())
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = str(uuid.uuid4())
        my_batch_time = self.get_dummy_batch_time()

        used_values = {}
        json_offset_specs.add(topic=topic_1, partition=partition_1,
                              app_name=app_name_1, from_offset=from_offset_1,
                              until_offset=until_offset_1,
                              batch_time_info=my_batch_time)

        kafka_offset_dict = self.load_offset_file_as_json(file_path)
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }
        offset_value_1 = kafka_offset_dict.get(offset_key_1)
        self.assertions_on_offset(used_value=used_values.get(offset_key_1),
                                  offset_value=offset_value_1)

        topic_2 = str(uuid.uuid4())
        partition_2 = random.randint(0, 1024)
        until_offset_2 = random.randint(0, sys.maxsize)
        from_offset_2 = random.randint(0, sys.maxsize)
        app_name_2 = str(uuid.uuid4())
        json_offset_specs.add(topic=topic_2, partition=partition_2,
                              app_name=app_name_2, from_offset=from_offset_2,
                              until_offset=until_offset_2,
                              batch_time_info=my_batch_time)
        offset_key_2 = "%s_%s_%s" % (app_name_2, topic_2, partition_2)
        used_values[offset_key_2] = {
            "topic": topic_2, "partition": partition_2, "app_name": app_name_2,
            "from_offset": from_offset_2, "until_offset": until_offset_2
        }

        kafka_offset_dict = self.load_offset_file_as_json(file_path)
        for key in [offset_key_1, offset_key_2]:
            self.assertions_on_offset(used_value=used_values.get(key),
                                      offset_value=kafka_offset_dict.get(key))

        # if assertions fail then file is left for analysis
        os.remove(file_path)

    def test_as_dict(self):
        topic_1 = str(uuid.uuid4())
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = str(uuid.uuid4())

        offset_spec = OffsetSpec(
            app_name=app_name_1, topic=topic_1, partition=partition_1,
            from_offset=from_offset_1, until_offset=until_offset_1)
        offset_spec_dict = JSONOffsetSpecs.as_dict(offset_spec)
        self.assertions_on_offset(
            used_value={
                "topic": topic_1, "partition": partition_1,
                "app_name": app_name_1, "from_offset": from_offset_1,
                "until_offset": until_offset_1},
            offset_value=offset_spec_dict)

    def test_write_then_read(self):
        filename = '%s.json' % str(uuid.uuid4())
        json_offset_specs = JSONOffsetSpecs(
            path=self.test_resources_path,
            filename=filename
        )
        topic_1 = str(uuid.uuid4())
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = str(uuid.uuid4())
        my_batch_time = self.get_dummy_batch_time()

        used_values = {}
        json_offset_specs.add(topic=topic_1, partition=partition_1,
                              app_name=app_name_1, from_offset=from_offset_1,
                              until_offset=until_offset_1,
                              batch_time_info=my_batch_time)
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }
        topic_2 = str(uuid.uuid4())
        partition_2 = random.randint(0, 1024)
        until_offset_2 = random.randint(0, sys.maxsize)
        from_offset_2 = random.randint(0, sys.maxsize)
        app_name_2 = str(uuid.uuid4())
        json_offset_specs.add(topic=topic_2, partition=partition_2,
                              app_name=app_name_2, from_offset=from_offset_2,
                              until_offset=until_offset_2,
                              batch_time_info=my_batch_time)
        offset_key_2 = "%s_%s_%s" % (app_name_2, topic_2, partition_2)
        used_values[offset_key_2] = {
            "topic": topic_2, "partition": partition_2, "app_name": app_name_2,
            "from_offset": from_offset_2, "until_offset": until_offset_2
        }
        # now create a new JSONOffsetSpecs
        json_offset_specs_2 = JSONOffsetSpecs(self.test_resources_path,
                                              filename)
        found_offsets = json_offset_specs_2.get_kafka_offsets(app_name_2)
        json_found_offsets = {key: JSONOffsetSpecs.as_dict(value)
                              for key, value in found_offsets.items()}
        for key, value in used_values.items():
            found_value = json_found_offsets.get(key)
            self.assertEqual(value.get("app_name"),
                             found_value.get("app_name"))
            self.assertEqual(value.get("topic"), found_value.get("topic"))
            self.assertEqual(value.get("partition"),
                             found_value.get("partition"))
            self.assertEqual(value.get("from_offset"),
                             found_value.get("from_offset"))
            self.assertEqual(value.get("until_offset"),
                             found_value.get("until_offset"))

        os.remove(os.path.join(self.test_resources_path, filename))

    def test_update_offset_values(self):
        filename = '%s.json' % str(uuid.uuid4())
        file_path = os.path.join(self.test_resources_path, filename)
        json_offset_specs = JSONOffsetSpecs(
            path=self.test_resources_path,
            filename=filename
        )
        topic_1 = str(uuid.uuid4())
        partition_1 = random.randint(0, 1024)
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)
        app_name_1 = str(uuid.uuid4())
        my_batch_time = self.get_dummy_batch_time()

        used_values = {}
        json_offset_specs.add(topic=topic_1, partition=partition_1,
                              app_name=app_name_1, from_offset=from_offset_1,
                              until_offset=until_offset_1,
                              batch_time_info=my_batch_time)

        kafka_offset_dict = self.load_offset_file_as_json(file_path)
        offset_key_1 = "%s_%s_%s" % (app_name_1, topic_1, partition_1)
        used_values[offset_key_1] = {
            "topic": topic_1, "partition": partition_1, "app_name": app_name_1,
            "from_offset": from_offset_1, "until_offset": until_offset_1
        }
        offset_value_1 = kafka_offset_dict.get(offset_key_1)
        self.assertions_on_offset(used_value=used_values.get(offset_key_1),
                                  offset_value=offset_value_1)

        until_offset_2 = random.randint(0, sys.maxsize)
        while until_offset_2 == until_offset_1:
            until_offset_2 = random.randint(0, sys.maxsize)

        from_offset_2 = random.randint(0, sys.maxsize)
        while from_offset_2 == from_offset_1:
            from_offset_2 = random.randint(0, sys.maxsize)

        json_offset_specs.add(topic=topic_1, partition=partition_1,
                              app_name=app_name_1, from_offset=from_offset_2,
                              until_offset=until_offset_2,
                              batch_time_info=my_batch_time)

        kafka_offset_dict = self.load_offset_file_as_json(file_path)
        offset_value_updated = kafka_offset_dict.get(offset_key_1)
        self.assertEqual(from_offset_2,
                         offset_value_updated.get('from_offset'))
        self.assertEqual(until_offset_2,
                         offset_value_updated.get('until_offset'))
        os.remove(file_path)

    def test_get_most_recent_batch_time(self):
        filename = '%s.json' % str(uuid.uuid4())
        file_path = os.path.join(self.test_resources_path, filename)
        json_offset_specs = JSONOffsetSpecs(
            path=self.test_resources_path,
            filename=filename
        )
        app_name = "mon_metrics_kafka"

        topic_1 = str(uuid.uuid4())
        partition_1 = 0
        until_offset_1 = random.randint(0, sys.maxsize)
        from_offset_1 = random.randint(0, sys.maxsize)

        my_batch_time = self.get_dummy_batch_time()

        json_offset_specs.add(topic=topic_1, partition=partition_1,
                              app_name=app_name,
                              from_offset=from_offset_1,
                              until_offset=until_offset_1,
                              batch_time_info=my_batch_time)

        most_recent_batch_time = (
            json_offset_specs.get_most_recent_batch_time_from_offsets(
                app_name, topic_1))

        self.assertEqual(most_recent_batch_time, my_batch_time)

        os.remove(file_path)

    def load_offset_file_as_json(self, file_path):
        with open(file_path, 'r') as f:
            json_file = json.load(f)
        return json_file

    @unittest.skip("skipping not implemented")
    def test_get_offsets_is_obj_based(self):
        self.fail('We need to assert that we get objects back '
                  'from the get offsets method')

    def assertions_on_offset(self, used_value=None, offset_value=None):
        self.assertEqual(used_value.get('topic'),
                         offset_value.get('topic'))
        self.assertEqual(used_value.get('partition'),
                         int(offset_value.get('partition')))
        self.assertEqual(used_value.get('until_offset'),
                         int(offset_value.get('until_offset')))
        self.assertEqual(used_value.get('from_offset'),
                         int(offset_value.get('from_offset')))
        self.assertEqual(used_value.get('app_name'),
                         offset_value.get('app_name'))
