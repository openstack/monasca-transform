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
import logging
import os

from monasca_transform.offset_specs import OffsetSpec
from monasca_transform.offset_specs import OffsetSpecs

log = logging.getLogger(__name__)


class JSONOffsetSpecs(OffsetSpecs):

    def __init__(self, path=None, filename=None):
        self.kafka_offset_spec_file = os.path.join(
            (path or "/tmp/"), (filename or 'kafka_offset_specs.json'))

        self._kafka_offsets = {}
        if os.path.exists(self.kafka_offset_spec_file):
            try:
                f = open(self.kafka_offset_spec_file)
                kafka_offset_dict = json.load(f)
                for key, value in kafka_offset_dict.items():
                    log.info("Found offset %s: %s", key, value)
                    self._kafka_offsets[key] = OffsetSpec(
                        app_name=value.get('app_name'),
                        topic=value.get('topic'),
                        partition=value.get('partition'),
                        from_offset=value.get('from_offset'),
                        until_offset=value.get('until_offset'),
                        batch_time=value.get('batch_time'),
                        last_updated=value.get('last_updated'),
                        revision=value.get('revision')
                    )
            except Exception:
                log.info('Invalid or corrupts offsets file found at %s,'
                         ' starting over' % self.kafka_offset_spec_file)
        else:
            log.info('No kafka offsets found at startup')

    def _save(self):
        """get the specs of last run time of offset
        """
        log.info("Saving json offsets: %s", self._kafka_offsets)

        with open(self.kafka_offset_spec_file, 'w') as offset_file:
            offset_file.write('{')
            # json_values = []
            # for key, value in self._kafka_offsets.items():
            #     json_values.append({key: })
            offset_file.write(','.join(
                ['\"%s\": %s' % (key, json.dumps(self.as_dict(value)))
                 for key, value in self._kafka_offsets.items()]))
            offset_file.write('}')

    @staticmethod
    def as_dict(offset_value):
        return {"app_name": offset_value.get_app_name(),
                "topic": offset_value.get_topic(),
                "partition": offset_value.get_partition(),
                "from_offset": offset_value.get_from_offset(),
                "until_offset": offset_value.get_until_offset(),
                "batch_time": offset_value.get_batch_time(),
                "last_updated": offset_value.get_last_updated(),
                "revision": offset_value.get_revision()}

    def add(self, app_name, topic, partition,
            from_offset, until_offset, batch_time_info):

        # batch time
        batch_time = \
            batch_time_info.strftime(
                '%Y-%m-%d %H:%M:%S')

        # last updated
        last_updated = \
            datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')

        NEW_REVISION_NO = 1

        key_name = "%s_%s_%s" % (
            app_name, topic, partition)
        offset = OffsetSpec(
            app_name=app_name,
            topic=topic,
            partition=partition,
            from_offset=from_offset,
            until_offset=until_offset,
            batch_time=batch_time,
            last_updated=last_updated,
            revision=NEW_REVISION_NO
        )
        log.info('Adding offset %s for key %s to current offsets: %s' %
                 (offset, key_name, self._kafka_offsets))
        self._kafka_offsets[key_name] = offset
        log.info('Added so kafka offset is now  %s', self._kafka_offsets)
        self._save()

    def get_kafka_offsets(self, app_name):
        return self._kafka_offsets

    def get_most_recent_batch_time_from_offsets(self, app_name, topic):
        try:
            # get partition 0 as a representative of all others
            key = "%s_%s_%s" % (app_name, topic, 0)
            offset = self._kafka_offsets[key]
            most_recent_batch_time = datetime.datetime.strptime(
                offset.get_batch_time(),
                '%Y-%m-%d %H:%M:%S')
        except Exception:
            most_recent_batch_time = None

        return most_recent_batch_time

    def delete_all_kafka_offsets(self, app_name):
        log.info("Deleting json offsets file: %s", self.kafka_offset_spec_file)
        os.remove(self.kafka_offset_spec_file)

    def add_all_offsets(self, app_name, offsets, batch_time_info):

        # batch time
        batch_time = \
            batch_time_info.strftime(
                '%Y-%m-%d %H:%M:%S')

        # last updated
        last_updated = \
            datetime.datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S')

        NEW_REVISION_NO = -1

        for o in offsets:

            key_name = "%s_%s_%s" % (
                app_name, o.topic, o.partition)

            offset = OffsetSpec(
                topic=o.topic,
                app_name=app_name,
                partition=o.partition,
                from_offset=o.fromOffset,
                until_offset=o.untilOffset,
                batch_time=batch_time,
                last_updated=last_updated,
                revision=NEW_REVISION_NO)

            log.info('Adding offset %s for key %s to current offsets: %s' %
                     (offset, key_name, self._kafka_offsets))
            self._kafka_offsets[key_name] = offset
            log.info('Added so kafka offset is now  %s', self._kafka_offsets)
            self._save()
