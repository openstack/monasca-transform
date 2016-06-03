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
import datetime
import json
import logging
import os
import six

log = logging.getLogger(__name__)


class OffsetSpec(object):

    def __init__(self, app_name=None, topic=None, partition=None,
                 from_offset=None, until_offset=None,
                 batch_time=None, last_updated=None,
                 revision=None):

        self.app_name = app_name
        self.topic = topic
        self.partition = partition
        self.from_offset = from_offset
        self.until_offset = until_offset
        self.batch_time = batch_time
        self.last_updated = last_updated
        self.revision = revision

    def get_app_name(self):
        return self.app_name

    def get_topic(self):
        return self.topic

    def get_partition(self):
        return self.partition

    def get_from_offset(self):
        return self.from_offset

    def get_until_offset(self):
        return self.until_offset

    def get_batch_time(self):
        return self.batch_time

    def get_last_updated(self):
        return self.last_updated

    def get_revision(self):
        return self.revision


@six.add_metaclass(abc.ABCMeta)
class OffsetSpecs(object):
    """Class representing offset specs to help recover.
    From where processing should pick up in case of failure
    """

    @abc.abstractmethod
    def add(self, app_name, topic, partition,
            from_offset, until_offset, batch_time_info):
        raise NotImplementedError(
            "Class %s doesn't implement add(self, app_name, topic, "
            "partition, from_offset, until_offset, batch_time,"
            "last_updated, revision)"
            % self.__class__.__name__)

    @abc.abstractmethod
    def add_all_offsets(self, app_name, offsets, batch_time_info):
        raise NotImplementedError(
            "Class %s doesn't implement add(self, app_name, topic, "
            "partition, from_offset, until_offset, batch_time,"
            "last_updated, revision)"
            % self.__class__.__name__)

    @abc.abstractmethod
    def get_kafka_offsets(self, app_name):
        raise NotImplementedError(
            "Class %s doesn't implement get_kafka_offsets()"
            % self.__class__.__name__)

    @abc.abstractmethod
    def delete_all_kafka_offsets(self, app_name):
        raise NotImplementedError(
            "Class %s doesn't implement delete_all_kafka_offsets()"
            % self.__class__.__name__)


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
