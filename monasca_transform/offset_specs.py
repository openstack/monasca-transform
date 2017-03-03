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
import six


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

    @abc.abstractmethod
    def get_most_recent_batch_time_from_offsets(self, app_name, topic):
        raise NotImplementedError(
            "Class %s doesn't implement "
            "get_most_recent_batch_time_from_offsets()"
            % self.__class__.__name__)
