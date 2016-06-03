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


class Processor(object):
    """processor object """

    @abc.abstractmethod
    def get_app_name(self):
        """get name of this application. Will be used to
        store offsets in database
        """
        raise NotImplementedError(
            "Class %s doesn't implement get_app_name()"
            % self.__class__.__name__)

    @abc.abstractmethod
    def is_time_to_run(self, current_time):
        """return True if its time to run this processor"""
        raise NotImplementedError(
            "Class %s doesn't implement is_time_to_run()"
            % self.__class__.__name__)

    @abc.abstractmethod
    def run_processor(self, time):
        """Run application"""
        raise NotImplementedError(
            "Class %s doesn't implement run_processor()"
            % self.__class__.__name__)
