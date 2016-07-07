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

from pyspark import StorageLevel


class InvalidCacheStorageLevelException(Exception):
    """Exception thrown when an invalid cache storage level is encountered
    Attributes:
    value: string representing the error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class StorageUtils(object):
    """storage util functions"""

    @staticmethod
    def get_storage_level(storage_level_str):
        """get pyspark storage level from storage level
        string
        """
        if (storage_level_str == "DISK_ONLY"):
            return StorageLevel.DISK_ONLY
        elif (storage_level_str == "DISK_ONLY_2"):
            return StorageLevel.DISK_ONLY_2
        elif (storage_level_str == "MEMORY_AND_DISK"):
            return StorageLevel.MEMORY_AND_DISK
        elif (storage_level_str == "MEMORY_AND_DISK_2"):
            return StorageLevel.MEMORY_AND_DISK_2
        elif (storage_level_str == "MEMORY_AND_DISK_SER"):
            return StorageLevel.MEMORY_AND_DISK_SER
        elif (storage_level_str == "MEMORY_AND_DISK_SER_2"):
            return StorageLevel.MEMORY_AND_DISK_SER_2
        elif (storage_level_str == "MEMORY_ONLY"):
            return StorageLevel.MEMORY_ONLY
        elif (storage_level_str == "MEMORY_ONLY_2"):
            return StorageLevel.MEMORY_ONLY_2
        elif (storage_level_str == "MEMORY_ONLY_SER"):
            return StorageLevel.MEMORY_ONLY_SER
        elif (storage_level_str == "MEMORY_ONLY_SER_2"):
            return StorageLevel.MEMORY_ONLY_SER_2
        elif (storage_level_str == "OFF_HEAP"):
            return StorageLevel.OFF_HEAP
        else:
            raise InvalidCacheStorageLevelException(
                "Unrecognized cache storage level: %s" % storage_level_str)
