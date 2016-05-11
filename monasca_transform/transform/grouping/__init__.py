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

from collections import namedtuple

RecordStoreWithGroupByBase = namedtuple("RecordStoreWithGroupBy",
                                        ["record_store_data",
                                         "group_by_columns_list"])


class RecordStoreWithGroupBy(RecordStoreWithGroupByBase):
    """A tuple which is a wrapper containing record store data
    and the group by columns

    namdetuple contains:

    record_store_data - record store data
    group_by_columns_list - group by columns list
    """

GroupingResultsBase = namedtuple("GroupingResults",
                                 ["grouping_key",
                                  "results",
                                  "grouping_key_dict"])


class GroupingResults(GroupingResultsBase):
    """A tuple which is a wrapper containing grouping key
    and grouped result set

    namdetuple contains:

    grouping_key - group by key
    results - grouped results
    grouping_key_dict - group by key as dictionary
    """


class Grouping(object):
    """Base class for all grouping classes."""

    @staticmethod
    def _parse_grouping_key(grouping_str):
        """parse grouping key which in "^key1=value1^key2=value2..." format
        into a dictionary of key value pairs
        """
        group_by_dict = {}
        #
        # convert key=value^key1=value1 string into a dict
        #
        for key_val_pair in grouping_str.split("^"):
            if "=" in key_val_pair:
                key_val = key_val_pair.split("=")
                group_by_dict[key_val[0]] = key_val[1]

        return group_by_dict
