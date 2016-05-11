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

from monasca_transform.transform.grouping import Grouping
from monasca_transform.transform.grouping import GroupingResults
from monasca_transform.transform.grouping import RecordStoreWithGroupBy


class GroupSortbyTimestamp(Grouping):

    @staticmethod
    def log_debug(logStr):
        print(str)
        # LOG.debug(logStr)

    @staticmethod
    def _get_group_by_key(row_decorated):
        """Build a group by key using the group by column list.

        row_decorated: [[Rows(a=1, b=1, c=2, d=3)],[group_by_a,group_by_b]]
        """
        # LOG.debug(whoami(row_decorated))
        # LOG.debug(row_decorated)

        group_by_columns_list = row_decorated[1]
        group_by_key = ""
        for gcol in group_by_columns_list:
            group_by_key = "^".join((group_by_key,
                                     eval(".".join(("row", gcol)))))
            return group_by_key

    @staticmethod
    def _prepare_for_group_by(record_store_with_group_by_rdd):
        """creates a new rdd where the first element of each row
        contains array of grouping key and event timestamp fields.
        Grouping key and event timestamp fields are used by
        partitioning and sorting function to partition the data
        by grouping key and then sort the elements in a group by the
        timestamp
        """

        # get the record store data and group by columns
        record_store_data = record_store_with_group_by_rdd.record_store_data

        group_by_columns_list = \
            record_store_with_group_by_rdd.group_by_columns_list

        # construct a group by key
        # key1=value1^key2=value2^...
        group_by_key_value = ""
        for gcol in group_by_columns_list:
            group_by_key_value = \
                "^".join((group_by_key_value,
                          "=".join((gcol,
                                    eval(".".join(("record_store_data",
                                                   gcol)))))))

        # return a key-value rdd
        return [group_by_key_value, record_store_data]

    @staticmethod
    def _sort_by_timestamp(result_iterable):
        # LOG.debug(whoami(result_iterable.data[0]))

        # sort list might cause OOM, if the group has lots of items
        # use group_sort_by_timestamp_partitions module instead if you run
        # into OOM
        sorted_list = sorted(result_iterable.data,
                             key=lambda row: row.event_timestamp_string)
        return sorted_list

    @staticmethod
    def _group_sort_by_timestamp(record_store_df, group_by_columns_list):
        # convert the dataframe rdd to normal rdd and add the group by column
        # list
        record_store_with_group_by_rdd = record_store_df.rdd.\
            map(lambda x: RecordStoreWithGroupBy(x, group_by_columns_list))

        # convert rdd into key-value rdd
        record_store_with_group_by_rdd_key_val = \
            record_store_with_group_by_rdd.\
            map(GroupSortbyTimestamp._prepare_for_group_by)

        first_step = record_store_with_group_by_rdd_key_val.groupByKey()
        record_store_rdd_grouped_sorted = first_step.mapValues(
            GroupSortbyTimestamp._sort_by_timestamp)

        return record_store_rdd_grouped_sorted

    @staticmethod
    def _get_group_first_last_quantity_udf(grouplistiter):
        """Return stats that include first row key, first_event_timestamp,
        first event quantity, last_event_timestamp and last event quantity
        """
        first_row = None
        last_row = None

        # extract key and value list
        group_key = grouplistiter[0]
        grouped_values = grouplistiter[1]

        count = 0.0
        for row in grouped_values:

            # set the first row
            if first_row is None:
                first_row = row

            # set the last row
            last_row = row
            count = count + 1

        first_event_timestamp_unix = None
        first_event_timestamp_string = None
        first_event_quantity = None

        if first_row is not None:
            first_event_timestamp_unix = first_row.event_timestamp_unix
            first_event_timestamp_string = first_row.event_timestamp_string
            first_event_quantity = first_row.event_quantity

        last_event_timestamp_unix = None
        last_event_timestamp_string = None
        last_event_quantity = None

        if last_row is not None:
            last_event_timestamp_unix = last_row.event_timestamp_unix
            last_event_timestamp_string = last_row.event_timestamp_string
            last_event_quantity = last_row.event_quantity

        results_dict = {"firstrecord_timestamp_unix":
                        first_event_timestamp_unix,
                        "firstrecord_timestamp_string":
                        first_event_timestamp_string,
                        "firstrecord_quantity": first_event_quantity,
                        "lastrecord_timestamp_unix":
                        last_event_timestamp_unix,
                        "lastrecord_timestamp_string":
                        last_event_timestamp_string,
                        "lastrecord_quantity": last_event_quantity,
                        "record_count": count}

        group_key_dict = Grouping._parse_grouping_key(group_key)

        return GroupingResults(group_key, results_dict, group_key_dict)

    @staticmethod
    def fetch_group_latest_oldest_quantity(record_store_df,
                                           transform_spec_df,
                                           group_by_columns_list):
        """function to group record store data, sort by timestamp within group
        and get first and last timestamp along with quantity within each group

        This function uses key-value pair rdd's groupBy function to do group_by
        """
        # group and order elements in group
        record_store_grouped_data_rdd = \
            GroupSortbyTimestamp._group_sort_by_timestamp(
                record_store_df, group_by_columns_list)

        # find stats for a group
        record_store_grouped_rows = \
            record_store_grouped_data_rdd.\
            map(GroupSortbyTimestamp.
                _get_group_first_last_quantity_udf)

        return record_store_grouped_rows
