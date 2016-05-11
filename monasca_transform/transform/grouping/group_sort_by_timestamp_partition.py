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


class GroupSortbyTimestampPartition(Grouping):

    @staticmethod
    def log_debug(logStr):
        print(str)
        # LOG.debug(logStr)

    @staticmethod
    def _get_group_first_last_quantity_udf(partition_list_iter):
        """user defined function to to through a list of partitions. Each
        partition contains elements for a group. All the elements are sorted by
        timestamp.
        The stats include first row key, first_event_timestamp,
        fist event quantity, last_event_timestamp and last event quantity
        """
        first_row = None
        last_row = None

        count = 0.0
        for row in partition_list_iter:

            # set the first row
            if first_row is None:
                first_row = row

            # set the last row
            last_row = row
            count = count + 1

        first_event_timestamp_unix = None
        first_event_timestamp_string = None
        first_event_quantity = None
        first_row_key = None
        if first_row is not None:
            first_event_timestamp_unix = first_row[1].event_timestamp_unix
            first_event_timestamp_string = first_row[1].event_timestamp_string
            first_event_quantity = first_row[1].event_quantity

            # extract the grouping_key from composite grouping_key
            # composite grouping key is a list, where first item is the
            # grouping key and second item is the event_timestamp_string
            first_row_key = first_row[0][0]

        last_event_timestamp_unix = None
        last_event_timestamp_string = None
        last_event_quantity = None
        if last_row is not None:
            last_event_timestamp_unix = last_row[1].event_timestamp_unix
            last_event_timestamp_string = last_row[1].event_timestamp_string
            last_event_quantity = last_row[1].event_quantity

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

        first_row_key_dict = Grouping._parse_grouping_key(first_row_key)

        yield [GroupingResults(first_row_key, results_dict,
                               first_row_key_dict)]

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
                          "=".join((gcol, eval(".".join(("record_store_data",
                                                         gcol)))))))

        # return a key-value rdd
        # key is a composite key which consists of grouping key and
        # event_timestamp_string
        return [[group_by_key_value,
                 record_store_data.event_timestamp_string], record_store_data]

    @staticmethod
    def _get_partition_by_group(group_composite):
        """get a hash of the grouping key, which is then used by partitioning
        function to get partition where the groups data should end up in.
        It uses hash % num_partitions to get partition
        """
        # FIXME: find out of hash function in python gives same value on
        # different machines
        # Look at using portable_hash method in spark rdd
        grouping_key = group_composite[0]
        grouping_key_hash = hash(grouping_key)
        # log_debug("group_by_sort_by_timestamp_partition: got hash : %s" \
        #    % str(returnhash))
        return grouping_key_hash

    @staticmethod
    def _sort_by_timestamp(group_composite):
        """get timestamp which will be used to sort grouped data
        """
        event_timestamp_string = group_composite[1]
        return event_timestamp_string

    @staticmethod
    def _group_sort_by_timestamp_partition(record_store_df,
                                           group_by_columns_list,
                                           num_of_groups):
        """component that does a group by and then sorts all
        the items within the group by event timestamp.
        """
        # convert the dataframe rdd to normal rdd and add the group by
        # column list
        record_store_with_group_by_rdd = record_store_df.rdd.\
            map(lambda x: RecordStoreWithGroupBy(x, group_by_columns_list))

        # prepare the data for repartitionAndSortWithinPartitions function
        record_store_rdd_prepared = \
            record_store_with_group_by_rdd.\
            map(GroupSortbyTimestampPartition._prepare_for_group_by)

        # repartition data based on a grouping key and sort the items within
        # group by timestamp
        # give high number of partitions
        # numPartitions > number of groups expected, so that each group gets
        # allocated a separate partition
        record_store_rdd_partitioned_sorted = \
            record_store_rdd_prepared.\
            repartitionAndSortWithinPartitions(
                numPartitions=num_of_groups,
                partitionFunc=GroupSortbyTimestampPartition.
                _get_partition_by_group,
                keyfunc=GroupSortbyTimestampPartition.
                _sort_by_timestamp)

        return record_store_rdd_partitioned_sorted

    @staticmethod
    def _remove_none_filter(row):
        """remove any rows which have None as grouping key
        [GroupingResults(grouping_key="key1", results={})] rows get created
        when partition does not get any grouped data assigned to it
        """
        if len(row[0].results) > 0 and row[0].grouping_key is not None:
            return row

    @staticmethod
    def fetch_group_first_last_quantity(record_store_df,
                                        transform_spec_df,
                                        group_by_columns_list,
                                        num_of_groups):
        """function to group record store data, sort by timestamp within group
        and get first and last timestamp along with quantity within each group

        To do group by it uses custom partitioning function which creates a new
        partition
        for each group and uses RDD's repartitionAndSortWithinPartitions
        function to do the grouping and sorting within the group.

        This is more scalable than just using RDD's group_by as using this
        technique
        group is not materialized into a list and stored in memory, but rather
        it uses RDD's in built partitioning capability to do the sort

        num_of_groups should be more than expected groups, otherwise the same
        partition can get used for two groups which will cause incorrect
        results.
        """

        # group and order elements in group using repartition
        record_store_grouped_data_rdd = \
            GroupSortbyTimestampPartition.\
            _group_sort_by_timestamp_partition(record_store_df,
                                               group_by_columns_list,
                                               num_of_groups)

        # do some operations on all elements in the group
        grouping_results_tuple_with_none = \
            record_store_grouped_data_rdd.\
            mapPartitions(GroupSortbyTimestampPartition.
                          _get_group_first_last_quantity_udf)

        # filter all rows which have no data (where grouping key is None) and
        # convert resuts into grouping results tuple
        grouping_results_tuple1 = grouping_results_tuple_with_none.\
            filter(GroupSortbyTimestampPartition._remove_none_filter)

        grouping_results_tuple = grouping_results_tuple1.map(lambda x: x[0])

        return grouping_results_tuple
