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

import json

from pyspark.sql import DataFrameReader

from monasca_transform.data_driven_specs.data_driven_specs_repo \
    import DataDrivenSpecsRepo
from monasca_transform.db.db_utils import DbUtil


class MySQLDataDrivenSpecsRepo(DataDrivenSpecsRepo):

    transform_specs_data_frame = None
    pre_transform_specs_data_frame = None

    def get_data_driven_specs(self, sql_context=None,
                              data_driven_spec_type=None):
        data_driven_spec = None
        if self.transform_specs_type == data_driven_spec_type:
            if not self.transform_specs_data_frame:
                self.generate_transform_specs_data_frame(
                    spark_context=sql_context._sc,
                    sql_context=sql_context)
            data_driven_spec = self.transform_specs_data_frame
        elif self.pre_transform_specs_type == data_driven_spec_type:
            if not self.pre_transform_specs_data_frame:
                self.generate_pre_transform_specs_data_frame(
                    spark_context=sql_context._sc,
                    sql_context=sql_context)
            data_driven_spec = self.pre_transform_specs_data_frame
        return data_driven_spec

    def generate_transform_specs_data_frame(self, spark_context=None,
                                            sql_context=None):

        data_frame_reader = DataFrameReader(sql_context)
        transform_specs_data_frame = data_frame_reader.jdbc(
            DbUtil.get_java_db_connection_string(),
            'transform_specs'
        )
        data = []
        for item in transform_specs_data_frame.collect():
            spec = json.loads(item['transform_spec'])
            data.append(json.dumps(spec))

        data_frame = sql_context.jsonRDD(spark_context.parallelize(data))
        self.transform_specs_data_frame = data_frame

    def generate_pre_transform_specs_data_frame(self, spark_context=None,
                                                sql_context=None):

        data_frame_reader = DataFrameReader(sql_context)
        pre_transform_specs_data_frame = data_frame_reader.jdbc(
            DbUtil.get_java_db_connection_string(),
            'pre_transform_specs'
        )
        data = []
        for item in pre_transform_specs_data_frame.collect():
            spec = json.loads(item['pre_transform_spec'])
            data.append(json.dumps(spec))

        data_frame = sql_context.jsonRDD(spark_context.parallelize(data))
        self.pre_transform_specs_data_frame = data_frame
