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

from pyspark.context import SparkContext
from pyspark import SparkConf
import unittest


class SparkContextTest(unittest.TestCase):

    def setUp(self):
        # Create a local Spark context with 4 cores
        spark_conf = SparkConf().setMaster('local[4]')
        self.spark_context = SparkContext.getOrCreate(conf=spark_conf)

    def tearDown(self):
        # we don't stop the spark context because it doesn't work cleanly,
        # a context is left behind that cannot work.  Instead we rely on the
        # context to be shutdown at the end of the tests run
        pass
