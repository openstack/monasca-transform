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
import json
from kafka import KafkaClient
from kafka import SimpleProducer
from oslo_config import cfg
import simport


class MessageAdapter(object):

    adapter_impl = None

    @staticmethod
    def init():
        # object to keep track of offsets
        MessageAdapter.adapter_impl = simport.load(
            cfg.CONF.messaging.adapter)()

    @staticmethod
    def send_metric(metric):
        if not MessageAdapter.adapter_impl:
            MessageAdapter.init()
        MessageAdapter.adapter_impl.do_send_metric(metric)

    @abc.abstractmethod
    def do_send_metric(self, metric):
        raise NotImplementedError(
            "Class %s doesn't implement do_send_metric(self, metric)"
            % self.__class__.__name__)


class KafkaMessageAdapter(MessageAdapter):

    def __init__(self):
        client_for_writing = KafkaClient(cfg.CONF.messaging.brokers)
        self.producer = SimpleProducer(client_for_writing)
        self.topic = cfg.CONF.messaging.topic

    def do_send_metric(self, metric):
        self.producer.send_messages(
            self.topic,
            json.dumps(metric, separators=(',', ':')))
        return
