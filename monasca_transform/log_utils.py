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

import logging
from oslo_config import cfg


class LogUtils(object):
    """util methods  for logging"""

    @staticmethod
    def log_debug(message):
        log = logging.getLogger(__name__)
        print(message)
        log.debug(message)

    @staticmethod
    def who_am_i(obj):
        sep = "*" * 10
        debugstr = "\n".join((sep, "name: %s " % type(obj).__name__))
        debugstr = "\n".join((debugstr, "type: %s" % (type(obj))))
        debugstr = "\n".join((debugstr, "dir: %s" % (dir(obj)), sep))
        LogUtils.log_debug(debugstr)

    @staticmethod
    def init_logger(logger_name):

        # initialize logger
        log = logging.getLogger(logger_name)
        _h = logging.FileHandler('%s/%s' % (
            cfg.CONF.service.service_log_path,
            cfg.CONF.service.service_log_filename))
        _h.setFormatter(logging.Formatter("'%(asctime)s - %(pathname)s:"
                                          "%(lineno)s - %(levelname)s"
                                          " - %(message)s'"))
        log.addHandler(_h)
        if cfg.CONF.service.enable_debug_log_entries:
            log.setLevel(logging.DEBUG)
        else:
            log.setLevel(logging.INFO)

        return log
