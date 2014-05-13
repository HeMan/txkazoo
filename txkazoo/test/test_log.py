# Copyright 2013-2014 Rackspace, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests for Twisted-wrapped Kazoo logging.
"""
from logging import INFO
from mock import Mock
from twisted.trial.unittest import SynchronousTestCase
from txkazoo.log import TxLogger


class TxLoggerTests(SynchronousTestCase):

    """Tests for ``TxLogger``."""

    def setUp(self):
        self.log = Mock(spec=['msg', 'err'])
        self.txlog = TxLogger(self.log)

    def test_log(self):
        self.txlog.log(INFO, 'abc', c=2)
        self.log.msg.assert_called_once_with('abc', c=2)

    def test_log_arg(self):
        self.txlog.log(INFO, 'abc %d', 3, c=2)
        self.log.msg.assert_called_once_with('abc 3', c=2)

    def test_log_arg_fail(self):
        self.txlog.log(INFO, 'abc %d', 'xyz', c=2)
        self.log.msg.assert_called_once_with('abc %d', c=2)

    def test_debug(self):
        self.txlog.debug('abc', d=4)
        self.log.msg.assert_called_once_with('abc', d=4)

    def test_info(self):
        self.txlog.info('abc', d=4)
        self.log.msg.assert_called_once_with('abc', d=4)

    def test_warning(self):
        self.txlog.info('abc', d=4)
        self.log.msg.assert_called_once_with('abc', d=4)

    def test_error(self):
        self.txlog.error('abc', d=4)
        self.log.err.assert_called_once_with(None, 'abc', d=4)

    def test_exception(self):
        self.txlog.exception('abc', d=4)
        self.log.err.assert_called_once_with(None, 'abc', d=4)
