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

"""Various utilities for testing txkazoo."""
import mock

from twisted.trial.unittest import TestCase
from txkazoo import TxKazooClient


class TxKazooTestCase(TestCase):

    """Test case mixin for txkazoo tests."""

    def setUp(self):
        """Mock actual KazooClient and deferToThread."""
        self.kazoo_client = mock.patch('kazoo.client.KazooClient').start()
        self.kz_obj = self.kazoo_client.return_value
        self.defer_to_thread = mock.patch(
            'twisted.internet.threads.deferToThread').start()
        self.txkzclient = TxKazooClient(hosts='abc', threads=20)

    def tearDown(self):
        """Stop the patching."""
        mock.patch.stopall()
