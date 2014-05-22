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

import mock

from txkazoo.client import TxKazooClient
from txkazoo.test.util import TxKazooTestCase


class TxKazooClientTests(TxKazooTestCase):

    """Tests for `TxKazooClient`."""
    @mock.patch('txkazoo.client.reactor')
    def test_init(self, mock_reactor):
        """__init__ sets up thread size and creates KazooClient."""
        self.txkzclient = TxKazooClient(hosts='abc', arg2='12', threads=20)
        mock_reactor.suggestThreadPoolSize.assert_called_once_with(20)
        self.kazoo_client.assert_called_with(hosts='abc', arg2='12')
        self.assertEqual(self.txkzclient.client, self.kz_obj)

    def test_method(self):
        """Any method is called in seperate thread."""
        d = self.txkzclient.start()
        self.defer_to_thread.assert_called_once_with(
            self.txkzclient.client.start)
        self.assertEqual(d, self.defer_to_thread.return_value)

    def test_property_get(self):
        """Accessing property does not defer to thread.

        It is returned immediately

        """
        s = self.txkzclient.state
        self.assertFalse(self.defer_to_thread.called)
        self.assertEqual(s, self.kazoo_client.return_value.state)
