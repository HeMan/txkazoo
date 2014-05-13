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

"""Tests for the txkazoo equivalent of ``kazoo.recipe.lock``."""
import mock

from txkazoo.recipe.lock import Lock
from twisted.internet import defer
from txkazoo.test.util import TxKazooTestCase


class LockTests(TxKazooTestCase):

    """Tests for `Lock`."""

    def test_method(self):
        """Any method invocation happens in seperate thread."""
        self.defer_to_thread.return_value = defer.succeed(4)
        _lock = mock.Mock()
        lock = Lock(_lock)
        d = lock.acquire(timeout=10)
        self.assertEqual(self.successResultOf(d), 4)
        self.defer_to_thread.assert_called_once_with(_lock.acquire, timeout=10)
