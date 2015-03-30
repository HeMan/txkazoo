# Copyright 2015 Rackspace, Inc.
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

"""Tests for txkazoo.recipe.watchers."""

from twisted.trial.unittest import SynchronousTestCase

from txkazoo.client import TxKazooClient
from txkazoo.recipe.watchers import watch_children
from txkazoo.test.util import FakeReactor, FakeThreadPool, FakeKazooClient


def FakeChildrenWatch(client, path, func=None, allow_session_lost=True,
                      send_event=False):
    return (client, path, func, allow_session_lost, send_event)


class WatchChildrenTests(SynchronousTestCase):
    """Tests for :func:`watch_children`."""
    def setUp(self):
        self.reactor = FakeReactor()
        self.pool = FakeThreadPool()
        self.client = FakeKazooClient()
        self.tx_client = TxKazooClient(self.reactor, self.pool, self.client)

    def _my_callback(self, children):
        self.assertEqual(self.reactor.context.getContext('virtual-thread'),
                         'reactor')
        return ('called back', children)

    def test_basic_watch(self):
        """
        Parameters are passed to :obj:`ChildrenWatch`, and the callback
        function is wrapped such that it is run in the reactor thread and its
        result is returned to the ChildrenWatch. This is important since
        ChildrenWatch interprets the result to determine whether to continue
        triggering callbacks.
        """
        result = watch_children(self.tx_client, '/foo', self._my_callback,
                                ChildrenWatch=FakeChildrenWatch)
        result = self.successResultOf(result)
        self.assertEqual(result[0], self.tx_client.kazoo_client)
        self.assertEqual(result[1], '/foo')
        self.assertEqual(result[3], True)
        self.assertEqual(result[4], False)

        self.assertEqual(
            result[2](['foo', 'bar']),
            ('called back', ['foo', 'bar']))

    def test_kwargs(self):
        """
        ``allow_session_lost`` and ``send_event`` are passed through to the
        ChildrenWatch.
        """
        result = watch_children(self.tx_client, '/foo', None,
                                allow_session_lost=False,
                                send_event=True,
                                ChildrenWatch=FakeChildrenWatch)
        result = self.successResultOf(result)
        self.assertEqual(result[0], self.tx_client.kazoo_client)
        self.assertEqual(result[1], '/foo')
        self.assertEqual(result[3], False)
        self.assertEqual(result[4], True)

    def test_error(self):
        """
        If the call to :obj:`ChildrenWatch` raises an exception, the returned
        Deferred will fail with that exception.
        """
        def FCW(*args, **kwargs):
            raise RuntimeError('foo')
        result = watch_children(self.tx_client, '/foo', None,
                                ChildrenWatch=FCW)
        f = self.failureResultOf(result)
        self.assertEqual(f.type, RuntimeError)
        self.assertEqual(str(f.value), 'foo')
