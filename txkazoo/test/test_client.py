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

from kazoo.recipe.partitioner import PartitionState
from thimble import Thimble
from txkazoo import client
from txkazoo.test.util import FakeReactor, FakeThreadPool, FakeKazooClient
from twisted.trial.unittest import SynchronousTestCase


class _RunCallbacksInReactorThreadTests(SynchronousTestCase):
    def setUp(self):
        self.reactor = FakeReactor()
        self.client = FakeKazooClient()
        self.wrapper = client._RunCallbacksInReactorThreadWrapper(self.reactor,
                                                                  self.client)
        self.received_event = None

    def test_attrs(self):
        """
        All of the attributes passed to the wrapper are available.
        """
        for self_attr, wrapper_attr in [("reactor", "_reactor"),
                                        ("client", "_client")]:
            self.assertIdentical(getattr(self, self_attr),
                                 getattr(self.wrapper, wrapper_attr))

    def _assert_in_reactor_thread(self, event):
        """
        Asserts that we would be called in the reactor thread, by checking
        that this function's caller is :meth:`FakeReactor.callFromThread`.
        """
        self.assertEqual(self.reactor.context.getContext('virtual-thread'),
                         'reactor')
        self.received_event = event

    def test_add_listener(self):
        """
        Tests that a listener is added to the underlying client, after
        being wrapped in such a way that it would be executed in the
        reactor thread.
        """
        self.wrapper.add_listener(self._assert_in_reactor_thread)
        event = object()
        internal_listener, = self.client.listeners
        internal_listener(event)
        self.assertIdentical(self.received_event, event)

    def test_remove_listener(self):
        """Removing a listener works."""
        listener = lambda state: state
        self.wrapper.add_listener(listener)
        self.assertEqual(len(self.client.listeners), 1)
        self.wrapper.remove_listener(listener)
        self.assertEqual(len(self.client.listeners), 0)

    def test_remove_nonexistent_listener(self):
        """
        Attempting to remove a listener that was never added raises an
        exception.
        """
        self.assertRaises(KeyError, self.wrapper.remove_listener, object())

    def test_regular_method(self):
        """
        Regular methods (methods that do not have a watch function) can be
        accessed through the wrapper.
        """
        self.assertIdentical(self.wrapper.close.im_func,
                             self.client.close.im_func)

    def test_watch_function_method(self):
        """
        Methods that have a watch function will get called with a wrapped
        watch function that calls the original watch function in the
        reactor thread.
        """
        self.wrapper.get("abc", watch=self._assert_in_reactor_thread)
        event = object()
        self.client.watch(event)
        self.assertIdentical(self.received_event, event)

    def test_optional_watch_functions(self):
        """
        Methods that take an optional watch function still work when the watch
        function is not provided.
        """
        self.wrapper.get("abc")
        event = object()
        self.assertEqual(self.client.watch, None)


class TxKazooClientTests(SynchronousTestCase):
    """
    Tests for the twisted-wrapped Kazoo client.
    """
    def setUp(self):
        self.reactor = FakeReactor()
        self.pool = FakeThreadPool()
        self.client = FakeKazooClient()
        self.tx_client = client.TxKazooClient(self.reactor,
                                              self.pool,
                                              self.client)

    def test_reactor(self):
        """
        The txkazoo client uses the appropriate reactor.
        """
        self.assertIdentical(self.tx_client._reactor, self.reactor)

    def test_thread_pool(self):
        """
        The txkazoo client uses the appropriate thread pool.
        """
        self.assertIdentical(self.tx_client._pool, self.pool)

    def test_client_methods(self):
        """
        The blocking txkazoo client methods are asynchronified.
        """
        self.assertTrue(isinstance(self.tx_client, Thimble))
        self.assertEqual(self.tx_client._blocking_methods,
                         client._blocking_client_methods)

    def test_lock(self):
        """
        The Lock class derived from the client works as expected.
        """
        lock = self.tx_client.Lock("xyzzy", identifier="iddqd")
        self.assertIdentical(lock._reactor, self.reactor)
        self.assertIdentical(lock._pool, self.pool)

        self.assertEqual(lock.path, "xyzzy")
        self.assertEqual(lock.identifier, "iddqd")

    def test_partitioner(self):
        """
        The Partitioner class derived from the client works as expected.
        """
        args = "xyzzy", set([1, 2, 3])
        partitioner = self.tx_client.SetPartitioner(*args)
        self.assertEqual(partitioner.state, PartitionState.ALLOCATING)
        self.assertEqual(partitioner._partitioner.args, args)
        self.assertEqual(partitioner._partitioner.kwargs, {})

        partitioner._partitioner.state = PartitionState.ACQUIRED
        self.assertEqual(partitioner.state, PartitionState.ACQUIRED)

    def test_cant_allocate_partitioner(self):
        """When allocating a partitioner raises an exception, the
        SetPartitioner-like object returned is in the failed state.
        """
        def just_raise(*a, **kw):
            raise ValueError("Something went wrong!")
        self.client.SetPartitioner = just_raise
        partitioner = self.tx_client.SetPartitioner("xyzzy", set([1, 2, 3]))
        self.assertTrue(partitioner.failed)

    def test_partitioner_iter(self):
        """Iterating over the wrapper yields results from the wrapped."""
        partitioner = self.tx_client.SetPartitioner("xyzzy", "iddqd")
        self.assertEqual(list(partitioner), [1])
