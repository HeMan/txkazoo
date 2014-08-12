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

from inspect import currentframe
from txkazoo.client import _RunCallbacksInReactorThreadWrapper, TxKazooClient
from txkazoo.test.util import FakeReactor, FakeThreadPool, FakeKazooClient
from twisted.trial.unittest import SynchronousTestCase


class _RunCallbacksInReactorThreadTests(SynchronousTestCase):
    def setUp(self):
        self.reactor = FakeReactor()
        self.pool = FakeThreadPool()
        self.client = FakeKazooClient()
        self.wrapper = _RunCallbacksInReactorThreadWrapper(self.reactor,
                                                           self.pool,
                                                           self.client)
        self.received_event = None

    def test_attrs(self):
        """
        All of the attributes passed to the wrapper are available.
        """
        for self_attr, wrapper_attr in [("reactor", "_reactor"),
                                        ("pool", "_pool"),
                                        ("client", "_client")]:
            self.assertIdentical(getattr(self, self_attr),
                                 getattr(self.wrapper, wrapper_attr))

    def _assert_in_reactor_thread(self, event):
        """
        Asserts that we would be called in the reactor thread, by checking
        that this function's caller is :meth:`FakeReactor.callFromThread`.
        """
        caller_frame = currentframe().f_back
        self.assertIdentical(caller_frame.f_locals["self"], self.reactor)
        self.assertEqual(caller_frame.f_code.co_name, "callFromThread")
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
