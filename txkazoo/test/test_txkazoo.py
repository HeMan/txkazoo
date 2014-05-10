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
Tests for `txkazoo.py`.
"""
from __future__ import print_function
import sys

import mock

from kazoo.recipe.partitioner import PartitionState
from twisted.trial.unittest import TestCase
from twisted.internet import task, defer
from twisted.python import log
from txkazoo import TxKazooClient, Lock


class TxKazooTestCase(TestCase):
    """
    Test case mixin for txkazoo tests.
    """
    def setUp(self):
        """
        Mock actual KazooClient and deferToThread.
        """
        self.kazoo_client = mock.patch('txkazoo.KazooClient').start()
        self.kz_obj = self.kazoo_client.return_value
        self.defer_to_thread = mock.patch('twisted.internet.threads.deferToThread').start()
        self.txkzclient = TxKazooClient(hosts='abc', threads=20)


    def tearDown(self):
        """
        Stop the patching.
        """
        mock.patch.stopall()



class TxKazooClientTests(TxKazooTestCase):
    """
    Tests for `TxKazooClient`.
    """
    @mock.patch('txkazoo.reactor')
    def test_init(self, mock_reactor):
        """
        __init__ sets up thread size and creates KazooClient
        """
        self.txkzclient = TxKazooClient(hosts='abc', arg2='12', threads=20)
        mock_reactor.suggestThreadPoolSize.assert_called_once_with(20)
        self.kazoo_client.assert_called_with(hosts='abc', arg2='12')
        self.assertEqual(self.txkzclient.client, self.kz_obj)


    def test_method(self):
        """
        Any method is called in seperate thread
        """
        d = self.txkzclient.start()
        self.defer_to_thread.assert_called_once_with(self.txkzclient.client.start)
        self.assertEqual(d, self.defer_to_thread.return_value)


    def test_property_get(self):
        """
        Accessing property does not defer to thread. It is returned immediately
        """
        s = self.txkzclient.state
        self.assertFalse(self.defer_to_thread.called)
        self.assertEqual(s, self.kazoo_client.return_value.state)



class LockTests(TxKazooTestCase):
    """
    Tests for `Lock`.
    """

    def test_method(self):
        """
        Any method invocation happens in seperate thread
        """
        self.defer_to_thread.return_value = defer.succeed(4)
        _lock = mock.Mock()
        lock = Lock(_lock)
        d = lock.acquire(timeout=10)
        self.assertEqual(self.successResultOf(d), 4)
        self.defer_to_thread.assert_called_once_with(_lock.acquire, timeout=10)



class SetPartitionerTests(TxKazooTestCase):
    """
    Tests for `SetPartitioner`
    """
    def test_init(self):
        """
        Init calls actual SetPartitioner in seperate thread
        """
        kz_part = mock.Mock()
        self.defer_to_thread.return_value = defer.succeed(kz_part)
        part = self.txkzclient.SetPartitioner(
            '/path', set(range(2, 5)), time_boundary=20)
        self.defer_to_thread.assert_called_with(
            self.kz_obj.SetPartitioner, '/path', set(range(2, 5)), time_boundary=20)
        self.assertEqual(part._partitioner, kz_part)
        self.assertIsNone(part._state)


    def test_state_before_object(self):
        """
        .state returns ALLOCATING before SetPartitioner object is created
        """
        self.defer_to_thread.return_value = defer.Deferred()
        partitioner = self.txkzclient.SetPartitioner('/path', set(range(1, 10)))
        self.assertEqual(partitioner.state, PartitionState.ALLOCATING)


    def test_state_after_object(self):
        """
        .state returns SetPartitioner.state after object is created
        """
        kz_part = mock.Mock(state='allocated')
        self.defer_to_thread.return_value = defer.succeed(kz_part)
        partitioner = self.txkzclient.SetPartitioner('/path', set(range(1, 10)))
        self.assertEqual(partitioner.state, 'allocated')


    def test_state_on_error(self):
        """
        .state returns FAILURE if SetPartitioner creation errored
        """
        self.defer_to_thread.return_value = defer.fail(ValueError(2))
        partitioner = self.txkzclient.SetPartitioner('/path', set(range(1, 10)))
        self.assertEqual(partitioner.state, PartitionState.FAILURE)


    def test_state_based_properties(self):
        """
        accessing other state based properties return True or False depending on
        current value of `state`
        """
        attrs = {
            'failed': PartitionState.FAILURE,
            'release': PartitionState.RELEASE,
            'acquired': PartitionState.ACQUIRED,
            'allocating': PartitionState.ALLOCATING
        }
        partitioner = self.txkzclient.SetPartitioner('/path', set(range(1, 10)))
        for attr, value in attrs.items():
            partitioner._state = value
            self.assertTrue(getattr(partitioner, attr))
            partitioner._state = 'else'
            self.assertFalse(getattr(partitioner, attr))


    def test_method_invocation(self):
        """
        Methods are invoked in seperate thread
        """
        # First deferToThread to create SetPartitioner
        part_obj = mock.Mock()
        self.defer_to_thread.return_value = defer.succeed(part_obj)
        partitioner = self.txkzclient.SetPartitioner('/path', set(range(1, 10)))
        # Second deferToThread to call wait_for_acquire
        self.defer_to_thread.return_value = defer.succeed(3)
        d = partitioner.wait_for_acquire(timeout=40)
        self.assertEqual(self.successResultOf(d), 3)
        self.defer_to_thread.assert_called_with(part_obj.wait_for_acquire, timeout=40)


    def test_iter(self):
        """
        __iter__() delegates to actual SetPartitioner.__iter__
        """
        part_obj = mock.MagicMock()
        part_obj.__iter__.return_value = [2, 3]
        self.defer_to_thread.return_value = defer.succeed(part_obj)
        partitioner = self.txkzclient.SetPartitioner('/path', set(range(1, 10)))
        self.assertEqual(list(partitioner), [2, 3])



@defer.inlineCallbacks
def partitioning(reactor, client):
    client.add_listener(zk_listener)
    part = client.SetPartitioner('/manitest_partition', set(range(1,11)), time_boundary=15)
    start = reactor.seconds()
    while True:
        print('current state', client.state)
        if part.failed:
            print('failed, creating new')
            part = client.SetPartitioner('/manitest_partition', set(range(1,11)))
        if part.release:
            print('part changed. releasing')
            start = reactor.seconds()
            yield part.release_set()
        elif part.acquired:
            print('got part {} in {} seconds'.format(list(part), reactor.seconds() - start))
        elif part.allocating:
            print('allocating')
        d = defer.Deferred()
        reactor.callLater(1, d.callback, None)
        yield d



def zk_listener(state):
    print('state change', state)



@defer.inlineCallbacks
def state_changes(reactor, client):
    client.add_listener(zk_listener)
    while True:
        print('state', client.state)
        d = defer.Deferred()
        reactor.callLater(1, d.callback, None)
        yield d



@defer.inlineCallbacks
def locking(reactor, client):
    lock = client.Lock('/locks')
    yield lock.acquire()
    print('got lock')
    d = defer.Deferred()
    reactor.callLater(100, d.callback, None)
    yield d
    yield lock.release()



@defer.inlineCallbacks
def test_via_cli(reactor, hosts):
    log.startLogging(sys.stdout)
    client = TxKazooClient(hosts=hosts, txlog=log)
    yield client.start()
    yield partitioning(reactor, client)
    #yield locking(reactor, client)
    yield client.stop()



if __name__ == '__main__':
    task.react(test_via_cli, sys.argv[1:])
