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
import mock
import sys

from twisted.internet import task, defer
from twisted.python import log
from txkazoo import TxKazooClient, Lock
from txkazoo.test.util import TxKazooTestCase


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
