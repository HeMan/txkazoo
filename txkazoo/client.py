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

"""The Twistified Kazoo client."""
from functools import partial
from txkazoo.log import TxLogger
from kazoo import client
from twisted.internet import reactor, threads
from txkazoo.recipe.lock import Lock
from txkazoo.recipe.partitioner import SetPartitioner


class TxKazooClient(object):

    """Twisted wrapper for `kazoo.client.KazooClient`.

    Runs blocking methods of `kazoo.client.KazooClient` in a different
    thread, returning Deferreds that fire with the method's return value
    or errback with any exception occurred during method execution.

    """

    kz_get_attributes = [
        'handler',
        'retry',
        'state',
        'client_state',
        'client_id',
        'connected'
    ]

    def __init__(self, **kwargs):
        """Initialize `TxKazooClient`.

        Takes same arguments as KazooClient and extra keyword argument
        `threads` that suggests thread pool size to be used

        """
        num_threads = kwargs.pop('threads', 10)
        reactor.suggestThreadPoolSize(num_threads)

        log = kwargs.pop('txlog', None)
        if log:
            kwargs['logger'] = TxLogger(log)

        self.client = client.KazooClient(**kwargs)
        self._internal_listeners = dict()

    def __getattr__(self, name):
        """Delegates method executions to a thread pool. Does this by returning
        a function that calls `KazooClient.method` in seperate thread if `name`
        is method name.

        :return: `Deferred` that fires with result of executed method if `name`
                 is name of method. Otherwise, `KazooClient.property` value is returned

        """
        if name in self.kz_get_attributes:
            # Assuming all attributes access are not blocking
            return getattr(self.client, name)

        blocking_method = getattr(self.client, name)
        return partial(threads.deferToThread, blocking_method)

    def add_listener(self, listener):
        # This call does not block and is probably not thread safe. It is best if it
        # is called from twisted reactor thread only

        def _listener(state):
            # Called from kazoo thread. Replaying the original listener in reactor
            # thread
            reactor.callFromThread(listener, state)

        self._internal_listeners[listener] = _listener
        return self.client.add_listener(_listener)

    def remove_listener(self, listener):
        _listener = self._internal_listeners.pop(listener)
        self.client.remove_listener(_listener)

    def _watch_func(self, func, path, watch=None, **kwargs):
        if not watch:
            return threads.deferToThread(func, path, **kwargs)

        def _watch(event):
            # Called from kazoo thread. Replaying in reactor
            reactor.callFromThread(watch, event)

        return threads.deferToThread(func, path, watch=_watch, **kwargs)

    def exists(self, path, watch=None):
        return self._watch_func(self.client.exists, path, watch)

    def exists_async(self, path, watch=None):
        return self._watch_func(self.client.exists_async, path, watch)

    def get(self, path, watch=None):
        return self._watch_func(self.client.get, path, watch)

    def get_async(self, path, watch=None):
        return self._watch_func(self.client.get_async, path, watch)

    def get_children(self, path, watch=None, include_data=False):
        return self._watch_func(self.client.get_children, path, watch, include_data=include_data)

    def get_children_async(self, path, watch=None, include_data=False):
        return self._watch_func(self.client.get_children_async, path, watch, include_data=include_data)

    def Lock(self, path, identifier=None):
        """Return Twisted wrapper for `Lock` object corresponding to this
        client."""
        return Lock(self.client.Lock(path, identifier))

    def SetPartitioner(self, path, set, **kwargs):
        """Return Twisted wrapper for `SetPartitioner` object corresponding to
        this client."""
        return SetPartitioner(self.client, path, set, **kwargs)
