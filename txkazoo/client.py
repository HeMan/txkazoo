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
from functools import partial, wraps
from funcsigs import signature
from txkazoo.recipe.partitioner import _SetPartitionerWrapper
from thimble import Thimble


class _RunCallbacksInReactorThreadWrapper(object):

    """
    A wrapper for a Kazoo client.

    Its job is making sure the listeners and watch functions are called in a
    reactor thread.

    This is internal, because it doesn't make any sense to use by itself; it
    wraps a blocking Kazoo client, but assumes that e.g. listeners and watch
    functions ought to be called in the reactor thread. As a result, it only
    makes sense if used in conjunction with something that will defer all the
    blocking method calls to a thread pool, such as a :class:`Thimble`.
    """

    _methods_with_watch_fn = ("exists",
                              "exists_async",
                              "get",
                              "get_async",
                              "get_children",
                              "get_children_async")

    def __init__(self, reactor, client):
        self._reactor = reactor
        self._client = client
        self._internal_listeners = {}

    def add_listener(self, listener):
        """Add the given listener to the wrapped client.

        The listener will be wrapped, so that it will be called in the reactor
        thread. This way, it can safely use Twisted APIs.
        """
        internal_listener = partial(self._call_in_reactor_thread, listener)
        self._internal_listeners[listener] = internal_listener
        return self._client.add_listener(internal_listener)

    def remove_listener(self, listener):
        """Remove the given listener from the wrapped client.

        :param listener: A listener previously passed to :meth:`add_listener`.
        """
        internal_listener = self._internal_listeners.pop(listener)
        return self._client.remove_listener(internal_listener)

    def _wrapped_method_with_watch_fn(self, f, *args, **kwargs):
        """A wrapped method with a watch function.

        When this method is called, it will call the underlying method with
        the same arguments, *except* that if the ``watch`` argument isn't
        :data:`None`, it will be replaced with a wrapper around that watch
        function, so that the watch function will be called in the reactor
        thread. This means that the watch function can safely use Twisted
        APIs.
        """
        bound_args = signature(f).bind(*args, **kwargs)
        orig_watch = bound_args.arguments.get("watch")

        if orig_watch is not None:
            wrapped_watch = partial(self._call_in_reactor_thread, orig_watch)
            wrapped_watch = wraps(orig_watch)(wrapped_watch)
            bound_args.arguments["watch"] = wrapped_watch

        return f(**bound_args.arguments)

    def _call_in_reactor_thread(self, f, *args, **kwargs):
        """Call the given function with args in the reactor thread."""
        self._reactor.callFromThread(f, *args, **kwargs)

    def __getattr__(self, attr):
        """Get an attr from the client, and maybe wrap its watch function.

        First, get the attribute with the given name from the
        underlying client. If the attribute is a special method with a
        watch function, wrap that watch function appropriately.

        :param str attr: The attribute name.
        :return: The attribute value, possibly wrapped.
        """
        value = getattr(self._client, attr)
        if attr in self._methods_with_watch_fn:
            value = partial(self._wrapped_method_with_watch_fn, value)
        return value


_blocking_client_methods = ("get", "get_children", "set", "delete",
                            "start", "stop", "restart", "close",
                            "command", "add_auth", "unchroot",
                            "sync", "create", "ensure_path", "exists",
                            "get_acls", "set_acls", "transaction")
_blocking_lock_methods = "acquire", "cancel", "contenders", "release"


def TxKazooClient(reactor, pool, client):
    """Create a client for txkazoo.

    :param twisted.internet.interfaces.IReactorThreads reactor: The reactor
        used to interact with the thread pool.
    :param ThreadPool pool: The thread pool to which blocking calls will be
        deferred.
    :param kazoo.client.KazooClient client: The blocking Kazoo client, whose
        blocking methods will be deferred to the thread pool.
    :return: An object with a similar interface to the Kazoo client, but
        returning deferreds for all blocking methods. The actual method calls
        will be executed in the thread pool.
    """
    make_thimble = partial(Thimble, reactor, pool)

    wrapper = _RunCallbacksInReactorThreadWrapper(reactor, client)
    client_thimble = make_thimble(wrapper, _blocking_client_methods)

    def _Lock(path, identifier=None):
        """Return a wrapped :class:`kazoo.recipe.lock.Lock` for this client."""
        lock = client.Lock(path, identifier)
        return Thimble(reactor, pool, lock, _blocking_lock_methods)

    client_thimble.Lock = _Lock
    client_thimble.SetPartitioner = partial(_SetPartitionerWrapper,
                                            reactor, pool, client)

    # Expose these so e.g. recipes can access them from the kzclient
    client.reactor = reactor
    client.pool = pool
    client.kazoo_client = client

    return client_thimble
