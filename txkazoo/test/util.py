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

from twisted.internet.interfaces import IReactorThreads
from twisted.python.failure import Failure
from zope.interface import implementer

class FakeKazooClient(object):
    """A fake Kazoo client for testing."""
    def __init__(self):
        self.listeners = []

    def add_listener(self, listener):
        """Add a listener."""
        self.listeners.append(listener)

    def remove_listener(self, listener):
        """Remove the listener."""
        self.listeners.remove(listener)

    def close(self):
        """No-op."""

    def get(self, path, watch=None):
        """Stores the watch function."""
        self.watch = watch


class FakeThreadPool(object):
    """
    A fake thread pool that actually just runs things synchronously in
    the calling thread.
    """
    def callInThread(self, func, *args, **kw):
        """
        Calls ``func`` with given arguments in the calling thread.
        """
        return func(*args, **kw)

    def callInThreadWithCallback(self, onResult, func, *args, **kw):
        """
        Calls ``func`` with given arguments in the calling thread.

        If ``onResult`` is :const:`None`, it is not used. Otherwise,
        it is called with :const:`True` and the result if the call
        succeeded, or :const:`False` and the failure if it failed.
        """
        if onResult is None:
            onResult = lambda success, result: None

        try:
            result = func(*args, **kw)
        except Exception as e:
            onResult(False, Failure(e))
        else:
            onResult(True, result)


@implementer(IReactorThreads)
class FakeReactor(object):
    """
    An IReactorThreads implementation that doesn't actually run
    anything in any other threads.
    """
    def getThreadPool(self):
        """Return a new :class:`FakeThreadPool`."""
        return FakeThreadPool()

    def callInThread(self, f, *args, **kwargs):
        """Just call the function with the arguments."""
        return f(*args, **kwargs)

    def callFromThread(self, f, *args, **kw):
        """Just call the function with the arguments."""
        return f(*args, **kw)
