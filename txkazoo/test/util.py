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

from kazoo.recipe.partitioner import PartitionState
from twisted.internet.interfaces import IReactorThreads
from twisted.python.context import ContextTracker
from twisted.python.failure import Failure
from zope.interface import implementer


class FakeKazooClient(object):

    """A fake Kazoo client for testing."""

    def __init__(self):
        """Initialize a fake Kazoo client for testing."""
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
        """Store the watch function."""
        self.watch = watch

    def Lock(self, *args, **kwargs):
        """Build a fake lock, for testing."""
        return FakeLock(*args, **kwargs)

    def SetPartitioner(self, *args, **kwargs):
        """Build a fake set partitioner, for testing."""
        return FakeSetPartitioner(*args, **kwargs)


class FakeLock(object):

    """A fake Lock for testing."""

    def __init__(self, path, identifier=None):
        """Initialize fake Lock for testing."""
        self.path = path
        self.identifier = identifier


class FakeSetPartitioner(object):

    """A fake SetPartitioner for testing."""

    def __init__(self, *args, **kwargs):
        """Initialize a fake SetPartitioner for testing."""
        self.state = PartitionState.ALLOCATING
        self.args, self.kwargs = args, kwargs

    def __iter__(self):
        """Just yield 1."""
        yield 1


class FakeThreadPool(object):

    """A fake thread pool, for testing.

    It actually just runs things synchronously in the calling thread.
    """

    def callInThread(self, func, *args, **kw):
        """Call ``func`` with given arguments in the calling thread."""
        return func(*args, **kw)

    def callInThreadWithCallback(self, onResult, func, *args, **kw):
        """
        Call ``func`` with given arguments in the calling thread.

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

    """A fake threaded reactor, for testing."""

    def __init__(self):
        self.context = ContextTracker()

    def getThreadPool(self):
        """Return a new :class:`FakeThreadPool`."""
        return FakeThreadPool()

    def callInThread(self, f, *args, **kwargs):
        """Just call the function with the arguments."""
        self.context.callWithContext(
            {'virtual-thread': 'non-reactor'},
            f, *args, **kwargs)

    def callFromThread(self, f, *args, **kw):
        """Just call the function with the arguments."""
        self.context.callWithContext(
            {'virtual-thread': 'reactor'},
            f, *args, **kw)
