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

"""The txkazoo equivalent of ``kazoo.recipe.partitioner``."""

from kazoo.recipe.partitioner import PartitionState
from thimble import Thimble
from twisted.internet.threads import deferToThreadPool

_blocking_partitioner_methods = "finish", "release_set", "wait_for_acquire"


class _SetPartitionerWrapper(object):

    """Wrapper for :class:`~kazoo.recipe.partitioner.SetPartitioner`.

    This is a Twisted-friendly wrapped based on a thread pool.
    Unfortunately, :class:`~kazoo.recipe.partitioner.SetPartitioner`
    decides to do a bunch of blocking IO when it is initialized, which
    makes this class necessary.
    """

    def __init__(self, reactor, pool, client, path, set, **kwargs):
        """Initialize SetPartitioner.

        :param reactor: The reactor to use when deferring to a thread.
        :param pool: The thread pool to defer execution to.
        :param client: blocking kazoo client
        :type client: :class:`kazoo.client.KazooClient`

        After the ``client`` argument, takes same arguments as
        :class:`kazoo.recipe.partitioner.SetPartitioner.__init__`.
        """
        self.reactor, self.pool = reactor, pool
        self._partitioner = self._thimble = None
        self._state = PartitionState.ALLOCATING
        d = deferToThreadPool(reactor, pool,
                              client.SetPartitioner, path, set, **kwargs)
        d.addCallback(self._initialized)
        d.addErrback(self._errored)

    def _initialized(self, partitioner):
        """Store the partitioner and reset the internal state.

        Now that we successfully got an actual
        :class:`kazoo.recipe.partitioner.SetPartitioner` object, we
        store it and reset our internal ``_state`` to ``None``,
        causing the ``state`` property to defer to the partitioner's
        state.

        """
        self._partitioner = partitioner
        self._thimble = Thimble(self.reactor, self.pool,
                                partitioner, _blocking_partitioner_methods)
        self._state = None

    def _errored(self, failure):
        """Remember that we failed to initialize.

        This means we couldn't get a
        :class:`kazoo.recipe.partitioner.SetPartitioner`: most likely
        a session expired or a network error occurred. The internal
        state is set to ``PartitionState.FAILURE``.
        """
        self._state = PartitionState.FAILURE

    @property
    def state(self):
        """The current state of this partitioner.

        If we are still initializing, or we've failed to initialize,
        this will be this object's internal state. Otherwise, defers
        to the partitioner's state.
        """
        if self._state is not None:
            return self._state
        else:
            return self._partitioner.state

    @property
    def allocating(self):
        """Check if the partitioner is still allocating.

        This means either we're still getting a partitioner, or the
        partitioner itself is still allocating (see
        :py:func:`kazoo.recipe.partitioner.Partitioner.allocating`).

        """
        return self.state == PartitionState.ALLOCATING

    @property
    def failed(self):
        """Check if the partitioner has failed.

        This means we've either failed to get a partitioner, or the
        partitioner itself has failed to partition the set (see
        :py:func:`kazoo.recipe.partitioner.Partitioner.failed`).

        """
        return self.state == PartitionState.FAILURE

    @property
    def release(self):
        """Check if the set needs to be repartitioned.

        See :py:func:`kazoo.recipe.partitioner.Partitioner.released`.
        """
        return self.state == PartitionState.RELEASE

    @property
    def acquired(self):
        """Check if the set partitioning has been acquired.

        See :py:func:`kazoo.recipe.partitioner.Partitioner.acquired`.
        """
        return self.state == PartitionState.ACQUIRED

    def __getattr__(self, name):
        """Get a method of the partitioner through the thimble."""
        return getattr(self._thimble, name)

    def __iter__(self):
        """Iterate over the wrapped partitioner."""
        return iter(self._partitioner)
