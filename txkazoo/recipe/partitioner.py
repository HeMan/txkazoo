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

from functools import partial
from kazoo.recipe.partitioner import PartitionState
from twisted.internet import threads


class SetPartitioner(object):

    """Wrapper for :class:`kazoo.recipe.partitioner.SetPartitioner`.

    This is a Twisted-friendly wrapped based on a thread pool.
    """

    def __init__(self, client, path, set, **kwargs):
        """Initialize SetPartitioner.

        After the ``client`` argument, takes same arguments as
        :class:`kazoo.recipe.partitioner.SetPartitioner.__init__`.

        :param client: blocking kazoo client
        :type client: :class:`kazoo.client.KazooClient`

        """
        self._partitioner = None
        self._state = PartitionState.ALLOCATING
        d = threads.deferToThread(client.SetPartitioner, path, set, **kwargs)
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
        return self._state or self._partitioner.state

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
        """Get a method of the partitioner and wraps with a thread pool."""
        blocking_method = getattr(self._partitioner, name)
        return partial(threads.deferToThread, blocking_method)

    def __iter__(self):
        """Iterate over the wrapped partitioner."""
        return iter(self._partitioner)
