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

    """
    Twisted-friendly wrapper for :class`kazoo.recipe.partitioner.SetPartitioner`.
    """

    def __init__(self, client, path, set, **kwargs):
        """Initialize SetPartitioner.

        After the `client` argument, takes same arguments as
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
        """Now that we successfully got an actual
        :class:`kazoo.recipe.partitioner.SetPartitioner` object, we
        store it and reset our internal ``_state`` to ``None``,
        causing the ``state`` property to defer to the partitioner's
        state.

        """
        self._partitioner = partitioner
        self._state = None

    def _errored(self, failure):
        """Couldn't get a :class:`kazoo.recipe.partitioner.SetPartitioner`:
        most likely a session expired or a network error occurred.

        Sets the internal state to ``PartitionState.FAILURE``.

        """
        self._state = PartitionState.FAILURE

    @property
    def state(self):
        # Return our state if we have it, otherwise delegate to
        # self._partitioner
        return self._state or self._partitioner.state

    @property
    def allocating(self):
        return self.state == PartitionState.ALLOCATING

    @property
    def failed(self):
        return self.state == PartitionState.FAILURE

    @property
    def release(self):
        return self.state == PartitionState.RELEASE

    @property
    def acquired(self):
        return self.state == PartitionState.ACQUIRED

    def __getattr__(self, name):
        blocking_method = getattr(self._partitioner, name)
        return partial(threads.deferToThread, blocking_method)

    def __iter__(self):
        return iter(self._partitioner)
