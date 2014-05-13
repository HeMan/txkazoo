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
from twisted.internet import threads


class SetPartitioner(object):

    """
    Twisted-friendly wrapper for ``kazoo.recipe.partitioner.SetPartitioner``.
    """

    def __init__(self, client, path, set, **kwargs):
        """Initializes SetPartitioner. After `client`, takes same arguments as
        `kazoo.recipe.partitioner.SetPartitioner.__init__`

        :param client: `kazoo.client.KazooClient` object

        """
        self._partitioner = None
        self._state = PartitionState.ALLOCATING
        d = threads.deferToThread(client.SetPartitioner, path, set, **kwargs)
        d.addCallback(self._initialized)
        d.addErrback(self._errored)

    def _initialized(self, partitioner):
        # Now that we successfully got actual SetPartitioner object, we store it and
        # reset our internal self._state to delegate state to this new object
        self._partitioner = partitioner
        self._state = None

    def _errored(self, failure):
        # SetPartioner was not created due to session expiry or network error
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
        return lambda *args, **kwargs: threads.deferToThread(getattr(self._partitioner, name), *args, **kwargs)

    def __iter__(self):
        for elem in self._partitioner:
            yield elem
