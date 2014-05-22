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

"""The txkazoo equivalent of ``kazoo.recipe.lock``."""

from functools import partial
from twisted.internet import threads


class Lock(object):

    """
    Twisted-friendly wrapper for :class:`kazoo.recipe.lock.Lock`.

    All blocking methods of that class are delegated to a thread pool.
    """

    def __init__(self, lock):
        """Initialize the txkazoo Lock.

        :param lock: the blocking kazoo lock being wrapped.
        :type lock: :class:`kazoo.recipe.lock.Lock`

        """
        self._lock = lock

    def __getattr__(self, name):
        """Get a method of the lock and wraps with a thread pool."""
        blocking_method = getattr(self._lock, name)
        return partial(threads.deferToThread, blocking_method)
