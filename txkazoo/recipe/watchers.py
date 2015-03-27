# Copyright 2015 Rackspace, Inc.
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

"""The txkazoo equivalent of ``kazoo.recipe.watchers``."""

from twisted.internet.threads import blockingCallFromThread, deferToThreadPool

from kazoo.recipe.watchers import ChildrenWatch


def watch_children(kzclient,
                   path, func, allow_session_lost=True, send_event=False,
                   ChildrenWatch=ChildrenWatch):
    """
    Install a Kazoo :obj:`ChildrenWatch` on the given path.

    The given `func` will be called in the reactor thread when any children are
    created or deleted, or if the node itself is deleted.

    Returns a Deferred which usually has no result, but may fail with an
    exception if e.g. the path does not exist.
    """

    def wrapped_func(*args, **kwargs):
        return blockingCallFromThread(kzclient.reactor, func, *args, **kwargs)

    return deferToThreadPool(
        kzclient.reactor,
        kzclient.pool,
        lambda: ChildrenWatch(
            kzclient.kazoo_client,
            path,
            func=wrapped_func,
            allow_session_lost=allow_session_lost,
            send_event=send_event))
