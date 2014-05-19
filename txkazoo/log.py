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

"""Bridge between :mod:`twisted.python.log` and :mod:`logging`."""

import logging
from functools import partial


class TxLogger(object):

    """Wraps twisted's log object as a ``logging.Logger``."""

    def __init__(self, log):
        """Initialize a ``TxLogger``.

        :param log: A twisted logger (has ``msg`` and ``err`` methods)

        """
        self._log = log

        self.debug = partial(self._msg, logging.DEBUG)
        self.info = partial(self._msg, logging.INFO)
        self.warning = partial(self._msg, logging.WARNING)
        self.exception = self.error = partial(self._msg, logging.ERROR)
        self.log = self._msg

    def _msg(self, lvl, msg, *args, **kwargs):
        try:
            msg = msg % args
        except TypeError:
            pass
        if lvl <= logging.WARNING:
            self._log.msg(msg, **kwargs)
        else:
            self._log.err(None, msg, **kwargs)
