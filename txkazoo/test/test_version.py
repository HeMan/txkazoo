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

import txkazoo

from twisted.trial.unittest import SynchronousTestCase


class VersionTests(SynchronousTestCase):

    """Tests for programmatically acquiring the version of txkazoo."""

    def test_both_names(self):
        """The version is programmatically avaialble on the ``txkazoo`` module
        as ``__version__`` and ``version``.

        They are the same object.

        """
        self.assertIdentical(txkazoo.__version__, txkazoo.version)
