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

"""Tests for the txkazoo equivalent of ``kazoo.recipe.partitioner``."""
import mock

from kazoo.recipe.partitioner import PartitionState
from twisted.internet import defer
from txkazoo.test.util import TxKazooTestCase


class SetPartitionerTests(TxKazooTestCase):

    """Tests for txkazoo's ``SetPartitioner``."""

    def test_init(self):
        """Init calls actual SetPartitioner in seperate thread."""
        kz_part = mock.Mock()
        self.defer_to_thread.return_value = defer.succeed(kz_part)
        part = self.txkzclient.SetPartitioner(
            '/path', set(range(2, 5)), time_boundary=20)
        self.defer_to_thread.assert_called_with(
            self.kz_obj.SetPartitioner,
            '/path', set(range(2, 5)), time_boundary=20)
        self.assertEqual(part._partitioner, kz_part)
        self.assertIsNone(part._state)

    def test_state_before_object(self):
        """.state returns ALLOCATING before SetPartitioner object is
        created."""
        self.defer_to_thread.return_value = defer.Deferred()
        partitioner = self.txkzclient.SetPartitioner(
            '/path', set(range(1, 10)))
        self.assertEqual(partitioner.state, PartitionState.ALLOCATING)

    def test_state_after_object(self):
        """.state returns SetPartitioner.state after object is created."""
        kz_part = mock.Mock(state='allocated')
        self.defer_to_thread.return_value = defer.succeed(kz_part)
        partitioner = self.txkzclient.SetPartitioner(
            '/path', set(range(1, 10)))
        self.assertEqual(partitioner.state, 'allocated')

    def test_state_on_error(self):
        """.state returns FAILURE if SetPartitioner creation errored."""
        self.defer_to_thread.return_value = defer.fail(ValueError(2))
        partitioner = self.txkzclient.SetPartitioner(
            '/path', set(range(1, 10)))
        self.assertEqual(partitioner.state, PartitionState.FAILURE)

    def test_state_based_properties(self):
        """accessing other state based properties return True or False
        depending on current value of `state`"""
        attrs = {
            'failed': PartitionState.FAILURE,
            'release': PartitionState.RELEASE,
            'acquired': PartitionState.ACQUIRED,
            'allocating': PartitionState.ALLOCATING
        }
        partitioner = self.txkzclient.SetPartitioner(
            '/path', set(range(1, 10)))
        for attr, value in attrs.items():
            partitioner._state = value
            self.assertTrue(getattr(partitioner, attr))
            partitioner._state = 'else'
            self.assertFalse(getattr(partitioner, attr))

    def test_method_invocation(self):
        """Methods are invoked in seperate thread."""
        # First deferToThread to create SetPartitioner
        part_obj = mock.Mock()
        self.defer_to_thread.return_value = defer.succeed(part_obj)
        partitioner = self.txkzclient.SetPartitioner(
            '/path', set(range(1, 10)))
        # Second deferToThread to call wait_for_acquire
        self.defer_to_thread.return_value = defer.succeed(3)
        d = partitioner.wait_for_acquire(timeout=40)
        self.assertEqual(self.successResultOf(d), 3)
        self.defer_to_thread.assert_called_with(
            part_obj.wait_for_acquire, timeout=40)

    def test_iter(self):
        """__iter__() delegates to actual SetPartitioner.__iter__"""
        part_obj = mock.MagicMock()
        part_obj.__iter__.return_value = [2, 3]
        self.defer_to_thread.return_value = defer.succeed(part_obj)
        partitioner = self.txkzclient.SetPartitioner(
            '/path', set(range(1, 10)))
        self.assertEqual(list(partitioner), [2, 3])
