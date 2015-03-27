"""Tests for txkazoo.recipe.watchers."""

from twisted.trial.unittest import SynchronousTestCase

from txkazoo.client import TxKazooClient
from txkazoo.recipe.watchers import watch_children
from txkazoo.test.util import FakeReactor, FakeThreadPool, FakeKazooClient


def FakeChildrenWatch(client, path, func=None, allow_session_lost=True,
                      send_event=False):
    return (client, path, func, allow_session_lost, send_event)


class WatchChildrenTests(SynchronousTestCase):
    """Tests for :func:`watch_children`."""
    def setUp(self):
        self.reactor = FakeReactor()
        self.pool = FakeThreadPool()
        self.client = FakeKazooClient()
        self.tx_client = TxKazooClient(self.reactor, self.pool, self.client)

    def _my_callback(self, children):
        return ('called back', children)

    def test_basic_watch(self):
        """
        The callback is invoked and its result is propagated to the
        :obj:`ChildrenWatch`.
        """
        result = watch_children(self.tx_client, '/foo', self._my_callback,
                                ChildrenWatch=FakeChildrenWatch)
        result = self.successResultOf(result)
        self.assertEqual(result[0], self.tx_client.kazoo_client)
        self.assertEqual(result[1], '/foo')
        self.assertEqual(result[3], True)
        self.assertEqual(result[4], False)

        self.assertEqual(
            result[2](['foo', 'bar']),
            ('called back', ['foo', 'bar']))

    def test_kwargs(self):
        """
        ``allow_session_lost`` and ``send_event`` are passed through to the
        ChildrenWatch.
        """
        result = watch_children(self.tx_client, '/foo', None,
                                allow_session_lost=False,
                                send_event=True,
                                ChildrenWatch=FakeChildrenWatch)
        result = self.successResultOf(result)
        self.assertEqual(result[0], self.tx_client.kazoo_client)
        self.assertEqual(result[1], '/foo')
        self.assertEqual(result[3], False)
        self.assertEqual(result[4], True)
