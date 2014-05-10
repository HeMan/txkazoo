import txkazoo

from twisted.trial.unittest import SynchronousTestCase


class VersionTests(SynchronousTestCase):
    """
    Tests for programmatically acquiring the version of txkazoo.
    """
    def test_both_names(self):
        """
        The version is programmatically avaialble on the ``txkazoo``
        module as ``__version__`` and ``version``. They are the same
        object.
        """
        self.assertIdentical(txkazoo.__version__, txkazoo.version)
