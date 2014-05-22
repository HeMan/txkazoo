# txkazoo

[![Build Status](https://travis-ci.org/rackerlabs/txkazoo.svg)](https://travis-ci.org/rackerlabs/txkazoo)
[![Coverage Status](https://coveralls.io/repos/rackerlabs/txkazoo/badge.png)](https://coveralls.io/r/rackerlabs/txkazoo)

Twisted wrapper for [Kazoo](https://github.com/python-zk/kazoo/).

## Status

This is not yet meant for public use.

## Running tests

Use [`tox`](http://tox.testrun.org/). By default, this will run the
entire test suite plus a number of sanity checks. Any additional
positional arguments passed to `tox` will be passed to `trial`,  the
test runner. You can use this to only test part of the test suite, for
example:

```
tox -- txkazoo.test.test_something
```
