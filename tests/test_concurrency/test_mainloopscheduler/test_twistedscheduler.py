import pytest

from datetime import datetime
from time import sleep

from rx.concurrency.mainloopscheduler import TwistedScheduler


twisted = pytest.importorskip('twisted')
skip = not twisted
if not skip:
    try:
        from twisted.internet import reactor, defer
        from twisted.trial import unittest
    except ImportError:
        skip = True


@pytest.mark.skipif('skip == True')
class TestTwistedScheduler(unittest.TestCase):

    def test_twisted_now(self):
        scheduler = TwistedScheduler(reactor)

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = datetime.utcfromtimestamp(float(reactor.seconds()))
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_twisted_now_units(self):
        scheduler = TwistedScheduler(reactor)
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    @defer.inlineCallbacks
    def test_twisted_schedule(self):
        scheduler = TwistedScheduler(reactor)
        promise = defer.Deferred()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule(action)

        def done():
            promise.callback('Done')

        reactor.callLater(0.1, done)

        yield promise

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    @defer.inlineCallbacks
    def test_twisted_schedule_relative(self):
        scheduler = TwistedScheduler(reactor)
        promise = defer.Deferred()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule_relative(0.1, action)

        def done():
            promise.callback('Done')

        reactor.callLater(0.3, done)

        yield promise

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    @defer.inlineCallbacks
    def test_twisted_schedule_relative_cancel(self):
        scheduler = TwistedScheduler(reactor)
        promise = defer.Deferred()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        def done():
            promise.callback('Done')

        reactor.callLater(0.3, done)

        yield promise

        assert ran is False
