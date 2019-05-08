import pytest

from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import TwistedScheduler
from rx.disposable import SingleAssignmentDisposable

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
            promise.callback(True)

        scheduler.schedule(action)

        promise.addTimeout(0.1, reactor)

        try:
            yield promise
        except defer.TimeoutError:
            promise.result = False

        assert promise.result is True

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
            promise.callback(True)

        scheduler.schedule_relative(0.1, action)

        promise.addTimeout(0.3, reactor)

        try:
            yield promise
        except defer.TimeoutError:
            promise.result = False

        assert promise.result is True

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
            promise.callback(True)

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        promise.addTimeout(0.3, reactor)

        try:
            yield promise
        except defer.TimeoutError:
            promise.result = False

        assert promise.result is False

        assert ran is False

    @defer.inlineCallbacks
    def test_twisted_schedule_absolute(self):
        scheduler = TwistedScheduler(reactor)
        promise = defer.Deferred()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            promise.callback(True)

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        promise.addTimeout(0.3, reactor)

        try:
            yield promise
        except defer.TimeoutError:
            promise.result = False

        assert promise.result is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    @defer.inlineCallbacks
    def test_twisted_schedule_absolute_cancel(self):
        scheduler = TwistedScheduler(reactor)
        promise = defer.Deferred()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            promise.callback(True)

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        def done():
            promise.callback('Done')

        promise.addTimeout(0.3, reactor)

        try:
            yield promise
        except defer.TimeoutError:
            promise.result = False

        assert promise.result is False

        assert ran is False

    @defer.inlineCallbacks
    def test_twisted_schedule_periodic(self):
        scheduler = TwistedScheduler(reactor)
        promise = defer.Deferred()
        sad = SingleAssignmentDisposable()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif promise.called is False:
                sad.dispose()
                promise.callback(True)
            return state

        sad.disposable = scheduler.schedule_periodic(0.1, action, state=repeat)

        promise.addTimeout(0.6, reactor)

        try:
            yield promise
        except defer.TimeoutError:
            promise.result = False

        assert promise.result is True

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    @defer.inlineCallbacks
    def test_twisted_schedule_periodic_cancel(self):
        scheduler = TwistedScheduler(reactor)
        promise1 = defer.Deferred()
        promise2 = defer.Deferred()
        sad = SingleAssignmentDisposable()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif promise2.called is False:
                sad.dispose()
                promise2.callback(True)
            return state

        sad.disposable = scheduler.schedule_periodic(0.1, action, state=repeat)

        def dispose():
            sad.dispose()
            promise1.callback(True)

        reactor.callLater(0.15, dispose)

        yield promise1

        assert promise1.result is True

        promise2.addTimeout(0.15, reactor)

        try:
            yield promise2
        except defer.TimeoutError:
            promise2.result = False

        assert promise2.result is False

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    @defer.inlineCallbacks
    def test_twisted_schedule_periodic_zero(self):
        scheduler = TwistedScheduler(reactor)
        promise = defer.Deferred()
        sad = SingleAssignmentDisposable()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif promise.called is False:
                sad.dispose()
                promise.callback(True)
            return state

        sad.disposable = scheduler.schedule_periodic(0.0, action, state=repeat)

        promise.addTimeout(0.6, reactor)

        try:
            yield promise
        except defer.TimeoutError:
            promise.result = False

        assert promise.result is False

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
