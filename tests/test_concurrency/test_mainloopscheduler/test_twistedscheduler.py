import pytest

from datetime import datetime, timedelta
from threading import current_thread
from time import sleep

from rx.concurrency.mainloopscheduler import TwistedScheduler
from rx.disposable import SingleAssignmentDisposable
from rx.testing import SchedulerHistory

twisted = pytest.importorskip('twisted')
skip = not twisted
if not skip:
    try:
        from twisted.internet import reactor, defer
        from twisted.trial import unittest
    except ImportError:
        skip = True


delay = 0.05
grace = 0.10
repeat = 3
timeout_single = delay + grace
timeout_period = repeat * delay + grace
state = 0xdeadbeef


class Event(defer.Deferred):
    def set(self):
        self.callback(None)


def wait(event, timeout):
    event.addTimeout(timeout, reactor)
    try:
        yield event
    except defer.TimeoutError:
        pass


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
        event = Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        scheduler.schedule(action, state=state)

        yield from wait(event, grace)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2

    @defer.inlineCallbacks
    def test_twisted_schedule_relative(self):
        scheduler = TwistedScheduler(reactor)
        event = Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        scheduler.schedule_relative(delay, action, state=state)

        yield from wait(event, timeout_single)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    @defer.inlineCallbacks
    def test_twisted_schedule_relative_dispose(self):
        scheduler = TwistedScheduler(reactor)
        event = Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        disposable = scheduler.schedule_relative(delay, action, state=state)
        disposable.dispose()

        yield from wait(event, timeout_single)

        assert len(history) == 0

    @defer.inlineCallbacks
    def test_twisted_schedule_absolute(self):
        scheduler = TwistedScheduler(reactor)
        event = Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        duetime = scheduler.now + timedelta(seconds=delay)
        scheduler.schedule_absolute(duetime, action, state=state)

        yield from wait(event, timeout_single)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    @defer.inlineCallbacks
    def test_twisted_schedule_absolute_dispose(self):
        scheduler = TwistedScheduler(reactor)
        event = Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        duetime = scheduler.now + timedelta(seconds=delay)
        disposable = scheduler.schedule_absolute(duetime, action, state=state)
        disposable.dispose()

        yield from wait(event, timeout_single)

        assert len(history) == 0

    @defer.inlineCallbacks
    def test_twisted_schedule_periodic(self):
        scheduler = TwistedScheduler(reactor)
        event = Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        disposable = scheduler.schedule_periodic(delay, action, state=0)

        yield from wait(event, timeout_period)

        disposable.dispose()  # Otherwise reactor complains about being unclean

        assert len(history) == repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    @defer.inlineCallbacks
    def test_twisted_schedule_periodic_dispose(self):
        scheduler = TwistedScheduler(reactor)
        event = Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        disposable = scheduler.schedule_periodic(delay, action, state=0)

        reactor.callLater(timeout_period / 2, disposable.dispose)

        yield from wait(event, timeout_period)

        assert 0 < len(history) < repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    @defer.inlineCallbacks
    def test_twisted_schedule_periodic_zero(self):
        scheduler = TwistedScheduler(reactor)
        event = Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        scheduler.schedule_periodic(0.0, action, state=0)

        yield from wait(event, grace)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is None
        assert call.state == 0
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2
