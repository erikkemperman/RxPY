import pytest
import unittest

from datetime import datetime, timedelta
from threading import current_thread

from rx.concurrency.mainloopscheduler import GEventScheduler
from rx.testing import SchedulerHistory

gevent = pytest.importorskip('gevent')
skip = not gevent
if not skip:
    try:
        import gevent.event
    except ImportError:
        skip = True


delay = 0.05
grace = 0.10
repeat = 3
timeout_single = delay + grace
timeout_period = repeat * delay + grace
state = 0xdeadbeef


class TestGEventScheduler(unittest.TestCase):

    def test_gevent_now(self):
        scheduler = GEventScheduler()
        hub = gevent.get_hub()

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = datetime.utcfromtimestamp(hub.loop.now())
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_gevent_now_units(self):
        scheduler = GEventScheduler()
        time1 = scheduler.now

        gevent.sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gevent_schedule(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        scheduler.schedule(action, state=state)

        event.wait(grace)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2

    def test_gevent_schedule_relative(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        scheduler.schedule_relative(delay, action, state=state)

        event.wait(timeout_single)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_gevent_schedule_relative_dispose(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        disposable = scheduler.schedule_relative(delay, action, state=state)
        disposable.dispose()

        event.wait(timeout_single)

        assert len(history) == 0

    def test_gevent_schedule_absolute(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        duetime = scheduler.now + timedelta(seconds=delay)
        scheduler.schedule_absolute(duetime, action, state=state)

        event.wait(timeout_single)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_gevent_schedule_absolute_dispose(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        duetime = scheduler.now + timedelta(seconds=delay)
        disposable = scheduler.schedule_absolute(duetime, action, state=state)
        disposable.dispose()

        event.wait(timeout_single)

        assert len(history) == 0

    def test_gevent_schedule_periodic(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        scheduler.schedule_periodic(delay, action, state=0)

        event.wait(timeout_period)

        assert len(history) == repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_gevent_schedule_periodic_dispose(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        disposable = scheduler.schedule_periodic(delay, action, state=0)

        gevent.sleep(timeout_period / 2)

        disposable.dispose()

        event.wait(timeout_period / 2)

        assert 0 < len(history) < repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_gevent_schedule_periodic_zero(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        scheduler.schedule_periodic(0.0, action, state=0)

        event.wait(grace)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is None
        assert call.state == 0
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2
