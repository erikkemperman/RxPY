import pytest
import unittest

from datetime import datetime, timedelta
from threading import current_thread
from time import sleep

from rx.concurrency.mainloopscheduler import EventletScheduler
from rx.testing import SchedulerHistory

eventlet = pytest.importorskip('eventlet')
skip = not eventlet
if not skip:
    try:
        import eventlet.event
        import eventlet.hubs
    except ImportError:
        skip = True

delay = 0.05
grace = 0.10
repeat = 3
timeout_single = delay + grace
timeout_period = repeat * delay + grace
state = 0xdeadbeef


@pytest.mark.skipif('skip == True')
class TestEventletScheduler(unittest.TestCase):

    def test_eventlet_now(self):
        scheduler = EventletScheduler()
        hub = eventlet.hubs.get_hub()

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = datetime.utcfromtimestamp(hub.clock())
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_eventlet_now_units(self):
        scheduler = EventletScheduler()
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_eventlet_schedule(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.send)

        scheduler.schedule(action, state=state)

        event.wait(grace)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2

    def test_eventlet_schedule_relative(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.send)

        scheduler.schedule_relative(delay, action, state=state)

        event.wait(timeout_single)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_eventlet_schedule_relative_dispose(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.send)

        disposable = scheduler.schedule_relative(delay, action, state=state)
        disposable.dispose()

        event.wait(timeout_single)

        assert len(history) == 0

    def test_eventlet_schedule_absolute(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.send)

        duetime = scheduler.now + timedelta(seconds=delay)
        scheduler.schedule_absolute(duetime, action, state=state)

        event.wait(timeout_single)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_eventlet_schedule_absolute_dispose(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.send)

        duetime = scheduler.now + timedelta(seconds=delay)
        disposable = scheduler.schedule_absolute(duetime, action, state=state)
        disposable.dispose()

        event.wait(timeout_single)

        assert len(history) == 0

    def test_eventlet_schedule_periodic(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.send, lambda s: s + 1)

        scheduler.schedule_periodic(delay, action, state=0)

        event.wait(timeout_period)

        assert len(history) == repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_eventlet_schedule_periodic_dispose(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.send, lambda s: s + 1)

        disposable = scheduler.schedule_periodic(delay, action, state=0)

        eventlet.sleep(timeout_period / 2)

        disposable.dispose()

        event.wait(timeout_period / 2)

        assert 0 < len(history) < repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_eventlet_schedule_periodic_zero(self):
        scheduler = EventletScheduler()
        event = eventlet.event.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.send, lambda s: s + 1)

        scheduler.schedule_periodic(0.0, action, state=0)

        event.wait(grace)

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is None
        assert call.state == 0
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2
