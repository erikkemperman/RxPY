import pytest
import unittest

from datetime import datetime, timedelta

from rx.concurrency.mainloopscheduler import GEventScheduler

gevent = pytest.importorskip('gevent')
skip = not gevent
if not skip:
    try:
        import gevent.event
    except ImportError:
        skip = True


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
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule(action)

        event.wait(0.1)

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_gevent_schedule_relative(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule_relative(0.1, action)

        event.wait(0.3)

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gevent_schedule_relative_cancel(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        event.wait(0.3)

        assert event.is_set() is False

        assert ran is False

    def test_gevent_schedule_absolute(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        event.wait(0.3)

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gevent_schedule_absolute_cancel(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        event.wait(0.3)

        assert event.is_set() is False

        assert ran is False

    def test_eventlet_schedule_periodic(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            else:
                event.set()
            return state

        scheduler.schedule_periodic(0.1, action, state=repeat)

        event.wait(0.6)

        assert event.is_set() is True

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_eventlet_schedule_periodic_cancel(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            else:
                event.set()
            return state

        disp = scheduler.schedule_periodic(0.1, action, state=repeat)

        gevent.sleep(0.15)

        disp.dispose()

        event.wait(0.15)

        assert event.is_set() is False

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_eventlet_schedule_periodic_zero(self):
        scheduler = GEventScheduler()
        event = gevent.event.Event()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            else:
                event.set()
            return state

        scheduler.schedule_periodic(0.0, action, state=repeat)

        event.wait(0.2)

        assert event.is_set() is False

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
