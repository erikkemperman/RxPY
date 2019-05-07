import pytest
import unittest

from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import EventLetEventScheduler


eventlet = pytest.importorskip('eventlet')
skip = not eventlet
if not skip:
    try:
        import eventlet.hubs
    except ImportError:
        skip = True


@pytest.mark.skipif('skip == True')
class TestEventLetEventScheduler(unittest.TestCase):

    def test_eventlet_now(self):
        scheduler = EventLetEventScheduler()
        hub = eventlet.hubs.get_hub()

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = datetime.utcfromtimestamp(hub.clock())
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_eventlet_now_units(self):
        scheduler = EventLetEventScheduler()
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_eventlet_schedule(self):
        scheduler = EventLetEventScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule(action)

        eventlet.sleep(0.1)

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_eventlet_schedule_relative(self):
        scheduler = EventLetEventScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule_relative(0.1, action)

        eventlet.sleep(0.3)

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_eventlet_schedule_relative_cancel(self):
        scheduler = EventLetEventScheduler()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        eventlet.sleep(0.3)

        assert ran is False

    def test_eventlet_schedule_absolute(self):
        scheduler = EventLetEventScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        eventlet.sleep(0.3)

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_eventlet_schedule_absolute_cancel(self):
        scheduler = EventLetEventScheduler()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        eventlet.sleep(0.3)

        assert ran is False

    def test_eventlet_schedule_periodic(self):
        scheduler = EventLetEventScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(0.1, action, state=repeat)

        eventlet.sleep(0.6)

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_eventlet_schedule_periodic_cancel(self):
        scheduler = EventLetEventScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        disp = scheduler.schedule_periodic(0.1, action, state=repeat)

        eventlet.sleep(0.15)

        disp.dispose()

        eventlet.sleep(0.15)

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_eventlet_schedule_periodic_zero(self):
        scheduler = EventLetEventScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(0.0, action, state=repeat)

        eventlet.sleep(0.2)

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
