import pytest
import unittest

from datetime import datetime, timedelta

from rx.concurrency.mainloopscheduler import GEventScheduler


gevent = pytest.importorskip('gevent')


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
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule(action)

        gevent.sleep(0.1)

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_gevent_schedule_relative(self):
        scheduler = GEventScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule_relative(0.1, action)

        gevent.sleep(0.3)

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gevent_schedule_relative_cancel(self):
        scheduler = GEventScheduler()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        gevent.sleep(0.3)

        assert ran is False

    def test_gevent_schedule_absolute(self):
        scheduler = GEventScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        gevent.sleep(0.3)

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gevent_schedule_absolute_cancel(self):
        scheduler = GEventScheduler()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        gevent.sleep(0.3)

        assert ran is False

    def test_eventlet_schedule_periodic(self):
        scheduler = GEventScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(0.1, action, state=repeat)

        gevent.sleep(0.6)

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_eventlet_schedule_periodic_cancel(self):
        scheduler = GEventScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        disp = scheduler.schedule_periodic(0.1, action, state=repeat)

        gevent.sleep(0.15)

        disp.dispose()

        gevent.sleep(0.15)

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_eventlet_schedule_periodic_zero(self):
        scheduler = GEventScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(0.0, action, state=repeat)

        gevent.sleep(0.2)

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
