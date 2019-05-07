import pytest
import unittest

from datetime import datetime

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
