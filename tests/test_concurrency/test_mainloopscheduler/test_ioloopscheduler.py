import pytest
import unittest

from datetime import datetime
from time import sleep

from rx.concurrency.mainloopscheduler import IOLoopScheduler


tornado = pytest.importorskip('tornado')
skip = not tornado
if not skip:
    try:
        from tornado import ioloop
    except ImportError:
        skip = True


@pytest.mark.skipif('skip == True')
class TestIOLoopScheduler(unittest.TestCase):

    def test_ioloop_now(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = datetime.utcfromtimestamp(loop.time())
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_ioloop_now_units(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_ioloop_schedule(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule(action)

        loop.call_later(0.1, loop.stop)
        loop.start()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_ioloop_schedule_relative(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule_relative(0.1, action)

        loop.call_later(0.3, loop.stop)
        loop.start()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_ioloop_schedule_relative_cancel(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        loop.call_later(0.3, loop.stop)
        loop.start()

        assert ran is False
