import pytest
import unittest

from datetime import datetime, timedelta
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
        tag = None

        def action(scheduler, state):
            nonlocal time2, tag
            time2 = scheduler.now
            if tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None

        scheduler.schedule(action)

        tag = loop.call_later(0.1, loop.stop)
        loop.start()

        assert tag is None

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_ioloop_schedule_relative(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        time1 = scheduler.now
        time2 = None
        tag = None

        def action(scheduler, state):
            nonlocal time2, tag
            time2 = scheduler.now
            if tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None

        scheduler.schedule_relative(0.1, action)

        tag = loop.call_later(0.3, loop.stop)
        loop.start()

        assert tag is None

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_ioloop_schedule_relative_cancel(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        ran = False
        tag = None

        def action(scheduler, state):
            nonlocal ran, tag
            ran = True
            if tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        tag = loop.call_later(0.3, loop.stop)
        loop.start()

        assert tag is not None

        assert ran is False

    def test_ioloop_schedule_absolute(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        time1 = scheduler.now
        time2 = None
        tag = None

        def action(scheduler, state):
            nonlocal time2, tag
            time2 = scheduler.now
            if tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        tag = loop.call_later(0.3, loop.stop)
        loop.start()

        assert tag is None

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_ioloop_schedule_absolute_cancel(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        ran = False
        tag = None

        def action(scheduler, state):
            nonlocal ran, tag
            ran = True
            if tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        tag = loop.call_later(0.3, loop.stop)
        loop.start()

        assert tag is not None

        assert ran is False

    def test_ioloop_schedule_periodic(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        times = [scheduler.now]
        repeat = 3
        tag = None

        def action(state):
            nonlocal tag
            if state:
                times.append(scheduler.now)
                state -= 1
            elif tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None
            return state

        scheduler.schedule_periodic(0.1, action, state=repeat)

        tag = loop.call_later(0.6, loop.stop)
        loop.start()

        assert tag is None

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_ioloop_schedule_periodic_cancel(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        times = [scheduler.now]
        repeat = 3
        tag = None

        def action(state):
            nonlocal tag
            if state:
                times.append(scheduler.now)
                state -= 1
            elif tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None
            return state

        disp = scheduler.schedule_periodic(0.1, action, state=repeat)

        loop.call_later(0.15, disp.dispose)
        tag = loop.call_later(0.30, loop.stop)
        loop.start()

        assert tag is not None

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_ioloop_schedule_periodic_zero(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler()
        times = [scheduler.now]
        repeat = 3
        tag = None

        def action(state):
            nonlocal tag
            if state:
                times.append(scheduler.now)
                state -= 1
            elif tag is not None:
                loop.stop()
                loop.remove_timeout(tag)
                tag = None
            return state

        scheduler.schedule_periodic(0.0, action, state=repeat)

        tag = loop.call_later(0.2, loop.stop)
        loop.start()

        assert tag is not None

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
