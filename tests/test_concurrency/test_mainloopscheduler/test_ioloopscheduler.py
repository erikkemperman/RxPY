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
        from tornado import locks
        from tornado import util
    except ImportError:
        skip = True


async def wait(loop, event, timeout):
    try:
        await event.wait(loop.time() + timeout)
    except util.TimeoutError:
        pass
    loop.stop()


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
        event = locks.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule(action)

        loop.call_later(0.0, wait, loop, event, 0.1)
        loop.start()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_ioloop_schedule_relative(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule_relative(0.1, action)

        loop.call_later(0.0, wait, loop, event, 0.3)
        loop.start()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_ioloop_schedule_relative_cancel(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        loop.call_later(0.0, wait, loop, event, 0.3)
        loop.start()

        assert event.is_set() is False

        assert ran is False

    def test_ioloop_schedule_absolute(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        loop.call_later(0.0, wait, loop, event, 0.3)
        loop.start()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_ioloop_schedule_absolute_cancel(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        loop.call_later(0.0, wait, loop, event, 0.3)
        loop.start()

        assert event.is_set() is False

        assert ran is False

    def test_ioloop_schedule_periodic(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif event.is_set() is False:
                event.set()
            return state

        scheduler.schedule_periodic(0.1, action, state=repeat)

        loop.call_later(0.0, wait, loop, event, 0.6)
        loop.start()

        assert event.is_set() is True

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_ioloop_schedule_periodic_cancel(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif event.is_set() is False:
                event.set()
            return state

        disp = scheduler.schedule_periodic(0.1, action, state=repeat)

        loop.call_later(0.15, disp.dispose)
        loop.call_later(0.15, wait, loop, event, 0.15)
        loop.start()

        assert event.is_set() is False

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_ioloop_schedule_periodic_zero(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler()
        event = locks.Event()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif event.is_set() is False:
                event.set()
            return state

        scheduler.schedule_periodic(0.0, action, state=repeat)

        loop.call_later(0.0, wait, loop, event, 0.2)
        loop.start()

        assert event.is_set() is False

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
