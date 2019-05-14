import pytest
import unittest

from datetime import datetime, timedelta
from threading import current_thread
from time import sleep

from rx.concurrency.mainloopscheduler import IOLoopScheduler
from rx.testing import SchedulerHistory

tornado = pytest.importorskip('tornado')
skip = not tornado
if not skip:
    try:
        from tornado import ioloop
        from tornado import locks
        from tornado import util
    except ImportError:
        skip = True


delay = 0.05
grace = 0.10
repeat = 3
timeout_single = delay + grace
timeout_period = repeat * delay + grace
state = 0xdeadbeef


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
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        scheduler.schedule(action, state=state)

        loop.call_later(0.0, wait, loop, event, grace)
        loop.start()

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2

    def test_ioloop_schedule_relative(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        scheduler.schedule_relative(delay, action, state=state)

        loop.call_later(0.0, wait, loop, event, timeout_single)
        loop.start()

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_ioloop_schedule_relative_dispose(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        disposable = scheduler.schedule_relative(delay, action, state=state)
        disposable.dispose()

        loop.call_later(0.0, wait, loop, event, timeout_single)
        loop.start()

        assert len(history) == 0

    def test_ioloop_schedule_absolute(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        duetime = scheduler.now + timedelta(seconds=delay)
        scheduler.schedule_absolute(duetime, action, state=state)

        loop.call_later(0.0, wait, loop, event, timeout_single)
        loop.start()

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_ioloop_schedule_absolute_dispose(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        duetime = scheduler.now + timedelta(seconds=delay)
        disposable = scheduler.schedule_absolute(duetime, action, state=state)
        disposable.dispose()

        loop.call_later(0.0, wait, loop, event, timeout_single)
        loop.start()

        assert len(history) == 0

    def test_ioloop_schedule_periodic(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        scheduler.schedule_periodic(delay, action, state=0)

        loop.call_later(0.0, wait, loop, event, timeout_period)
        loop.start()

        assert len(history) == repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_ioloop_schedule_periodic_dispose(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        disposable = scheduler.schedule_periodic(delay, action, state=0)

        loop.call_later(timeout_period / 2, disposable.dispose)
        loop.call_later(0.0, wait, loop, event, timeout_period )
        loop.start()

        assert 0 < len(history) < repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_ioloop_schedule_periodic_zero(self):
        loop = ioloop.IOLoop.instance()
        scheduler = IOLoopScheduler(loop)
        event = locks.Event()
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        scheduler.schedule_periodic(0.0, action, state=0)

        loop.call_later(0.0, wait, loop, event, grace)
        loop.start()

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is None
        assert call.state == 0
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2
