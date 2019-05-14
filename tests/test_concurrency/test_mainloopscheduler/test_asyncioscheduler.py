import unittest

import asyncio
from datetime import datetime, timedelta
from sys import version_info
from threading import current_thread, Thread

from rx.concurrency.mainloopscheduler import AsyncIOScheduler
from rx.disposable import SingleAssignmentDisposable
from rx.testing import SchedulerHistory


if version_info < (3, 8, 0):
    from asyncio.futures import TimeoutError
else:
    from asyncio.exceptions import TimeoutError


delay = 0.05
grace = 0.10
repeat = 3
timeout_single = delay + grace
timeout_period = repeat * delay + grace
state = 0xdeadbeef


async def wait(loop, event, timeout):
    try:
        await asyncio.wait_for(event.wait(), timeout, loop=loop)
    except TimeoutError:
        pass


class TestAsyncIOScheduler(unittest.TestCase):

    def test_asyncio_now(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop)

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = datetime.utcfromtimestamp(loop.time())
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_asyncio_now_units(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop)
        time1 = scheduler.now

        yield from asyncio.sleep(0.1, loop=loop)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_asyncio_schedule(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            scheduler.schedule(action, state=state)

            yield from wait(loop, event, grace)

        loop.run_until_complete(go())

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2

    def test_asyncio_schedule_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            def schedule():
                scheduler.schedule(action, state=state)
            Thread(target=schedule).start()

            yield from wait(loop, event, grace)

        loop.run_until_complete(go())

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2

    def test_asyncio_schedule_relative(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            scheduler.schedule_relative(delay, action, state=state)

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_relative_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            def schedule():
                scheduler.schedule_relative(delay, action, state=state)
            Thread(target=schedule).start()

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_relative_dispose(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            disposable = scheduler.schedule_relative(delay, action, state=state)
            disposable.dispose()

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 0

    def test_asyncio_schedule_relative_dispose_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            def schedule():
                disposable = scheduler.schedule_relative(delay, action, state=state)
                disposable.dispose()
            Thread(target=schedule).start()

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 0

    def test_asyncio_schedule_absolute(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            duetime = scheduler.now + timedelta(seconds=delay)
            scheduler.schedule_absolute(duetime, action, state=state)

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_absolute_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            def schedule():
                duetime = scheduler.now + timedelta(seconds=delay)
                scheduler.schedule_absolute(duetime, action, state=state)
            Thread(target=schedule).start()

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is scheduler
        assert call.state == state
        assert call.thread_id == thread_id
        assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_absolute_dispose(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            duetime = scheduler.now + timedelta(seconds=delay)
            disposable = scheduler.schedule_absolute(duetime, action, state=state)
            disposable.dispose()

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 0

    def test_asyncio_schedule_absolute_dispose_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)

        history = SchedulerHistory(scheduler)
        action = history.make_action(event.set)

        @asyncio.coroutine
        def go():
            def schedule():
                duetime = scheduler.now + timedelta(seconds=delay)
                disposable = scheduler.schedule_absolute(duetime, action, state=state)
                disposable.dispose()
            Thread(target=schedule).start()

            yield from wait(loop, event, timeout_single)

        loop.run_until_complete(go())

        assert len(history) == 0

    def test_asyncio_schedule_periodic(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        @asyncio.coroutine
        def go():
            scheduler.schedule_periodic(delay, action, state=0)
            yield from wait(loop, event, timeout_period)

        loop.run_until_complete(go())

        assert len(history) == repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_periodic_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        @asyncio.coroutine
        def go():
            def schedule():
                scheduler.schedule_periodic(delay, action, state=0)
            Thread(target=schedule).start()

            yield from wait(loop, event, timeout_period)

        loop.run_until_complete(go())

        assert len(history) == repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_periodic_dispose(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        @asyncio.coroutine
        def go():
            disposable = scheduler.schedule_periodic(delay, action, state=0)
            yield from asyncio.sleep(timeout_period / 2, loop=loop)

            disposable.dispose()
            yield from wait(loop, event, timeout_period / 2)

        loop.run_until_complete(go())

        assert 0 < len(history) < repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_periodic_dispose_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        @asyncio.coroutine
        def go():
            sad = SingleAssignmentDisposable()
            
            def schedule():
                sad.disposable = scheduler.schedule_periodic(delay, action, state=0)
            Thread(target=schedule).start()

            yield from asyncio.sleep(timeout_period / 2, loop=loop)

            def dispose():
                sad.dispose()
            Thread(target=dispose).start()

            sad.dispose()

            yield from wait(loop, event, timeout_period / 2)

        loop.run_until_complete(go())

        assert 0 < len(history) < repeat
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert delay / 2 <= call.time_diff <= 2 * delay

    def test_asyncio_schedule_periodic_zero(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=False)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        @asyncio.coroutine
        def go():
            scheduler.schedule_periodic(0.0, action, state=0)
            yield from wait(loop, event, grace)

        loop.run_until_complete(go())

        assert len(history) == 1
        for i, call in enumerate(history):
            assert call.scheduler is None
            assert call.state == i
            assert call.thread_id == thread_id
            assert call.time_diff <= grace / 2

    def test_asyncio_schedule_periodic_zero_threadsafe(self):
        loop = asyncio.get_event_loop()
        scheduler = AsyncIOScheduler(loop, threadsafe=True)
        event = asyncio.Event(loop=loop)
        thread_id = current_thread().ident

        history = SchedulerHistory(scheduler)
        action = history.make_periodic(repeat, event.set, lambda s: s + 1)

        @asyncio.coroutine
        def go():
            def schedule():
                scheduler.schedule_periodic(0.0, action, state=0)
            Thread(target=schedule).start()

            yield from wait(loop, event, grace)

        loop.run_until_complete(go())

        assert len(history) == 1
        call = history[0]
        assert call.scheduler is None
        assert call.state == 0
        assert call.thread_id == thread_id
        assert call.time_diff <= grace / 2
