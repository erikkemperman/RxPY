import unittest

import asyncio
import threading
from datetime import datetime

from rx.concurrency.mainloopscheduler import AsyncIOScheduler


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

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now

            scheduler.schedule(action)

            yield from asyncio.sleep(0.1, loop=loop)

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now

            def schedule():
                scheduler.schedule(action)

            threading.Thread(target=schedule).start()

            yield from asyncio.sleep(0.1, loop=loop)

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now

            scheduler.schedule_relative(0.1, action)

            yield from asyncio.sleep(0.3, loop=loop)

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now

            def schedule():
                scheduler.schedule_relative(0.1, action)

            threading.Thread(target=schedule).start()

            yield from asyncio.sleep(0.3, loop=loop)

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_cancel(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop)
            ran = False

            def action(scheduler, state):
                nonlocal ran
                ran = True

            disp = scheduler.schedule_relative(0.1, action)
            disp.dispose()

            yield from asyncio.sleep(0.3, loop=loop)

            assert ran is False

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_cancel_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            time = None

            def action(scheduler, state):
                nonlocal time
                time = scheduler.now

            def schedule():
                disp = scheduler.schedule_relative(0.1, action)
                disp.dispose()

            threading.Thread(target=schedule).start()

            yield from asyncio.sleep(0.3, loop=loop)

            assert time is None

        loop.run_until_complete(go())
