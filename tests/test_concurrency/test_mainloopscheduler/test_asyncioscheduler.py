import unittest

import asyncio
import threading
from datetime import datetime, timedelta

from rx.concurrency.mainloopscheduler import AsyncIOScheduler
from rx.disposable import SingleAssignmentDisposable

from sys import version_info

if version_info < (3, 8, 0):
    from asyncio.futures import TimeoutError
else:
    from asyncio.exceptions import TimeoutError


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
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now
                event.set()

            scheduler.schedule(action)

            try:
                yield from asyncio.wait_for(event.wait(), 0.1, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now
                event.set()

            def schedule():
                scheduler.schedule(action)

            threading.Thread(target=schedule).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.1, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now
                event.set()

            scheduler.schedule_relative(0.1, action)

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now
                event.set()

            def schedule():
                scheduler.schedule_relative(0.1, action)

            threading.Thread(target=schedule).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_cancel(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            ran = False

            def action(scheduler, state):
                nonlocal ran
                ran = True
                event.set()

            disp = scheduler.schedule_relative(0.1, action)
            disp.dispose()

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False
            assert ran is False

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_cancel_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            ran = False

            def action(scheduler, state):
                nonlocal ran
                ran = True
                event.set()

            def schedule():
                disp = scheduler.schedule_relative(0.1, action)
                disp.dispose()

            threading.Thread(target=schedule).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False
            assert ran is False

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now
                event.set()

            duetime = scheduler.now + timedelta(seconds=0.1)
            scheduler.schedule_absolute(duetime, action)

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            time1 = scheduler.now
            time2 = None

            def action(scheduler, state):
                nonlocal time2
                time2 = scheduler.now
                event.set()

            def schedule():
                duetime = scheduler.now + timedelta(seconds=0.1)
                scheduler.schedule_absolute(duetime, action)

            threading.Thread(target=schedule).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert time2 is not None
            diff = (time2 - time1).total_seconds()
            assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute_cancel(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            ran = False

            def action(scheduler, state):
                nonlocal ran
                ran = True
                event.set()

            duetime = scheduler.now + timedelta(seconds=0.1)
            disp = scheduler.schedule_absolute(duetime, action)
            disp.dispose()

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False
            assert ran is False

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute_cancel_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            ran = False

            def action(scheduler, state):
                nonlocal ran
                ran = True
                event.set()

            def schedule():
                duetime = scheduler.now + timedelta(seconds=0.1)
                disp = scheduler.schedule_absolute(duetime, action)
                disp.dispose()

            threading.Thread(target=schedule).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.3, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False
            assert ran is False

        loop.run_until_complete(go())

    def test_asyncio_schedule_periodic(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
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

            try:
                yield from asyncio.wait_for(event.wait(), 0.6, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert len(times) - 1 == repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_periodic_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]
            repeat = 3

            def action(state):
                if state:
                    times.append(scheduler.now)
                    state -= 1
                else:
                    event.set()
                return state

            def schedule():
                scheduler.schedule_periodic(0.1, action, state=repeat)

            threading.Thread(target=schedule).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.6, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is True

            assert len(times) - 1 == repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_periodic_cancel(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            sad = SingleAssignmentDisposable()
            times = [scheduler.now]
            repeat = 3

            def action(state):
                if state:
                    times.append(scheduler.now)
                    state -= 1
                else:
                    event.set()
                return state

            sad.disposable = scheduler.schedule_periodic(0.1, action, state=repeat)

            yield from asyncio.sleep(0.15, loop=loop)

            sad.dispose()

            try:
                yield from asyncio.wait_for(event.wait(), 0.15, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False

            assert 0 < len(times) - 1 < repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_periodic_cancel_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            sad = SingleAssignmentDisposable()
            times = [scheduler.now]
            repeat = 3

            def action(state):
                if state:
                    times.append(scheduler.now)
                    state -= 1
                else:
                    event.set()
                return state

            def schedule():
                sad.disposable = scheduler.schedule_periodic(0.1, action, state=repeat)

            threading.Thread(target=schedule).start()

            yield from asyncio.sleep(0.15, loop=loop)

            threading.Thread(target=sad.dispose).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.14, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False

            assert 0 < len(times) - 1 < repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.25

        loop.run_until_complete(go())

    def test_asyncio_schedule_periodic_zero(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
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

            try:
                yield from asyncio.wait_for(event.wait(), 0.2, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False

            assert len(times) == 2
            diff = (times[1] - times[0]).total_seconds()
            assert diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_periodic_zero_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]
            repeat = 3

            def action(state):
                if state:
                    times.append(scheduler.now)
                    state -= 1
                else:
                    event.set()
                return state

            def schedule():
                scheduler.schedule_periodic(0.0, action, state=repeat)

            threading.Thread(target=schedule).start()

            try:
                yield from asyncio.wait_for(event.wait(), 0.2, loop=loop)
            except TimeoutError:
                pass

            assert event.is_set() is False

            assert len(times) == 2
            diff = (times[1] - times[0]).total_seconds()
            assert diff < 0.15

        loop.run_until_complete(go())
