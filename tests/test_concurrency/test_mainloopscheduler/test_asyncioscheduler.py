import unittest

import asyncio
from datetime import datetime, timedelta
from sys import version_info
from threading import current_thread, Thread

from rx.concurrency.mainloopscheduler import AsyncIOScheduler
from rx.disposable import SingleAssignmentDisposable


if version_info < (3, 8, 0):
    from asyncio.futures import TimeoutError
else:
    from asyncio.exceptions import TimeoutError


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

        yield from asyncio.sleep(0.05, loop=loop)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert diff < 0.10

    def test_asyncio_schedule(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            scheduler.schedule(action)

            yield from wait(loop, event, 0.05)

            assert len(times) is 2
            diff = (times[1] - times[0]).total_seconds()
            assert diff < 0.10


        loop.run_until_complete(go())

    def test_asyncio_schedule_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            def schedule():
                scheduler.schedule(action)

            Thread(target=schedule).start()

            yield from wait(loop, event, 0.05)

            assert len(times) is 2
            diff = (times[1] - times[0]).total_seconds()
            assert diff < 0.10

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            scheduler.schedule_relative(0.05, action)

            yield from wait(loop, event, 0.10)

            assert len(times) is 2
            diff = (times[1] - times[0]).total_seconds()
            assert 0.05 < diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            def schedule():
                scheduler.schedule_relative(0.05, action)

            Thread(target=schedule).start()

            yield from wait(loop, event, 0.10)

            assert len(times) is 2
            diff = (times[1] - times[0]).total_seconds()
            assert 0.05 < diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_cancel(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            disp = scheduler.schedule_relative(0.05, action)
            disp.dispose()

            yield from wait(loop, event, 0.10)

            assert len(times) is 1

        loop.run_until_complete(go())

    def test_asyncio_schedule_relative_cancel_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            def schedule():
                disp = scheduler.schedule_relative(0.05, action)
                disp.dispose()

            Thread(target=schedule).start()

            yield from wait(loop, event, 0.10)

            assert len(times) is 1

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            duetime = scheduler.now + timedelta(seconds=0.05)
            scheduler.schedule_absolute(duetime, action)

            yield from wait(loop, event, 0.10)

            assert len(times) is 2
            diff = (times[1] - times[0]).total_seconds()
            assert 0.05 < diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            def schedule():
                duetime = scheduler.now + timedelta(seconds=0.05)
                scheduler.schedule_absolute(duetime, action)

            Thread(target=schedule).start()

            yield from wait(loop, event, 0.10)

            assert len(times) is 2
            diff = (times[1] - times[0]).total_seconds()
            assert 0.05 < diff < 0.15

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute_cancel(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=False)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            duetime = scheduler.now + timedelta(seconds=0.05)
            disp = scheduler.schedule_absolute(duetime, action)
            disp.dispose()

            yield from wait(loop, event, 0.10)

            assert len(times) is 1

        loop.run_until_complete(go())

    def test_asyncio_schedule_absolute_cancel_threadsafe(self):
        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def go():
            scheduler = AsyncIOScheduler(loop, threadsafe=True)
            event = asyncio.Event(loop=loop)
            times = [scheduler.now]

            def action(scheduler, state):
                times.append(scheduler.now)
                event.set()

            def schedule():
                duetime = scheduler.now + timedelta(seconds=0.05)
                disp = scheduler.schedule_absolute(duetime, action)
                disp.dispose()

            Thread(target=schedule).start()

            yield from wait(loop, event, 0.10)

            assert len(times) is 1

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

            scheduler.schedule_periodic(0.05, action, state=repeat)

            yield from wait(loop, event, 0.30)

            assert len(times) - 1 == repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.15

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
                scheduler.schedule_periodic(0.05, action, state=repeat)

            Thread(target=schedule).start()

            yield from wait(loop, event, 0.3)

            assert len(times) - 1 == repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.15

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

            sad.disposable = scheduler.schedule_periodic(0.05, action, state=repeat)

            yield from asyncio.sleep(0.10, loop=loop)

            sad.dispose()

            yield from wait(loop, event, 0.10)

            assert 0 < len(times) - 1 < repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.15

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
                sad.disposable = scheduler.schedule_periodic(0.05, action, state=repeat)

            Thread(target=schedule).start()

            yield from asyncio.sleep(0.10, loop=loop)

            Thread(target=sad.dispose).start()

            yield from wait(loop, event, 0.10)

            assert 0 < len(times) - 1 < repeat
            for i in range(len(times) - 1):
                diff = (times[i + 1] - times[i]).total_seconds()
                assert 0.05 < diff < 0.15

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

            yield from wait(loop, event, 0.1)

            assert len(times) == 2
            diff = (times[1] - times[0]).total_seconds()
            assert diff < 0.10

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

            Thread(target=schedule).start()

            yield from wait(loop, event, 0.10)

            assert len(times) == 2
            diff = (times[1] - times[0]).total_seconds()
            assert diff < 0.10

        loop.run_until_complete(go())
