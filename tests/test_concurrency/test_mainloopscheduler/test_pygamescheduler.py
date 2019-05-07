import pytest
import unittest

from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import PyGameScheduler
from rx.internal.basic import default_now


pygame = pytest.importorskip('pygame')


class TestPyGameScheduler(unittest.TestCase):

    def test_pygame_now(self):
        scheduler = PyGameScheduler()

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = default_now()
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_pygame_now_units(self):
        scheduler = PyGameScheduler()
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_pygame_schedule(self):
        scheduler = PyGameScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule(action)
        scheduler.run()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_pygame_schedule_relative(self):
        scheduler = PyGameScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule_relative(0.1, action)
        scheduler.run()

        assert time2 is None

        sleep(0.1)
        scheduler.run()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_pygame_schedule_relative_cancel(self):
        scheduler = PyGameScheduler()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        sleep(0.2)
        scheduler.run()

        assert ran is False

    def test_pygame_schedule_absolute(self):
        scheduler = PyGameScheduler()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)
        scheduler.run()

        assert time2 is None

        sleep(0.1)
        scheduler.run()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_pygame_schedule_absolute_cancel(self):
        scheduler = PyGameScheduler()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        sleep(0.2)
        scheduler.run()

        assert ran is False

    def test_pygame_schedule_periodic(self):
        scheduler = PyGameScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(0.1, action, state=repeat)

        stop = scheduler.now + timedelta(seconds=0.6)
        while scheduler.now < stop:
            scheduler.run()
            sleep(0.05)

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_pygame_schedule_periodic_cancel(self):
        scheduler = PyGameScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        disp = scheduler.schedule_periodic(0.1, action, state=repeat)

        stop = scheduler.now + timedelta(seconds=0.15)
        while scheduler.now < stop:
            scheduler.run()
            sleep(0.05)

        disp.dispose()

        stop = scheduler.now + timedelta(seconds=0.15)
        while scheduler.now < stop:
            scheduler.run()
            sleep(0.05)

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_pygame_schedule_periodic_zero(self):
        scheduler = PyGameScheduler()
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(0.0, action, state=repeat)

        stop = scheduler.now + timedelta(seconds=0.2)
        while scheduler.now < stop:
            scheduler.run()
            sleep(0.05)

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
