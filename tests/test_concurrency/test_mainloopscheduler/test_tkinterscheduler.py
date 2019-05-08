import pytest
import unittest

import threading
from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import TkinterScheduler
from rx.internal.basic import default_now


tkinter = pytest.importorskip('tkinter')

master = None  # Prevent garbage collection


def make_master():
    global master
    if master is None:
        master = tkinter.Tk()
        master.withdraw()
    return master


class Quit(threading.Thread):

    def __init__(self, master, event, timeout):
        super().__init__()
        self.master = master
        self.event = event
        self.timeout = timeout

    def run(self):
        self.event.wait(self.timeout)
        self.master.quit()


class TestTkinterScheduler(unittest.TestCase):

    def test_tkinter_now(self):
        master = make_master()
        scheduler = TkinterScheduler(master)

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = default_now()
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_tkinter_now_units(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_tkinter_schedule(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule(action)

        Quit(master, event, 0.1).start()
        master.mainloop()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_tkinter_schedule_relative(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule_relative(0.1, action)

        Quit(master, event, 0.3).start()
        master.mainloop()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_tkinter_schedule_relative_cancel(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        Quit(master, event, 0.3).start()
        master.mainloop()

        assert event.is_set() is False

        assert ran is False

    def test_tkinter_schedule_absolute(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        Quit(master, event, 0.3).start()
        master.mainloop()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_tkinter_schedule_absolute_cancel(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        Quit(master, event, 0.3).start()
        master.mainloop()

        assert event.is_set() is False

        assert ran is False

    def test_tkinter_schedule_periodic(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
        times = [scheduler.now]
        repeat = 3
        period = 0.1

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif event.is_set() is False:
                event.set()
            return state

        scheduler.schedule_periodic(period, action, state=repeat)

        Quit(master, event, 0.6).start()
        master.mainloop()

        assert event.is_set() is True

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_tkinter_schedule_periodic_cancel(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
        times = [scheduler.now]
        repeat = 3
        period = 0.1

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            elif event.is_set() is False:
                event.set()
            return state

        disp = scheduler.schedule_periodic(period, action, state=repeat)

        master.after(150, disp.dispose)
        Quit(master, event, 0.15).start()
        master.mainloop()

        assert event.is_set() is False

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_tkinter_schedule_periodic_zero(self):
        master = make_master()
        scheduler = TkinterScheduler(master)
        event = threading.Event()
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

        Quit(master, event, 0.2).start()
        master.mainloop()

        assert event.is_set() is False

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
