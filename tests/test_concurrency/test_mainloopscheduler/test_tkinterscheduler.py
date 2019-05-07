import pytest
import unittest

from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import TkinterScheduler
from rx.internal.basic import default_now


skip = False
tkinter = pytest.importorskip('tkinter')
if tkinter:
    try:
        master = tkinter.Tk()
        master.wm_withdraw()
    except Exception:
        skip = True


@pytest.mark.skipif('skip == True')
class TestTkinterScheduler(unittest.TestCase):

    def test_tkinter_now(self):
        scheduler = TkinterScheduler(master)

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = default_now()
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_tkinter_now_units(self):
        scheduler = TkinterScheduler(master)
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_tkinter_schedule(self):
        scheduler = TkinterScheduler(master)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule(action)

        master.after_idle(master.quit)
        master.mainloop()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_tkinter_schedule_relative(self):
        scheduler = TkinterScheduler(master)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule_relative(0.1, action)

        master.after(300, master.quit)
        master.mainloop()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_tkinter_schedule_relative_cancel(self):
        scheduler = TkinterScheduler(master)
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        master.after(300, master.quit)
        master.mainloop()

        assert ran is False

    def test_tkinter_schedule_periodic(self):
        scheduler = TkinterScheduler(master)
        times = [scheduler.now]
        repeat = 3
        period = 0.1

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(period, action, state=repeat)

        master.after(600, master.quit)
        master.mainloop()

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25
