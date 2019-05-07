import pytest
import unittest

from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import WxScheduler
from rx.disposable import SingleAssignmentDisposable
from rx.internal.basic import default_now

wx = pytest.importorskip('wx')

app = None  # Prevent garbage collection
quit = None


class AppExit(wx.Timer):

    def Notify(self):
        app.DeletePendingEvents()
        app.ExitMainLoop()


def make_app():
    global app, quit
    if app is None:
        app = wx.AppConsole()
        quit = AppExit(app)
    return app, quit


class TestWxScheduler(unittest.TestCase):

    def test_wx_now(self):
        scheduler = WxScheduler(wx)
        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = default_now()
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_wx_now_units(self):
        scheduler = WxScheduler(wx)
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_wx_schedule(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        quit.Start(100, oneShot=True)

        scheduler.schedule(action)

        app.MainLoop()
        scheduler.cancel_all()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_wx_schedule_relative(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        quit.Start(300, oneShot=True)

        scheduler.schedule_relative(0.1, action)

        app.MainLoop()
        scheduler.cancel_all()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_wx_schedule_relative_cancel(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        quit.Start(300, oneShot=True)

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        app.MainLoop()
        scheduler.cancel_all()

        assert ran is False

    def test_wx_schedule_absolute(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        quit.Start(300, oneShot=True)

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        app.MainLoop()
        scheduler.cancel_all()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_wx_schedule_absolute_cancel(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        quit.Start(300, oneShot=True)

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        app.MainLoop()
        scheduler.cancel_all()

        assert ran is False

    def test_wx_schedule_periodic(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        times = [scheduler.now]
        repeat = 3
        period = 0.1

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        quit.Start(600, oneShot=True)

        scheduler.schedule_periodic(period, action, state=repeat)

        app.MainLoop()
        scheduler.cancel_all()

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_wx_schedule_periodic_cancel(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        times = [scheduler.now]
        repeat = 3
        period = 0.1

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        sad = SingleAssignmentDisposable()

        class Dispose(wx.Timer):
            def Notify(self):
                sad.dispose()

        Dispose().Start(150, oneShot=True)
        quit.Start(300, oneShot=True)

        sad.disposable = scheduler.schedule_periodic(period, action, state=repeat)

        app.MainLoop()
        scheduler.cancel_all()

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_wx_schedule_periodic_zero(self):
        app, quit = make_app()
        scheduler = WxScheduler(wx)
        times = [scheduler.now]
        repeat = 3

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(0.0, action, state=repeat)

        quit.Start(200)

        app.MainLoop()
        scheduler.cancel_all()

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
