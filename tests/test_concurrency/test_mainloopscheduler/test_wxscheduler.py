import pytest
import unittest

from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import WxScheduler
from rx.internal.basic import default_now

wx = pytest.importorskip('wx')


class AppExit(wx.Timer):

    def __init__(self, app) -> None:
        super().__init__()
        self.app = app

    def Notify(self):
        self.app.ExitMainLoop()


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
        app = wx.AppConsole()
        quit = AppExit(app)
        scheduler = WxScheduler(wx)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule(action)

        quit.Start(100, wx.TIMER_ONE_SHOT)

        app.MainLoop()
        scheduler.cancel_all()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_wx_schedule_relative(self):
        app = wx.AppConsole()
        quit = AppExit(app)
        scheduler = WxScheduler(wx)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        scheduler.schedule_relative(0.1, action)

        quit.Start(300, wx.TIMER_ONE_SHOT)

        app.MainLoop()
        scheduler.cancel_all()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_wx_schedule_absolute(self):
        app = wx.AppConsole()
        quit = AppExit(app)
        scheduler = WxScheduler(wx)
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now

        duetime = time1 + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        quit.Start(300, wx.TIMER_ONE_SHOT)

        app.MainLoop()
        scheduler.cancel_all()

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_wx_schedule_relative_cancel(self):
        app = wx.AppConsole()
        quit = AppExit(app)
        scheduler = WxScheduler(wx)
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        quit.Start(300, wx.TIMER_ONE_SHOT)

        app.MainLoop()
        scheduler.cancel_all()

        assert ran is False

    def test_wx_schedule_periodic(self):
        app = wx.AppConsole()
        quit = AppExit(app)
        scheduler = WxScheduler(wx)
        times = [scheduler.now]
        repeat = 3
        period = 0.1

        def action(state):
            if state:
                times.append(scheduler.now)
                state -= 1
            return state

        scheduler.schedule_periodic(period, action, state=repeat)

        quit.Start(300, wx.TIMER_ONE_SHOT)

        app.MainLoop()
        scheduler.cancel_all()

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25
