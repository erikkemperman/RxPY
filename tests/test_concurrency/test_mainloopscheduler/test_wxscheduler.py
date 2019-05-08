import pytest
import unittest

import threading
from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import WxScheduler
from rx.disposable import SingleAssignmentDisposable
from rx.internal.basic import default_now

wx = pytest.importorskip('wx')

app = None  # Prevent garbage collection


class Quit(wx.Timer):
    def __init__(self, app, event, timeout):
        super().__init__()
        self.app = app
        self.event = event
        self.timeout = timeout
        self.stop = default_now() + timedelta(seconds=timeout)

    def Notify(self):
        print('test')
        if self.event.is_set() or default_now() >= self.stop:
            self.app.ExitMainLoop()
            self.Stop()


def make_app():
    global app
    if app is None:
        app = wx.App()
        wx.Frame(None)  # need this for some reason
    return app


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
        app = make_app()
        scheduler = WxScheduler(wx)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule(action)

        def wait():
            event.wait(0.1)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_wx_schedule_relative(self):
        app = make_app()
        scheduler = WxScheduler(wx)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule_relative(0.1, action)

        def wait():
            event.wait(0.3)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_wx_schedule_relative_cancel(self):
        app = make_app()
        scheduler = WxScheduler(wx)
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        def wait():
            event.wait(0.3)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is False

        assert ran is False

    def test_wx_schedule_absolute(self):
        app = make_app()
        scheduler = WxScheduler(wx)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        def wait():
            event.wait(0.3)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_wx_schedule_absolute_cancel(self):
        app = make_app()
        scheduler = WxScheduler(wx)
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        def wait():
            event.wait(0.3)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is False

        assert ran is False

    def test_wx_schedule_periodic(self):
        app = make_app()
        scheduler = WxScheduler(wx)
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

        def wait():
            event.wait(0.6)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is True

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_wx_schedule_periodic_cancel(self):
        app = make_app()
        scheduler = WxScheduler(wx)
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

        sad = SingleAssignmentDisposable()

        sad.disposable = scheduler.schedule_periodic(period, action, state=repeat)

        def wait():
            sleep(0.15)
            sad.dispose()
            event.wait(0.15)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is False

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_wx_schedule_periodic_zero(self):
        app = make_app()
        scheduler = WxScheduler(wx)
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

        def wait():
            event.wait(0.2)
            app.ExitMainLoop()

        threading.Thread(target=wait).start()

        app.MainLoop()
        scheduler.cancel_all()

        assert event.is_set() is False

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
