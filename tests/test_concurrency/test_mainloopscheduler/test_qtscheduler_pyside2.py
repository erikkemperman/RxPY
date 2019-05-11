import pytest
import unittest

import threading
from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import QtScheduler
from rx.internal.basic import default_now


PySide2 = pytest.importorskip('PySide2')
skip = not PySide2
if not skip:
    try:
        from PySide2 import QtCore
    except ImportError:
        skip = True

app = None  # Prevent garbage collection


def make_app():
    global app
    app = QtCore.QCoreApplication.instance()
    if app is None:
        app = QtCore.QCoreApplication([])
    return app


class Wait(QtCore.QThread):
    def __init__(self, app, event, timeout):
        super().__init__()
        self.app = app
        self.event = event
        self.timeout = timeout

    def run(self):
        self.event.wait(self.timeout)
        self.app.quit()


@pytest.mark.skipif('skip == True')
class TestQtScheduler(unittest.TestCase):

    def test_pyside2_now(self):
        scheduler = QtScheduler(QtCore)

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = default_now()
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_pyside2_now_units(self):
        scheduler = QtScheduler(QtCore)
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_pyside2_schedule(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule(action)

        Wait(app, event, 0.1).start()
        app.exec_()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_pyside2_schedule_relative(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule_relative(0.1, action)

        Wait(app, event, 0.3).start()
        app.exec_()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_pyside2_schedule_relative_cancel(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        Wait(app, event, 0.3).start()
        app.exec_()

        assert event.is_set() is False

        assert ran is False

    def test_pyside2_schedule_absolute(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        Wait(app, event, 0.3).start()
        app.exec_()

        assert event.is_set() is True
        
        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_pyside2_schedule_absolute_cancel(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        Wait(app, event, 0.3).start()
        app.exec_()

        assert event.is_set() is False

        assert ran is False

    def test_pyside2_schedule_periodic(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
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

        scheduler.schedule_periodic(0.1, action, state=repeat)

        Wait(app, event, 0.6).start()
        app.exec_()

        assert event.is_set() is True

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_pyside2_schedule_periodic_cancel(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
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

        disp = scheduler.schedule_periodic(0.1, action, state=repeat)

        QtCore.QTimer.singleShot(150, disp.dispose)

        Wait(app, event, 0.3).start()
        app.exec_()
        
        assert event.is_set() is False
        
        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_pyside2_schedule_zero(self):
        app = make_app()
        scheduler = QtScheduler(QtCore)
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

        Wait(app, event, 0.2).start()
        app.exec_()

        assert event.is_set() is False
        
        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
