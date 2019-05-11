import pytest
import unittest

import os
import threading
from datetime import datetime, timedelta
from time import sleep

from rx.concurrency.mainloopscheduler import GtkScheduler
from rx.internal.basic import default_now

gi = pytest.importorskip('gi')
skip = not gi
if not skip:
    try:
        gi.require_version('Gtk', '3.0')
        from gi.repository import GLib, Gtk
    except (ValueError, ImportError):
        skip = True


# Removing GNOME_DESKTOP_SESSION_ID from environment
# prevents QtScheduler test from failing with message
#   Gtk-ERROR **: GTK+ 2.x symbols detected.
#   Using GTK+ 2.x and GTK+ 3 in the same process is not supported
if 'GNOME_DESKTOP_SESSION_ID' in os.environ:
    del os.environ['GNOME_DESKTOP_SESSION_ID']


class Wait(threading.Thread):
    def __init__(self, event, timeout):
        super().__init__()
        self.event = event
        self.timeout = timeout

    def run(self):
        self.event.wait(self.timeout)
        Gtk.main_quit()


@pytest.mark.skipif('skip == True')
class TestGtkScheduler(unittest.TestCase):

    def test_gtk_now(self):
        scheduler = GtkScheduler()

        time1 = scheduler.now
        assert isinstance(time1, datetime)

        time2 = default_now()
        diff = (time2 - time1).total_seconds()
        assert abs(diff) < 0.01

    def test_gtk_now_units(self):
        scheduler = GtkScheduler()
        time1 = scheduler.now

        sleep(0.1)

        time2 = scheduler.now
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gtk_schedule(self):
        scheduler = GtkScheduler()
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule(action)

        Wait(event, 0.1).start()
        Gtk.main()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert diff < 0.15

    def test_gtk_schedule_relative(self):
        scheduler = GtkScheduler()
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        scheduler.schedule_relative(0.1, action)

        Wait(event, 0.3).start()
        Gtk.main()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gtk_schedule_relative_cancel(self):
        scheduler = GtkScheduler()
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        disp = scheduler.schedule_relative(0.1, action)
        disp.dispose()

        Wait(event, 0.3).start()
        Gtk.main()

        assert event.is_set() is False

        assert ran is False

    def test_gtk_schedule_absolute(self):
        scheduler = GtkScheduler()
        event = threading.Event()
        time1 = scheduler.now
        time2 = None

        def action(scheduler, state):
            nonlocal time2
            time2 = scheduler.now
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        scheduler.schedule_absolute(duetime, action)

        Wait(event, 0.3).start()
        Gtk.main()

        assert event.is_set() is True

        assert time2 is not None
        diff = (time2 - time1).total_seconds()
        assert 0.05 < diff < 0.25

    def test_gtk_schedule_absolute_cancel(self):
        scheduler = GtkScheduler()
        event = threading.Event()
        ran = False

        def action(scheduler, state):
            nonlocal ran
            ran = True
            event.set()

        duetime = scheduler.now + timedelta(seconds=0.1)
        disp = scheduler.schedule_absolute(duetime, action)
        disp.dispose()

        Wait(event, 0.3).start()
        Gtk.main()

        assert event.is_set() is False

        assert ran is False

    def test_gtk_schedule_periodic(self):
        scheduler = GtkScheduler()
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

        Wait(event, 0.6).start()
        Gtk.main()

        assert event.is_set() is True

        assert len(times) - 1 == repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_gtk_schedule_periodic_cancel(self):
        scheduler = GtkScheduler()
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

        def dispose(data):
            disp.dispose()
            return False

        GLib.timeout_add(150, dispose, None)

        Wait(event, 0.3).start()
        Gtk.main()

        assert event.is_set() is False

        assert 0 < len(times) - 1 < repeat
        for i in range(len(times) - 1):
            diff = (times[i + 1] - times[i]).total_seconds()
            assert 0.05 < diff < 0.25

    def test_gtk_schedule_periodic_zero(self):
        scheduler = GtkScheduler()
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

        Wait(event, 0.2).start()
        Gtk.main()

        assert event.is_set() is False

        assert len(times) == 2
        diff = (times[1] - times[0]).total_seconds()
        assert diff < 0.15
