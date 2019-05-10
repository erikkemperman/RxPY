import logging
import threading
from typing import Optional, Callable, Dict

from rx.core import typing
from rx.disposable import CompositeDisposable, Disposable, SingleAssignmentDisposable

from ..schedulerbase import SchedulerBase


log = logging.getLogger("Rx")


class WxScheduler(SchedulerBase):
    """A scheduler for a wxPython event loop."""

    def __init__(self, wx) -> None:
        super().__init__()
        self.wx = wx

        self._lock: threading.Lock = threading.Lock()
        self._timers: Dict[int, 'Timer'] = {}

        class Timer(wx.Timer):

            def __init__(self, callback: Callable[[], None]) -> None:
                super().__init__()
                self.callback: Callable[[], None] = callback

            def Notify(self):
                self.callback()

        self._timer_class = Timer

    def cancel_all(self) -> None:
        """Cancel all scheduled actions.

        Should be called when destroying wx controls to prevent
        accessing dead wx objects in actions that might be pending.
        """
        with self._lock:
            for timer in self._timers.values():
                timer.Stop()
            self._timers.clear()

    def _wxtimer_schedule(self,
                          duetime: typing.AbsoluteOrRelativeTime,
                          action: typing.ScheduledSingleOrPeriodicAction,
                          state: Optional[typing.TState] = None,
                          periodic: bool = False
                          ) -> typing.Disposable:
        once = not periodic or not bool(duetime)
        sad = SingleAssignmentDisposable()
        timer = None

        def interval() -> None:
            nonlocal state
            if periodic and sad.is_disposed is False:
                state = action(state)
            else:
                sad.disposable = action(self, state)

        msecs = max(1, int(SchedulerBase.to_seconds(duetime) * 1000.0))

        log.debug('timeout wx: %s', msecs)

        timer = self._timer_class(interval)
        started = timer.Start(msecs, oneShot=once)
        if started:
            with self._lock:
                self._timers[timer.GetId()] = timer

        def dispose() -> None:
            timer.Stop()
            with self._lock:
                del self._timers[timer.GetId()]

        return CompositeDisposable(sad, Disposable(dispose))

    def schedule(self,
                 action: typing.ScheduledAction,
                 state: Optional[typing.TState] = None
                 ) -> typing.Disposable:
        """Schedules an action to be executed.

        Args:
            action: Action to be executed.
            state: [Optional] state to be given to the action function.

        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        return self._wxtimer_schedule(0.0, action, state=state)

    def schedule_relative(self,
                          duetime: typing.RelativeTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed after duetime.

        Args:
            duetime: Relative time after which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.

        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """
        return self._wxtimer_schedule(duetime, action, state=state)

    def schedule_absolute(self,
                          duetime: typing.AbsoluteTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed at duetime.

        Args:
            duetime: Absolute time at which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.

        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        duetime = self.to_datetime(duetime)
        return self._wxtimer_schedule(duetime - self.now, action, state=state)

    def schedule_periodic(self,
                          period: typing.RelativeTime,
                          action: typing.ScheduledPeriodicAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules a periodic piece of work to be executed in the loop.

       Args:
            period: Period in seconds for running the work repeatedly.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.

        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        return self._wxtimer_schedule(period, action, state=state, periodic=True)
