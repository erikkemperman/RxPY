from threading import Timer
from typing import Optional

from rx.core import typing
from rx.disposable import CompositeDisposable, Disposable, SingleAssignmentDisposable

from .periodicscheduler import PeriodicScheduler


class TimeoutScheduler(PeriodicScheduler):
    """A scheduler that schedules work via a timed callback."""

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

        sad = SingleAssignmentDisposable()

        def interval() -> None:
            sad.disposable = self.invoke_action(action, state)

        timer = Timer(0, interval)
        timer.setDaemon(True)
        timer.start()

        def dispose() -> None:
            timer.cancel()

        return CompositeDisposable(sad, Disposable(dispose))

    def schedule_relative(self,
                          relative: typing.RelativeTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed after duetime.

        Args:
            relative: Relative time after which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.

        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        seconds = self.to_seconds(relative)
        if seconds <= 0.0:
            return self.schedule(action, state)

        sad = SingleAssignmentDisposable()

        def interval() -> None:
            sad.disposable = self.invoke_action(action, state)

        timer = Timer(seconds, interval)
        timer.setDaemon(True)
        timer.start()

        def dispose() -> None:
            timer.cancel()

        return CompositeDisposable(sad, Disposable(dispose))

    def schedule_absolute(self,
                          absolute: typing.AbsoluteTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed at duetime.

        Args:
            absolute: Absolute time at which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.

        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        relative = self.to_datetime(absolute) - self.now
        return self.schedule_relative(relative, action, state)


timeout_scheduler = TimeoutScheduler()
