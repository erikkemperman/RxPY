from typing import Optional

from rx.core import typing
from rx.internal.constants import DELTA_ZERO
from rx.internal.exceptions import WouldBlockException

from .scheduler import Scheduler


class ImmediateScheduler(Scheduler):

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

        return self.invoke_action(action, state)

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

        relative = self.to_timedelta(relative)
        if relative > DELTA_ZERO:
            raise WouldBlockException()

        return self.invoke_action(action, state)

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


immediate_scheduler = ImmediateScheduler()
