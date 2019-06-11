import logging
import asyncio

from datetime import datetime
from typing import Optional

from rx.core import typing
from rx.disposable import CompositeDisposable, Disposable, SingleAssignmentDisposable

from ..periodicscheduler import PeriodicScheduler


log = logging.getLogger("Rx")


class AsyncIOScheduler(PeriodicScheduler):
    """A scheduler that schedules work via the asyncio mainloop. This class
    does not use the asyncio threadsafe methods, if you need those please use
    the AsyncIOThreadSafeScheduler class."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Create a new AsyncIOScheduler.

        Args:
            loop: Instance of asyncio event loop to use; typically, you would
                get this by asyncio.get_event_loop()
        """
        super().__init__()
        self._loop: asyncio.AbstractEventLoop = loop

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
            sad.disposable = self.invoke_action(action, state=state)

        handle = self._loop.call_soon(interval)

        def dispose() -> None:
            handle.cancel()

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
        if seconds <= 0:
            return self.schedule(action, state)

        sad = SingleAssignmentDisposable()

        def interval() -> None:
            sad.disposable = self.invoke_action(action, state=state)

        handle = self._loop.call_later(seconds, interval)

        def dispose() -> None:
            handle.cancel()

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
        return self.schedule_relative(relative, action, state=state)

    @property
    def now(self) -> datetime:
        """Represents a notion of time for this scheduler. Tasks being
        scheduled on a scheduler will adhere to the time denoted by this
        property.

        Returns:
             The scheduler's current time, as a datetime instance.
        """

        return self.to_datetime(self._loop.time())
