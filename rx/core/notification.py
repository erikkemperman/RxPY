from abc import abstractmethod
from typing import Any, Optional

from rx.scheduler import immediate_scheduler

from .. import typing
from .observer import Observer
from .observable import Observable


class Notification:
    """Represents a notification to an observer."""

    def __init__(self) -> None:
        """Default constructor used by derived types."""
        self.has_value = False
        self.value = None
        self.kind = ''

    @abstractmethod
    def accept(self,
               on_next: Optional[typing.OnNext] = None,
               on_error: Optional[typing.OnError] = None,
               on_completed: Optional[typing.OnCompleted] = None):
        """Invokes the delegate corresponding to the notification and
        returns the produced result.

        Examples:
            >>> notification.accept(on_next, on_error, on_completed)

        Args:
            on_next: Delegate to invoke for an OnNext notification.
            on_error: [Optional] Delegate to invoke for an OnError
                notification.
            on_completed: [Optional] Delegate to invoke for an
                OnCompleted notification.

        Returns:
            Result produced by the observation."""

        raise NotImplementedError

    def to_observable(self, scheduler=None):
        """Returns an observable sequence with a single notification,
        using the specified scheduler, else the immediate scheduler.

        Args:
            scheduler: [Optional] Scheduler to send out the
                notification calls on.

        Returns:
            An observable sequence that surfaces the behavior of the
            notification upon subscription.
        """

        obs_scheduler = scheduler

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = obs_scheduler or scheduler or immediate_scheduler
            def action(_: typing.Scheduler, __: Any = None) -> None:
                self.accept(on_next, on_error, on_completed)
                if self.kind == 'N' and on_completed is not None:
                    on_completed()

            return sub_scheduler.schedule(action)
        return Observable(subscribe)

    def equals(self, other):
        """Indicates whether this instance and a specified object are
        equal."""

        other_string = '' if not other else str(other)
        return str(self) == other_string

    def __eq__(self, other):
        return self.equals(other)


class OnNext(Notification):
    """Represents an OnNext notification to an observer."""

    def __init__(self, value):
        """Constructs a notification of a new value."""

        super().__init__()
        self.value = value
        self.has_value = True
        self.kind = 'N'

    def accept(self,
               on_next: Optional[typing.OnNext] = None,
               on_error: Optional[typing.OnError] = None,
               on_completed: Optional[typing.OnCompleted] = None):
        return on_next(self.value)

    def __str__(self):
        return "OnNext(%s)" % str(self.value)


class OnError(Notification):
    """Represents an OnError notification to an observer."""

    def __init__(self, exception):
        """Constructs a notification of an exception."""

        super().__init__()
        self.exception = exception
        self.kind = 'E'

    def accept(self,
               on_next: Optional[typing.OnNext] = None,
               on_error: Optional[typing.OnError] = None,
               on_completed: Optional[typing.OnCompleted] = None):
        return on_error(self.exception)

    def __str__(self):
        return "OnError(%s)" % str(self.exception)


class OnCompleted(Notification):
    """Represents an OnCompleted notification to an observer."""

    def __init__(self):
        """Constructs a notification of the end of a sequence."""

        super().__init__()
        self.kind = 'C'

    def accept(self,
               on_next: Optional[typing.OnNext] = None,
               on_error: Optional[typing.OnError] = None,
               on_completed: Optional[typing.OnCompleted] = None):
        return on_completed()

    def __str__(self):
        return "OnCompleted()"


def from_notifier(handler):
    """Creates an observer from a notification callback.

    Args:
        handler: Action that handles a notification.

    Returns:
        The observer object that invokes the specified handler using
        a notification corresponding to each message it receives.
    """

    def _on_next(value):
        return handler(OnNext(value))

    def _on_error(error):
        return handler(OnError(error))

    def _on_completed():
        return handler(OnCompleted())

    return Observer(_on_next, _on_error, _on_completed)
