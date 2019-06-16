from typing import Any, Callable, Optional

from rx.core import typing
from rx.core import Observable
from rx.scheduler import current_thread_scheduler


def _return_value(value: Any, scheduler: Optional[typing.Scheduler] = None) -> Observable:
    """Returns an observable sequence that contains a single element,
    using the specified scheduler to send out observer messages.
    There is an alias called 'just'.

    Examples:
        >>> res = return(42)
        >>> res = return(42, rx.Scheduler.timeout)

    Args:
        value: Single element in the resulting observable sequence.

    Returns:
        An observable sequence containing the single specified
        element.
    """

    obs_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or current_thread_scheduler

        def action(_: typing.Scheduler, __: Any = None) -> None:
            if on_next is not None:
                on_next(value)
            if on_completed is not None:
                on_completed()

        return sub_scheduler.schedule(action)
    return Observable(subscribe)


def _from_callable(supplier: Callable[[], Any],
                   scheduler: Optional[typing.Scheduler] = None
                   ) -> Observable:
    obs_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or current_thread_scheduler

        def action(_: typing.Scheduler, __: Any = None) -> None:
            try:
                if on_next is not None:
                    on_next(supplier())
                if on_completed is not None:
                    on_completed()
            except Exception as e:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(e)
        return sub_scheduler.schedule(action)

    return Observable(subscribe)
