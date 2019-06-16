from typing import Iterable, Any, Optional

from rx.core import Observable, typing
from rx.scheduler import current_thread_scheduler
from rx.disposable import CompositeDisposable, Disposable


def from_iterable(iterable: Iterable, scheduler: Optional[typing.Scheduler] = None) -> Observable:
    """Converts an iterable to an observable sequence.

    Example:
        >>> from_iterable([1,2,3])

    Args:
        iterable: A Python iterable
        scheduler: An optional scheduler to schedule the values on.

    Returns:
        The observable sequence whose elements are pulled from the
        given iterable sequence.
    """

    obs_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or current_thread_scheduler
        iterator = iter(iterable)
        disposed = False

        def action(_: typing.Scheduler, __: Any = None) -> None:
            nonlocal disposed

            try:
                while not disposed:
                    value = next(iterator)
                    if on_next is not None:
                        on_next(value)
            except StopIteration:
                if on_completed is not None:
                    on_completed()
            except Exception as error:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(error)

        def dispose() -> None:
            nonlocal disposed
            disposed = True

        disp = Disposable(dispose)
        return CompositeDisposable(sub_scheduler.schedule(action), disp)
    return Observable(subscribe)
