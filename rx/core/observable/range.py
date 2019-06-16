from typing import Iterator, Optional

from rx.core import typing
from rx.core import Observable
from rx.scheduler import current_thread_scheduler
from rx.disposable import MultipleAssignmentDisposable


def _range(start: int,
           stop: Optional[int] = None,
           step: Optional[int] = None,
           scheduler: Optional[typing.Scheduler] = None
           ) -> Observable:
    """Generates an observable sequence of integral numbers within a
    specified range, using the specified scheduler to send out observer
    messages.

    Examples:
        >>> res = range(10)
        >>> res = range(0, 10)
        >>> res = range(0, 10, 1)

    Args:
        start: The value of the first integer in the sequence.
        count: The number of sequential integers to generate.
        scheduler: The scheduler to schedule the values on.

    Returns:
        An observable sequence that contains a range of sequential
        integral numbers.
    """

    obs_scheduler = scheduler

    if step is None and stop is None:
        range_t = range(start)
    elif step is None:
        range_t = range(start, stop)
    else:
        range_t = range(start, stop, step)

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or current_thread_scheduler

        sd = MultipleAssignmentDisposable()

        def action(act_scheduler: typing.Scheduler, it: Iterator[int]) -> None:
            try:
                if on_next is not None:
                    on_next(next(it))
                sd.disposable = act_scheduler.schedule(action, state=it)
            except StopIteration:
                if on_completed is not None:
                    on_completed()

        sd.disposable = sub_scheduler.schedule(action, iter(range_t))
        return sd
    return Observable(subscribe)
