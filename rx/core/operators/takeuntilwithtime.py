from typing import Any, Callable, Optional
from datetime import datetime

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable
from rx.scheduler import timeout_scheduler


def _take_until_with_time(end_time: typing.AbsoluteOrRelativeTime,
                          scheduler: Optional[typing.Scheduler] = None
                          ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def take_until_with_time(source: Observable) -> Observable:
        """Takes elements for the specified duration until the specified end
        time, using the specified scheduler to run timers.

        Examples:
            >>> res = take_until_with_time(source)

        Args:
            source: Source observale to take elements from.

        Returns:
            An observable sequence with the elements taken
            until the specified end time.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler

            if isinstance(end_time, datetime):
                scheduler_method = sub_scheduler.schedule_absolute
            else:
                scheduler_method = sub_scheduler.schedule_relative

            def action(_: typing.Scheduler, __: Any = None) -> None:
                if on_completed is not None:
                    on_completed()

            task = scheduler_method(end_time, action)
            sub = source.subscribe(on_next, on_error, on_completed,
                                   scheduler=scheduler)
            return CompositeDisposable(task, sub)
        return Observable(subscribe)
    return take_until_with_time
