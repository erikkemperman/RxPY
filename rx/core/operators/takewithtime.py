from typing import Any, Callable, Optional

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable
from rx.scheduler import timeout_scheduler


def _take_with_time(duration: typing.RelativeTime,
                    scheduler: Optional[typing.Scheduler] = None
                    ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def take_with_time(source: Observable) -> Observable:
        """Takes elements for the specified duration from the start of
        the observable source sequence.

        Example:
            >>> res = take_with_time(source)

        This operator accumulates a queue with a length enough to store
        elements received during the initial duration window. As more
        elements are received, elements older than the specified
        duration are taken from the queue and produced on the result
        sequence. This causes elements to be delayed with duration.

        Args:
            source: Source observable to take elements from.

        Returns:
            An observable sequence with the elements taken during the
            specified duration from the start of the source sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler

            def action(_: typing.Scheduler, __: Any = None) -> None:
                if on_completed is not None:
                    on_completed()

            disp = sub_scheduler.schedule_relative(duration, action)
            sub = source.subscribe(on_next, on_error, on_completed,
                                   scheduler=scheduler)
            return CompositeDisposable(disp, sub)
        return Observable(subscribe)
    return take_with_time
