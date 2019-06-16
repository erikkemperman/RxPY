from datetime import datetime
from typing import Any, Callable, Optional

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable
from rx.scheduler import timeout_scheduler


def _skip_until_with_time(start_time: typing.AbsoluteOrRelativeTime,
                          scheduler: Optional[typing.Scheduler] = None
                          ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def skip_until_with_time(source: Observable) -> Observable:
        """Skips elements from the observable source sequence until the
        specified start time.

        Errors produced by the source sequence are always forwarded to
        the result sequence, even if the error occurs before the start
        time.

        Examples:
            >>> res = source.skip_until_with_time(datetime)
            >>> res = source.skip_until_with_time(5.0)

        Args:
            start_time: Time to start taking elements from the source
                sequence. If this value is less than or equal to
                `datetime.utcnow`, no elements will be skipped.

        Returns:
            An observable sequence with the elements skipped until the
            specified start time.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler

            if isinstance(start_time, datetime):
                scheduler_method = sub_scheduler.schedule_absolute
            else:
                scheduler_method = sub_scheduler.schedule_relative

            open = [False]

            def _on_next(x):
                if open[0] and on_next is not None:
                    on_next(x)

            subscription = source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )

            def action(_: typing.Scheduler, __: Any = None) -> None:
                open[0] = True

            disp = scheduler_method(start_time, action)
            return CompositeDisposable(disp, subscription)
        return Observable(subscribe)
    return skip_until_with_time
