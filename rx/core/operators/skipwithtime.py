from typing import Any, Callable, Optional

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable
from rx.scheduler import timeout_scheduler


def _skip_with_time(duration: typing.RelativeTime,
                    scheduler: Optional[typing.Scheduler] = None
                    ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def skip_with_time(source: Observable) -> Observable:
        """Skips elements for the specified duration from the start of
        the observable source sequence.

        Args:
            >>> res = skip_with_time(5.0)

        Specifying a zero value for duration doesn't guarantee no
        elements will be dropped from the start of the source sequence.
        This is a side-effect of the asynchrony introduced by the
        scheduler, where the action that causes callbacks from the
        source sequence to be forwarded may not execute immediately,
        despite the zero due time.

        Errors produced by the source sequence are always forwarded to
        the result sequence, even if the error occurs before the
        duration.

        Args:
            duration: Duration for skipping elements from the start of
            the sequence.

        Returns:
            An observable sequence with the elements skipped during the
            specified duration from the start of the source sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler
            open = [False]

            def action(_: typing.Scheduler, __: Any = None) -> None:
                open[0] = True

            t = sub_scheduler.schedule_relative(duration, action)

            def _on_next(x):
                if open[0] and on_next is not None:
                    on_next(x)

            d = source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
            return CompositeDisposable(t, d)
        return Observable(subscribe)
    return skip_with_time
