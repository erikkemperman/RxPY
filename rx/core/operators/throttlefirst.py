from typing import Callable, Optional

from rx.core import Observable, typing
from rx.scheduler import timeout_scheduler


def _throttle_first(window_duration: typing.RelativeTime,
                    scheduler: Optional[typing.Scheduler] = None
                   ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def throttle_first(source: Observable) -> Observable:
        """Returns an observable that emits only the first item emitted
        by the source Observable during sequential time windows of a
        specifiedduration.

        Args:
            source: Source observable to throttle.

        Returns:
            An Observable that performs the throttle operation.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler

            duration = sub_scheduler.to_timedelta(window_duration or 0.0)
            if duration <= sub_scheduler.to_timedelta(0):
                raise ValueError('window_duration cannot be less or equal zero.')
            last_on_next = [0]

            def _on_next(x):
                emit = False
                now = sub_scheduler.now

                with source.lock:
                    if not last_on_next[0] or now - last_on_next[0] >= duration:
                        last_on_next[0] = now
                        emit = True
                if emit and on_next is not None:
                    on_next(x)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=sub_scheduler
            )
        return Observable(subscribe)
    return throttle_first
