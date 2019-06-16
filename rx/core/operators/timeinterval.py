from typing import Callable, NamedTuple, Any, Optional
from datetime import timedelta


from rx import operators as ops
from rx.core import Observable, typing
from rx.scheduler import timeout_scheduler


class TimeInterval(NamedTuple):
    value: Any
    interval: timedelta


def _time_interval(scheduler: Optional[typing.Scheduler] = None
                   ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def time_interval(source: Observable) -> Observable:
        """Records the time interval between consecutive values in an
        observable sequence.

            >>> res = time_interval(source)

        Return:
            An observable sequence with time interval information on
            values.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler
            last = sub_scheduler.now

            def mapper(value):
                nonlocal last

                now = sub_scheduler.now
                span = now - last
                last = now
                return TimeInterval(value=value, interval=span)

            return source.pipe(ops.map(mapper)).subscribe(
                on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return time_interval
