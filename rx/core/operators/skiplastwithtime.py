from typing import Union, Callable, Optional

from rx.core import Observable, typing
from rx.scheduler import timeout_scheduler


def _skip_last_with_time(duration: typing.RelativeTime, scheduler: Optional[typing.Scheduler] = None
                        ) -> Callable[[Observable], Observable]:
    """Skips elements for the specified duration from the end of the
    observable source sequence.

    Example:
        >>> res = skip_last_with_time(5.0)

    This operator accumulates a queue with a length enough to store
    elements received during the initial duration window. As more
    elements are received, elements older than the specified duration
    are taken from the queue and produced on the result sequence. This
    causes elements to be delayed with duration.

    Args:
        duration: Duration for skipping elements from the end of the
            sequence.
        scheduler: Scheduler to use for time handling.

    Returns:
        An observable sequence with the elements skipped during the
    specified duration from the end of the source sequence.
    """

    def skip_last_with_time(source: Observable) -> Observable:
        def subscribe_observer(observer: typing.Observer,
                               scheduler_: Optional[typing.Scheduler] = None
                               ) -> typing.Disposable:
            nonlocal duration

            _scheduler = scheduler or scheduler_ or timeout_scheduler
            duration = _scheduler.to_timedelta(duration)
            q = []

            def on_next(x):
                now = _scheduler.now
                q.append({"interval": now, "value": x})
                while q and now - q[0]["interval"] >= duration:
                    observer.on_next(q.pop(0)["value"])

            def on_completed():
                now = _scheduler.now
                while q and now - q[0]["interval"] >= duration:
                    observer.on_next(q.pop(0)["value"])

                observer.on_completed()

            return source.subscribe(
                on_next,
                observer.on_error,
                on_completed,
                scheduler=scheduler_
            )
        return Observable(subscribe_observer=subscribe_observer)
    return skip_last_with_time
