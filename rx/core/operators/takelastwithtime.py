from collections import deque
from typing import Callable, Optional

from rx.core import Observable, typing
from rx.scheduler import timeout_scheduler


def _take_last_with_time(duration: typing.RelativeTime,
                         scheduler: Optional[typing.Scheduler] = None
                         ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def take_last_with_time(source: Observable) -> Observable:
        """Returns elements within the specified duration from the end
        of the observable source sequence.

        Example:
            >>> res = take_last_with_time(source)

        This operator accumulates a queue with a length enough to store
        elements received during the initial duration window. As more
        elements are received, elements older than the specified
        duration are taken from the queue and produced on the result
        sequence. This causes elements to be delayed with duration.

        Args:
            duration: Duration for taking elements from the end of the
            sequence.

        Returns:
            An observable sequence with the elements taken during the
            specified duration from the end of the source sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            nonlocal duration

            sub_scheduler = op_scheduler or scheduler or timeout_scheduler
            duration = sub_scheduler.to_timedelta(duration)
            q = deque()

            def _on_next(x):
                now = sub_scheduler.now
                q.append({"interval": now, "value": x})
                while q and now - q[0]["interval"] >= duration:
                    q.popleft()

            def _on_completed():
                now = sub_scheduler.now
                while q:
                    _next = q.popleft()
                    if now - _next["interval"] <= duration \
                            and on_next is not None:
                        on_next(_next["value"])

                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return take_last_with_time
