from collections import deque
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from rx import operators as ops
from rx.core import Observable, typing
from rx.internal.constants import DELTA_ZERO
from rx.disposable import CompositeDisposable, SerialDisposable, MultipleAssignmentDisposable
from rx.scheduler import timeout_scheduler


class Timestamp(object):
    def __init__(self, value, timestamp):
        self.value = value
        self.timestamp = timestamp


def observable_delay_timespan(source: Observable,
                              duetime: typing.RelativeTime,
                              scheduler: Optional[typing.Scheduler] = None
                              ) -> Observable:
    op_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        nonlocal duetime

        sub_scheduler = op_scheduler or scheduler or timeout_scheduler

        if isinstance(duetime, datetime):
            duetime = sub_scheduler.to_datetime(duetime) - sub_scheduler.now
        else:
            duetime = sub_scheduler.to_timedelta(duetime)

        cancelable = SerialDisposable()
        exception = [None]
        active = [False]
        running = [False]
        queue = deque()

        def _on_next(notification):
            should_run = False

            with source.lock:
                if notification.value.kind == 'E':
                    queue.clear()
                    queue.append(notification)
                    exception[0] = notification.value.exception
                    should_run = not running[0]
                else:
                    stamp = notification.timestamp + duetime
                    queue.append(Timestamp(value=notification.value,
                                           timestamp=stamp))
                    should_run = not active[0]
                    active[0] = True

            if should_run:
                if exception[0]:
                    if on_error is not None:
                        on_error(exception[0])
                else:
                    mad = MultipleAssignmentDisposable()
                    cancelable.disposable = mad

                    def action(act_scheduler: typing.Scheduler, __: Any = None) -> None:
                        if exception[0]:
                            return

                        with source.lock:
                            running[0] = True
                            while True:
                                result = None
                                if queue and queue[0].timestamp <= act_scheduler.now:
                                    result = queue.popleft().value

                                if result:
                                    result.accept(on_next, on_error, on_completed)

                                if not result:
                                    break

                            should_continue = False
                            recurse_duetime = 0
                            if queue:
                                should_continue = True
                                diff = queue[0].timestamp - act_scheduler.now
                                zero = DELTA_ZERO if isinstance(diff, timedelta) else 0
                                recurse_duetime = max(zero, diff)
                            else:
                                active[0] = False

                            ex = exception[0]
                            running[0] = False

                        if ex:
                            if on_error is not None:
                                on_error(ex)
                        elif should_continue:
                            mad.disposable = act_scheduler.schedule_relative(recurse_duetime, action)

                    mad.disposable = sub_scheduler.schedule_relative(duetime, action)
        subscription = source.pipe(
            ops.materialize(),
            ops.timestamp()
        ).subscribe(_on_next, scheduler=scheduler)

        return CompositeDisposable(subscription, cancelable)
    return Observable(subscribe)


def _delay(duetime: typing.RelativeTime,
           scheduler: Optional[typing.Scheduler] = None
           ) -> Callable[[Observable], Observable]:
    def delay(source: Observable) -> Observable:
        """Time shifts the observable sequence.

        A partially applied delay operator function.

        Examples:
            >>> res = delay(source)

        Args:
            source: The observable sequence to delay.

        Returns:
            A time-shifted observable sequence.
        """
        return observable_delay_timespan(source, duetime, scheduler)
    return delay
