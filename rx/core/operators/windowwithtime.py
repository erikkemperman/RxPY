from collections import deque
from datetime import timedelta
from typing import Any, Callable, Optional

from rx.core import Observable, typing
from rx.scheduler import timeout_scheduler
from rx.internal.constants import DELTA_ZERO
from rx.internal.utils import add_ref
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable, RefCountDisposable, SerialDisposable
from rx.subject import Subject


def _window_with_time(timespan: typing.RelativeTime,
                      timeshift: Optional[typing.RelativeTime] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    if timeshift is None:
        timeshift = timespan

    if not isinstance(timespan, timedelta):
        timespan = timedelta(seconds=timespan)
    if not isinstance(timeshift, timedelta):
        timeshift = timedelta(seconds=timeshift)

    def window_with_time(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler

            timer_d = SerialDisposable()
            next_shift = [timeshift]
            next_span = [timespan]
            total_time = [DELTA_ZERO]
            q = deque()

            group_disposable = CompositeDisposable(timer_d)
            ref_count_disposable = RefCountDisposable(group_disposable)

            def create_timer():
                m = SingleAssignmentDisposable()
                timer_d.disposable = m
                is_span = False
                is_shift = False

                if next_span[0] == next_shift[0]:
                    is_span = True
                    is_shift = True
                elif next_span[0] < next_shift[0]:
                    is_span = True
                else:
                    is_shift = True

                new_total_time = next_span[0] if is_span else next_shift[0]

                ts = new_total_time - total_time[0]
                total_time[0] = new_total_time
                if is_span:
                    next_span[0] += timeshift

                if is_shift:
                    next_shift[0] += timeshift

                def action(_: typing.Scheduler, __: Any = None) -> None:
                    s = None

                    if is_shift:
                        s = Subject()
                        q.append(s)
                        ref = add_ref(s, ref_count_disposable)
                        if on_next is not None:
                            on_next(ref)

                    if is_span:
                        s = q.popleft()
                        s.on_completed()

                    create_timer()
                m.disposable = sub_scheduler.schedule_relative(ts, action)

            q.append(Subject())
            ref = add_ref(q[0], ref_count_disposable)
            if on_next is not None:
                on_next(ref)
            create_timer()

            def _on_next(x):
                for s in q:
                    s.on_next(x)

            def _on_error(e):
                for s in q:
                    s.on_error(e)

                if on_error is not None:
                    on_error(e)

            def _on_completed():
                for s in q:
                    s.on_completed()

                if on_completed is not None:
                    on_completed()

            group_disposable.add(source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            ))
            return ref_count_disposable
        return Observable(subscribe)
    return window_with_time
