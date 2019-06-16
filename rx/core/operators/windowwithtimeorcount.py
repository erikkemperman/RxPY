from typing import Any, Callable, Optional

from rx.core import Observable, typing
from rx.scheduler import timeout_scheduler
from rx.internal.utils import add_ref
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable, RefCountDisposable, SerialDisposable
from rx.subject import Subject


def _window_with_time_or_count(timespan: typing.RelativeTime,
                               count: int,
                               scheduler: Optional[typing.Scheduler] = None
                               ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def window_with_time_or_count(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler

            n = [0]
            s = [None]
            timer_d = SerialDisposable()
            window_id = [0]
            group_disposable = CompositeDisposable(timer_d)
            ref_count_disposable = RefCountDisposable(group_disposable)

            def create_timer(_id):
                m = SingleAssignmentDisposable()
                timer_d.disposable = m

                def action(_: typing.Scheduler, __: Any) -> None:
                    if _id != window_id[0]:
                        return

                    n[0] = 0
                    window_id[0] += 1
                    new_id = window_id[0]
                    s[0].on_completed()
                    s[0] = Subject()
                    ref = add_ref(s[0], ref_count_disposable)
                    if on_next is not None:
                        on_next(ref)
                    create_timer(new_id)

                m.disposable = sub_scheduler.schedule_relative(timespan, action)

            s[0] = Subject()
            ref = add_ref(s[0], ref_count_disposable)
            if on_next is not None:
                on_next(ref)
            create_timer(0)

            def _on_next(x):
                new_window = False
                new_id = 0

                s[0].on_next(x)
                n[0] += 1
                if n[0] == count:
                    new_window = True
                    n[0] = 0
                    window_id[0] += 1
                    new_id = window_id[0]
                    s[0].on_completed()
                    s[0] = Subject()
                    ref = add_ref(s[0], ref_count_disposable)
                    if on_next is not None:
                        on_next(ref)

                if new_window:
                    create_timer(new_id)

            def _on_error(e):
                s[0].on_error(e)
                if on_error is not None:
                    on_error(e)

            def _on_completed():
                s[0].on_completed()
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
    return window_with_time_or_count
