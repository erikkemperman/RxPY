from datetime import datetime
from typing import Any, Callable, Optional

from rx import from_future, throw
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable, SerialDisposable
from rx.scheduler import timeout_scheduler
from rx.internal.utils import is_future


def _timeout(duetime: typing.AbsoluteTime,
             other: Optional[Observable] = None,
             scheduler: Optional[typing.Scheduler] = None
             ) -> Callable[[Observable], Observable]:

    other = other or throw(Exception("Timeout"))
    other = from_future(other) if is_future(other) else other
    op_scheduler = scheduler

    def timeout(source: Observable) -> Observable:
        """Returns the source observable sequence or the other observable
        sequence if duetime elapses.

        Examples:
            >>> res = timeout(source)

        Args:
            source: Source observable to timeout

        Returns:
            An obserable sequence switching to the other sequence in
            case of a timeout.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler

            if isinstance(duetime, datetime):
                scheduler_method = sub_scheduler.schedule_absolute
            else:
                scheduler_method = sub_scheduler.schedule_relative

            switched = [False]
            _id = [0]

            original = SingleAssignmentDisposable()
            subscription = SerialDisposable()
            timer = SerialDisposable()
            subscription.disposable = original

            def create_timer():
                my_id = _id[0]

                def action(act_scheduler: typing.Scheduler, __: Any = None) -> None:
                    switched[0] = (_id[0] == my_id)
                    timer_wins = switched[0]
                    if timer_wins:
                        subscription.disposable = other.subscribe(
                            on_next,
                            on_error,
                            on_completed,
                            scheduler=act_scheduler
                        )

                timer.disposable = scheduler_method(duetime, action)

            create_timer()

            def _on_next(value):
                send_wins = not switched[0]
                if send_wins:
                    _id[0] += 1
                    if on_next is not None:
                        on_next(value)
                    create_timer()

            def _on_error(error):
                on_error_wins = not switched[0]
                if on_error_wins:
                    _id[0] += 1
                    if on_error is not None:
                        on_error(error)

            def _on_completed():
                on_completed_wins = not switched[0]
                if on_completed_wins:
                    _id[0] += 1
                    if on_completed is not None:
                        on_completed()

            original.disposable = source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
            return CompositeDisposable(subscription, timer)
        return Observable(subscribe)
    return timeout
