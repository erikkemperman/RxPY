from typing import Any, Optional

from rx import from_future
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable
from rx.internal.utils import is_future


def _amb(right_source: Observable):
    right_source = from_future(right_source) if is_future(right_source) else right_source

    def amb(left_source: Observable):
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            choice = [None]
            left_choice = 'L'
            right_choice = 'R'
            left_subscription = SingleAssignmentDisposable()
            right_subscription = SingleAssignmentDisposable()

            def choice_left():
                if not choice[0]:
                    choice[0] = left_choice
                    right_subscription.dispose()

            def choice_right():
                if not choice[0]:
                    choice[0] = right_choice
                    left_subscription.dispose()

            def on_next_left(value):
                with left_source.lock:
                    choice_left()
                if choice[0] == left_choice and on_next is not None:
                    on_next(value)

            def on_error_left(err):
                with left_source.lock:
                    choice_left()
                if choice[0] == left_choice and on_error is not None:
                    on_error(err)

            def on_completed_left():
                with left_source.lock:
                    choice_left()
                if choice[0] == left_choice and on_completed is not None:
                    on_completed()

            left_d = left_source.subscribe(
                on_next_left,
                on_error_left,
                on_completed_left,
                scheduler=scheduler
            )
            left_subscription.disposable = left_d

            def send_right(value: Any) -> None:
                with left_source.lock:
                    choice_right()
                if choice[0] == right_choice and on_next is not None:
                    on_next(value)

            def on_error_right(err: Exception) -> None:
                with left_source.lock:
                    choice_right()
                if choice[0] == right_choice and on_error is not None:
                    on_error(err)

            def on_completed_right() -> None:
                with left_source.lock:
                    choice_right()
                if choice[0] == right_choice and on_completed is not None:
                    on_completed()

            right_d = right_source.subscribe(
                send_right,
                on_error_right,
                on_completed_right,
                scheduler=scheduler
            )
            right_subscription.disposable = right_d
            return CompositeDisposable(left_subscription, right_subscription)
        return Observable(subscribe)
    return amb
