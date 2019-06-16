from typing import Callable, Optional

import rx
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable
from rx.internal.utils import is_future


def _skip_until(other: Observable) -> Callable[[Observable], Observable]:
    """Returns the values from the source observable sequence only after
    the other observable sequence produces a value.

    Args:
        other: The observable sequence that triggers propagation of
            elements of the source sequence.

    Returns:
        An observable sequence containing the elements of the source
    sequence starting from the point the other sequence triggered
    propagation.
    """

    other = rx.from_future(other) if is_future(other) else other

    def skip_until(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            is_open = [False]

            def on_next1(left):
                if is_open[0] and on_next is not None:
                    on_next(left)

            def on_completed1():
                if is_open[0] and on_completed is not None:
                    on_completed()

            subs = source.subscribe(
                on_next1,
                on_error,
                on_completed1,
                scheduler=scheduler
            )
            subscriptions = CompositeDisposable(subs)

            right_subscription = SingleAssignmentDisposable()
            subscriptions.add(right_subscription)

            def on_next2(x):
                is_open[0] = True
                right_subscription.dispose()

            def on_completed2():
                right_subscription.dispose()

            right_subscription.disposable = other.subscribe(
                on_next2,
                on_error,
                on_completed2,
                scheduler=scheduler
            )

            return subscriptions
        return Observable(subscribe)
    return skip_until
