from typing import Callable, Optional

from rx.core import Observable, typing
from rx.internal import ArgumentOutOfRangeException


def _skip(count: int) -> Callable[[Observable], Observable]:
    if count < 0:
        raise ArgumentOutOfRangeException()

    def skip(source: Observable) -> Observable:
        """The skip operator.

        Bypasses a specified number of elements in an observable sequence
        and then returns the remaining elements.

        Args:
            source: The source observable.

        Returns:
            An observable sequence that contains the elements that occur
            after the specified index in the input sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            remaining = count

            def _on_next(value):
                nonlocal remaining

                if remaining > 0:
                    remaining -= 1
                elif on_next is not None:
                    on_next(value)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return skip
