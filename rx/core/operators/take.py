from typing import Callable, Optional

from rx import empty
from rx.core import Observable, typing
from rx.internal import ArgumentOutOfRangeException


def _take(count: int) -> Callable[[Observable], Observable]:
    if count < 0:
        raise ArgumentOutOfRangeException()

    def take(source: Observable) -> Observable:
        """Returns a specified number of contiguous elements from the start of
        an observable sequence.

        >>> take(source)

        Keyword arguments:
        count -- The number of elements to return.

        Returns an observable sequence that contains the specified number of
        elements from the start of the input sequence.
        """

        if not count:
            return empty()

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
                    if on_next is not None:
                        on_next(value)
                    if not remaining and on_completed is not None:
                        on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return take
