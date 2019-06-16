from typing import Callable, Optional
from rx.core import Observable, typing


def _to_iterable() -> Callable[[Observable], Observable]:
    def to_iterable(source: Observable) -> Observable:
        """Creates an iterable from an observable sequence.

        Returns:
            An observable sequence containing a single element with an
            iterable containing all the elements of the source
            sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            nonlocal source

            queue = []

            def _on_next(item):
                queue.append(item)

            def _on_completed():
                if on_next is not None:
                    on_next(queue)
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return to_iterable
