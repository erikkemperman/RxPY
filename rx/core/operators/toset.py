from typing import Callable, Optional
from rx.core import Observable, typing


def _to_set() -> Callable[[Observable], Observable]:
    """Converts the observable sequence to a set.

    Returns an observable sequence with a single value of a set
    containing the values from the observable sequence.
    """

    def to_set(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            s = set()

            def _on_completed():
                if on_next is not None:
                    on_next(s)
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                s.add,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return to_set
