from typing import Callable, Optional

from rx.core import Observable, typing


def _as_observable() -> Callable[[Observable], Observable]:
    def as_observable(source: Observable) -> Observable:
        """Hides the identity of an observable sequence.

        Args:
            source: Observable source to hide identity from.

        Returns:
            An observable sequence that hides the identity of the
            source sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            return source.subscribe(on_next, on_error, on_completed,
                                    scheduler=scheduler)

        return Observable(subscribe)
    return as_observable
