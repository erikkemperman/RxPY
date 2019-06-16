from typing import Callable, Optional

from rx.core import Observable, typing


def _dematerialize() -> Callable[[Observable], Observable]:
    def dematerialize(source: Observable) -> Observable:
        """Partially applied dematerialize operator.

        Dematerializes the explicit notification values of an
        observable sequence as implicit notifications.

        Returns:
            An observable sequence exhibiting the behavior
            corresponding to the source sequence's notification values.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            def _on_next(value):
                return value.accept(on_next, on_error, on_completed)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return dematerialize
