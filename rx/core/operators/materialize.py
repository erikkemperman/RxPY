from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.notification import OnNext, OnError, OnCompleted


def _materialize() -> Callable[[Observable], Observable]:
    def materialize(source: Observable) -> Observable:
        """Partially applied materialize operator.

        Materializes the implicit notifications of an observable
        sequence as explicit notification values.

        Args:
            source: Source observable to materialize.

        Returns:
            An observable sequence containing the materialized
            notification values from the source sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            def _on_next(value):
                if on_next is not None:
                    on_next(OnNext(value))

            def _on_error(exception):
                if on_next is not None:
                    on_next(OnError(exception))
                if on_completed is not None:
                    on_completed()

            def _on_completed():
                if on_next is not None:
                    on_next(OnCompleted())
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return materialize
