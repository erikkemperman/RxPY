from typing import Callable, Optional

from rx import operators as ops
from rx.core import Observable, typing, pipe


def _skip_while(predicate: typing.Predicate) -> Callable[[Observable], Observable]:
    def skip_while(source: Observable) -> Observable:
        """Bypasses elements in an observable sequence as long as a
        specified condition is true and then returns the remaining
        elements. The element's index is used in the logic of the
        predicate function.

        Example:
            >>> skip_while(source)

        Args:
            source: The source observable to skip elements from.

        Returns:
            An observable sequence that contains the elements from the
            input sequence starting at the first element in the linear
            series that does not pass the test specified by predicate.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            running = False

            def _on_next(value):
                nonlocal running

                if not running:
                    try:
                        running = not predicate(value)
                    except Exception as exn:
                        if on_error is not None:
                            on_error(exn)
                        return

                if running and on_next is not None:
                    on_next(value)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return skip_while


def _skip_while_indexed(predicate: typing.PredicateIndexed) -> Callable[[Observable], Observable]:
    return pipe(
        ops.map_indexed(lambda x, i: (x, i)),
        ops.skip_while(lambda x: predicate(*x)),
        ops.map(lambda x: x[0])
    )
