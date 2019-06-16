from typing import Callable, Optional

from rx import operators as ops
from rx.core import Observable, typing
from rx.core.typing import Predicate


def _some(predicate: Optional[Predicate] = None) -> Callable[[Observable], Observable]:
    def some(source: Observable) -> Observable:
        """Partially applied operator.

        Determines whether some element of an observable sequence satisfies a
        condition if present, else if some items are in the sequence.

        Example:
            >>> obs = some(source)

        Args:
            predicate -- A function to test each element for a condition.

        Returns:
            An observable sequence containing a single element
            determining whether some elements in the source sequence
            pass the test in the specified predicate if given, else if
            some items are in the sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:

            def _on_next(_):
                if on_next is not None:
                    on_next(True)
                if on_completed is not None:
                    on_completed()

            def _on_error():
                if on_next is not None:
                    on_next(False)
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_error,
                scheduler=scheduler
            )

        if predicate:
            return source.pipe(
                ops.filter(predicate),
                _some(),
            )

        return Observable(subscribe)
    return some
