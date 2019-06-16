from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.typing import Predicate, PredicateIndexed


# pylint: disable=redefined-builtin
def _filter(predicate: Predicate) -> Callable[[Observable], Observable]:
    def filter(source: Observable) -> Observable:
        """Partially applied filter operator.

        Filters the elements of an observable sequence based on a
        predicate.

        Example:
            >>> filter(source)

        Args:
            source: Source observable to filter.

        Returns:
            A filtered observable sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            def _on_next(value):
                try:
                    should_run = predicate(value)
                except Exception as ex:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(ex)
                    return

                if should_run:
                    if on_next is not None:
                        on_next(value)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return filter


def _filter_indexed(predicate_indexed: Optional[PredicateIndexed] = None) -> Callable[[Observable], Observable]:
    def filter_indexed(source: Observable) -> Observable:
        """Partially applied indexed filter operator.

        Filters the elements of an observable sequence based on a
        predicate by incorporating the element's index.

        Example:
            >>> filter_indexed(source)

        Args:
            source: Source observable to filter.

        Returns:
            A filtered observable sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            count = 0

            def _on_next(value):
                nonlocal count
                try:
                    should_run = predicate_indexed(value, count)
                except Exception as ex:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(ex)
                    return
                else:
                    count += 1

                if should_run and on_next is not None:
                    on_next(value)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return filter_indexed
