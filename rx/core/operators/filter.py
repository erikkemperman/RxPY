from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.typing import Predicate, PredicateIndexed, Scheduler, Observer, Disposable


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

        def subscribe_observer(observer: typing.Observer,
                               scheduler: Optional[typing.Scheduler] = None
                               ) -> typing.Disposable:
            def on_next(value):
                try:
                    should_run = predicate(value)
                except Exception as ex:  # pylint: disable=broad-except
                    observer.on_error(ex)
                    return

                if should_run:
                    observer.on_next(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe_observer=subscribe_observer)
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

        def subscribe_observer(observer: typing.Observer,
                               scheduler: Optional[typing.Scheduler] = None
                               ) -> typing.Disposable:
            count = 0

            def on_next(value):
                nonlocal count
                try:
                    should_run = predicate_indexed(value, count)
                except Exception as ex:  # pylint: disable=broad-except
                    observer.on_error(ex)
                    return
                else:
                    count += 1

                if should_run:
                    observer.on_next(value)

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe_observer=subscribe_observer)
    return filter_indexed
