from typing import Callable, Iterable, Optional

import rx
from rx.core import Observable, typing


# pylint: disable=redefined-builtin
def _zip(*args: Observable) -> Callable[[Observable], Observable]:
    def zip(source: Observable) -> Observable:
        """Merges the specified observable sequences into one observable
        sequence by creating a tuple whenever all of the
        observable sequences have produced an element at a corresponding
        index.

        Example:
            >>> res = zip(source)

        Args:
            source: Source observable to zip.

        Returns:
            An observable sequence containing the result of combining
            elements of the sources as a tuple.
        """
        return rx.zip(source, *args)
    return zip


def _zip_with_iterable(seq: Iterable) -> Callable[[Observable], Observable]:
    def zip_with_iterable(source: Observable) -> Observable:
        """Merges the specified observable sequence and list into one
        observable sequence by creating a tuple whenever all of
        the observable sequences have produced an element at a
        corresponding index.

        Example
            >>> res = zip(source)

        Args:
            source: Source observable to zip.

        Returns:
            An observable sequence containing the result of combining
            elements of the sources as a tuple.
        """

        first = source
        second = iter(seq)

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            def _on_next(left):
                try:
                    right = next(second)
                except StopIteration:
                    if on_completed is not None:
                        on_completed()
                else:
                    result = (left, right)
                    if on_next is not None:
                        on_next(result)

            return first.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return zip_with_iterable
