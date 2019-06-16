from typing import Any, Callable, Optional

from rx.core import Observable, typing
from rx.core.typing import Predicate, PredicateIndexed


def _take_while(predicate: Predicate, inclusive: bool = False) -> Callable[[Observable], Observable]:
    def take_while(source: Observable) -> Observable:
        """Returns elements from an observable sequence as long as a
        specified condition is true. The element's index is used in the
        logic of the predicate function.

        Example:
            >>> take_while(source)

        Args:
            source: The source observable to take from.

        Returns:
            An observable sequence that contains the elements from the
            input sequence that occur before the element at which the
            test no longer passes.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            running = True

            def _on_next(value):
                nonlocal running

                with source.lock:
                    if not running:
                        return

                    try:
                        running = predicate(value)
                    except Exception as exn:
                        if on_error is not None:
                            on_error(exn)
                        return

                if running:
                    if on_next is not None:
                        on_next(value)
                else:
                    if inclusive and on_next is not None:
                        on_next(value)
                    if on_completed is not None:
                        on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return take_while


def _take_while_indexed(predicate: PredicateIndexed, inclusive: bool = False) -> Callable[[Observable], Observable]:
    def take_while_indexed(source: Observable) -> Observable:
        """Returns elements from an observable sequence as long as a
        specified condition is true. The element's index is used in the
        logic of the predicate function.

        Example:
            >>> take_while(source)

        Args:
            source: Source observable to take from.

        Returns:
            An observable sequence that contains the elements from the
            input sequence that occur before the element at which the
            test no longer passes.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            running = True
            i = 0

            def _on_next(value: Any) -> None:
                nonlocal running, i

                with source.lock:
                    if not running:
                        return

                    try:
                        running = predicate(value, i)
                    except Exception as exn:
                        if on_error is not None:
                            on_error(exn)
                        return
                    else:
                        i += 1

                if running:
                    if on_next is not None:
                        on_next(value)
                else:
                    if inclusive and on_next is not None:
                        on_next(value)
                    if on_completed is not None:
                        on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return take_while_indexed
