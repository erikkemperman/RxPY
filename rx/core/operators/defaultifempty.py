from typing import Any, Callable, Optional

from rx.core import Observable, typing


def _default_if_empty(default_value: Any = None) -> Callable[[Observable], Observable]:
    def default_if_empty(source: Observable) -> Observable:
        """Returns the elements of the specified sequence or the
        specified value in a singleton sequence if the sequence is
        empty.

        Examples:
            >>> obs = default_if_empty(source)

        Args:
            source: Source observable.

        Returns:
            An observable sequence that contains the specified default
            value if the source is empty otherwise, the elements of the
            source.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            found = [False]

            def _on_next(x: Any):
                found[0] = True
                if on_next is not None:
                    on_next(x)

            def _on_completed():
                if not found[0] and on_next is not None:
                    on_next(default_value)
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return default_if_empty
