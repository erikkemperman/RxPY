from typing import Callable, Optional

from rx.core import Observable, typing


def _pairwise() -> Callable[[Observable], Observable]:
    def pairwise(source: Observable) -> Observable:
        """Partially applied pairwise operator.

        Returns a new observable that triggers on the second and
        subsequent triggerings of the input observable. The Nth
        triggering of the input observable passes the arguments from
        the N-1th and Nth triggering as a pair. The argument passed to
        the N-1th triggering is held in hidden internal state until the
        Nth triggering occurs.

        Returns:
            An observable that triggers on successive pairs of
            observations from the input observable as an array.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            has_previous = [False]
            previous = [None]

            def _on_next(x):
                pair = None

                with source.lock:
                    if has_previous[0]:
                        pair = (previous[0], x)
                    else:
                        has_previous[0] = True

                    previous[0] = x

                if pair and on_next is not None:
                    on_next(pair)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed
            )
        return Observable(subscribe)
    return pairwise
