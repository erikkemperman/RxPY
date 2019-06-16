from collections import deque
from typing import Callable, Optional

from rx.core import Observable, typing


def _take_last_buffer(count: int) -> Callable[[Observable], Observable]:
    def take_last_buffer(source: Observable) -> Observable:
        """Returns an array with the specified number of contiguous
        elements from the end of an observable sequence.

        Example:
            >>> res = take_last(source)

        This operator accumulates a buffer with a length enough to
        store elements count elements. Upon completion of the source
        sequence, this buffer is drained on the result sequence. This
        causes the elements to be delayed.

        Args:
            source: Source observable to take elements from.

        Returns:
            An observable sequence containing a single list with the
            specified number of elements from the end of the source
            sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            q = deque()

            def _on_next(x):
                with source.lock:
                    q.append(x)
                    if len(q) > count:
                        q.popleft()

            def _on_completed():
                if on_next is not None:
                    on_next(list(q))
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return take_last_buffer
