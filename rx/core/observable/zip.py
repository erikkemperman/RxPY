from collections import deque
from typing import Deque, List, Optional

from rx import from_future
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable
from rx.internal.utils import is_future

# pylint: disable=redefined-builtin

def _zip(*args: Observable) -> Observable:
    """Merges the specified observable sequences into one observable
    sequence by creating a tuple whenever all of the
    observable sequences have produced an element at a corresponding
    index.

    Example:
        >>> res = zip(obs1, obs2)

    Args:
        args: Observable sources to zip.

    Returns:
        An observable sequence containing the result of combining
        elements of the sources as tuple.
    """

    sources = list(args)

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        n = len(sources)
        queues : List[Deque] = [deque() for _ in range(n)]
        is_done = [False] * n

        def _on_next(i):
            if all([len(q) for q in queues]):
                try:
                    queued_values = [x.popleft() for x in queues]
                    res = tuple(queued_values)
                except Exception as ex:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(ex)
                    return

                if on_next is not None:
                    on_next(res)
            elif on_completed is not None \
                    and all([x for j, x in enumerate(is_done) if j != i]):
                on_completed()

        def _done(i):
            is_done[i] = True
            if on_completed is not None and all(is_done):
                on_completed()

        subscriptions = [None] * n

        def func(i):
            source = sources[i]
            sad = SingleAssignmentDisposable()
            source = from_future(source) if is_future(source) else source

            def _next(x):
                queues[i].append(x)
                _on_next(i)

            sad.disposable = source.subscribe(
                _next,
                on_error,
                lambda: _done(i),
                scheduler=scheduler
            )
            subscriptions[i] = sad
        for idx in range(n):
            func(idx)
        return CompositeDisposable(subscriptions)
    return Observable(subscribe)
