from typing import Optional

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable


def _combine_latest(*sources: Observable) -> Observable:
    """Merges the specified observable sequences into one observable
    sequence by creating a tuple whenever any of the
    observable sequences produces an element.

    Examples:
        >>> obs = combine_latest(obs1, obs2, obs3)

    Returns:
        An observable sequence containing the result of combining
        elements of the sources into a tuple.
    """

    parent = sources[0]

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:

        n = len(sources)
        has_value = [False] * n
        has_value_all = [False]
        is_done = [False] * n
        values = [None] * n

        def _next(i):
            has_value[i] = True

            if has_value_all[0] or all(has_value):
                res = tuple(values)
                if on_next is not None:
                    on_next(res)

            elif all([x for j, x in enumerate(is_done) if j != i]):
                if on_completed is not None:
                    on_completed()

            has_value_all[0] = all(has_value)

        def _completed(i):
            is_done[i] = True
            if all(is_done) and on_completed is not None:
                on_completed()

        subscriptions = [None] * n

        def func(i):
            subscriptions[i] = SingleAssignmentDisposable()

            def _on_next(x):
                with parent.lock:
                    values[i] = x
                    _next(i)

            def _on_completed():
                with parent.lock:
                    _completed(i)

            subscriptions[i].disposable = sources[i].subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )

        for idx in range(n):
            func(idx)
        return CompositeDisposable(subscriptions)
    return Observable(subscribe)
