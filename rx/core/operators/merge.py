from collections import deque
from typing import Callable, Optional

import rx
from rx import from_future
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable
from rx.internal.concurrency import synchronized
from rx.internal.utils import is_future


def _merge(*sources: Observable,
           max_concurrent: Optional[int] = None
           ) -> Callable[[Observable], Observable]:

    def merge(source: Observable) -> Observable:
        """Merges an observable sequence of observable sequences into
        an observable sequence, limiting the number of concurrent
        subscriptions to inner sequences. Or merges two observable
        sequences into a single observable sequence.

        Examples:
            >>> res = merge(sources)

        Args:
            source: Source observable.

        Returns:
            The observable sequence that merges the elements of the
            inner sequences.
        """

        if max_concurrent is None:
            sources_ = tuple([source]) + sources
            return rx.merge(*sources_)

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            active_count = [0]
            group = CompositeDisposable()
            is_stopped = [False]
            queue = deque()

            def _subscribe(xs):
                subscription = SingleAssignmentDisposable()
                group.add(subscription)

                @synchronized(source.lock)
                def _completed():
                    group.remove(subscription)
                    if queue:
                        s = queue.popleft()
                        _subscribe(s)
                    else:
                        active_count[0] -= 1
                        if is_stopped[0] and active_count[0] == 0 \
                                and on_completed is not None:
                            on_completed()

                _next = None
                if on_next is not None:
                    _next = synchronized(source.lock)(on_next)
                _error = None
                if on_error is not None:
                    _error = synchronized(source.lock)(on_error)
                subscription.disposable = xs.subscribe(
                    _next,
                    _error,
                    _completed,
                    scheduler=scheduler
                )

            def _on_next(inner_source):
                if active_count[0] < max_concurrent:
                    active_count[0] += 1
                    _subscribe(inner_source)
                else:
                    queue.append(inner_source)

            def _on_completed():
                is_stopped[0] = True
                if active_count[0] == 0 and on_completed is not None:
                    on_completed()

            group.add(source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            ))
            return group
        return Observable(subscribe)
    return merge


def _merge_all() -> Callable[[Observable], Observable]:
    def merge_all(source: Observable) -> Observable:
        """Partially applied merge_all operator.

        Merges an observable sequence of observable sequences into an
        observable sequence.

        Args:
            source: Source observable to merge.

        Returns:
            The observable sequence that merges the elements of the inner
            sequences.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            group = CompositeDisposable()
            is_stopped = [False]
            m = SingleAssignmentDisposable()
            group.add(m)

            def _on_next(inner_source):
                inner_subscription = SingleAssignmentDisposable()
                group.add(inner_subscription)

                inner_source = from_future(inner_source) if is_future(inner_source) else inner_source

                @synchronized(source.lock)
                def _completed():
                    group.remove(inner_subscription)
                    if is_stopped[0] and len(group) == 1 \
                            and on_completed is not None:
                        on_completed()

                _next = None
                if on_next is not None:
                    _next = synchronized(source.lock)(on_next)
                _error = None
                if on_error is not None:
                    _error = synchronized(source.lock)(on_error)
                subscription = inner_source.subscribe(
                    _next,
                    _error,
                    _completed,
                    scheduler=scheduler
                )
                inner_subscription.disposable = subscription

            def _on_completed():
                is_stopped[0] = True
                if len(group) == 1 and on_completed is not None:
                    on_completed()

            m.disposable = source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
            return group

        return Observable(subscribe)
    return merge_all
