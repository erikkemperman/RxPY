import logging
from collections import deque
from typing import Callable, Optional

from rx.core import Observable, typing
from rx.internal.utils import add_ref
from rx.disposable import SingleAssignmentDisposable, RefCountDisposable
from rx.internal.exceptions import ArgumentOutOfRangeException
from rx.subject import Subject

log = logging.getLogger("Rx")


def _window_with_count(count: int, skip: Optional[int] = None) -> Callable[[Observable], Observable]:
    """Projects each element of an observable sequence into zero or more
    windows which are produced based on element count information.

    Examples:
        >>> window_with_count(10)
        >>> window_with_count(10, 1)

    Args:
        count: Length of each window.
        skip: [Optional] Number of elements to skip between creation of
            consecutive windows. If not specified, defaults to the
            count.

    Returns:
        An observable sequence of windows.
    """

    if count <= 0:
        raise ArgumentOutOfRangeException()

    if skip is None:
        skip = count

    if skip <= 0:
        raise ArgumentOutOfRangeException()

    def window_with_count(source: Observable) -> Observable:

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            m = SingleAssignmentDisposable()
            ref_count_disposable = RefCountDisposable(m)
            n = [0]
            q = deque()

            def create_window():
                s = Subject()
                q.append(s)
                ref = add_ref(s, ref_count_disposable)
                if on_next is not None:
                    on_next(ref)

            create_window()

            def _on_next(x):
                for item in q:
                    item.on_next(x)

                c = n[0] - count + 1
                if c >= 0 and c % skip == 0:
                    s = q.popleft()
                    s.on_completed()

                n[0] += 1
                if (n[0] % skip) == 0:
                    create_window()

            def _on_error(exception):
                while q:
                    q.popleft().on_error(exception)
                if on_error is not None:
                    on_error(exception)

            def _on_completed():
                while q:
                    q.popleft().on_completed()
                if on_completed is not None:
                    on_completed()

            m.disposable = source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
            return ref_count_disposable
        return Observable(subscribe)
    return window_with_count
