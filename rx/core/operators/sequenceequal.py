from collections import deque
from collections.abc import Iterable
from typing import Callable, Optional

import rx
from rx.core import Observable, typing
from rx.core.typing import Comparer
from rx.disposable import CompositeDisposable
from rx.internal import default_comparer


def _sequence_equal(second: Observable, comparer: Optional[Comparer] = None
                    ) -> Callable[[Observable], Observable]:
    comparer = comparer or default_comparer
    if isinstance(second, Iterable):
        second = rx.from_iterable(second)

    def sequence_equal(source: Observable) -> Observable:
        """Determines whether two sequences are equal by comparing the
        elements pairwise using a specified equality comparer.

        Examples:
            >>> res = sequence_equal([1,2,3])
            >>> res = sequence_equal([{ "value": 42 }], lambda x, y: x.value == y.value)
            >>> res = sequence_equal(rx.return_value(42))
            >>> res = sequence_equal(rx.return_value({ "value": 42 }), lambda x, y: x.value == y.value)

        Args:
            source: Source obserable to compare.

        Returns:
            An observable sequence that contains a single element which
        indicates whether both sequences are of equal length and their
        corresponding elements are equal according to the specified
        equality comparer.
        """
        first = source

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            donel = [False]
            doner = [False]
            ql = deque()
            qr = deque()

            def on_next1(x):
                if len(qr) > 0:
                    v = qr.popleft()
                    try:
                        equal = comparer(v, x)
                    except Exception as e:
                        if on_error is not None:
                            on_error(e)
                        return

                    if not equal:
                        if on_next is not None:
                            on_next(False)
                        if on_completed is not None:
                            on_completed()

                elif doner[0]:
                    if on_next is not None:
                        on_next(False)
                    if on_completed is not None:
                        on_completed()
                else:
                    ql.append(x)

            def on_completed1():
                donel[0] = True
                if not ql:
                    if qr:
                        if on_next is not None:
                            on_next(False)
                        if on_completed is not None:
                            on_completed()
                    elif doner[0]:
                        if on_next is not None:
                            on_next(True)
                        if on_completed is not None:
                            on_completed()

            def on_next2(x):
                if len(ql) > 0:
                    v = ql.popleft()
                    try:
                        equal = comparer(v, x)
                    except Exception as exception:
                        if on_error is not None:
                            on_error(exception)
                        return

                    if not equal:
                        if on_next is not None:
                            on_next(False)
                        if on_completed is not None:
                            on_completed()

                elif donel[0]:
                    if on_next is not None:
                        on_next(False)
                    if on_completed is not None:
                        on_completed()
                else:
                    qr.append(x)

            def on_completed2():
                doner[0] = True
                if not qr:
                    if len(ql) > 0:
                        if on_next is not None:
                            on_next(False)
                        if on_completed is not None:
                            on_completed()
                    elif donel[0]:
                        if on_next is not None:
                            on_next(True)
                        if on_completed is not None:
                            on_completed()

            subscription1 = first.subscribe(
                on_next1,
                on_error,
                on_completed1,
                scheduler=scheduler
            )
            subscription2 = second.subscribe(
                on_next2,
                on_error,
                on_completed2,
                scheduler=scheduler
            )
            return CompositeDisposable(subscription1, subscription2)
        return Observable(subscribe)
    return sequence_equal
