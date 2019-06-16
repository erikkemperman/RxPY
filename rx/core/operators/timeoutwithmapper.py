from typing import Callable, Optional, Any

import rx
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable, SerialDisposable


def _timeout_with_mapper(first_timeout: Optional[Observable] = None,
                         timeout_duration_mapper: Optional[Callable[[Any], Observable]] = None,
                         other: Optional[Observable] = None
                         ) -> Callable[[Observable], Observable]:
    """Returns the source observable sequence, switching to the other
    observable sequence if a timeout is signaled.

        res = timeout_with_mapper(rx.timer(500))
        res = timeout_with_mapper(rx.timer(500), lambda x: rx.timer(200))
        res = timeout_with_mapper(rx.timer(500), lambda x: rx.timer(200)), rx.return_value(42))

    Args:
        first_timeout -- [Optional] Observable sequence that represents the
            timeout for the first element. If not provided, this defaults to
            rx.never().
        timeout_duration_mapper -- [Optional] Selector to retrieve an
            observable sequence that represents the timeout between the
            current element and the next element.
        other -- [Optional] Sequence to return in case of a timeout. If not
            provided, this is set to rx.throw().

    Returns:
        The source sequence switching to the other sequence in case
    of a timeout.
    """

    first_timeout = first_timeout or rx.never()
    other = other or rx.throw(Exception('Timeout'))

    def timeout_with_mapper(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            subscription = SerialDisposable()
            timer = SerialDisposable()
            original = SingleAssignmentDisposable()

            subscription.disposable = original

            switched = False
            _id = [0]

            def set_timer(timeout: Observable) -> None:
                my_id = _id[0]

                def timer_wins():
                    return _id[0] == my_id

                d = SingleAssignmentDisposable()
                timer.disposable = d

                def _next(x):
                    if timer_wins():
                        subscription.disposable = other.subscribe(
                            on_next,
                            on_error,
                            on_completed,
                            scheduler=scheduler
                        )

                    d.dispose()

                def _error(e):
                    if timer_wins() and on_error is not None:
                        on_error(e)

                def _completed():
                    if timer_wins():
                        subscription.disposable = other.subscribe(
                            on_next,
                            on_error,
                            on_completed
                        )

                d.disposable = timeout.subscribe(
                    _next,
                    _error,
                    _completed,
                    scheduler=scheduler
                )

            set_timer(first_timeout)

            def observer_wins():
                res = not switched
                if res:
                    _id[0] += 1

                return res

            def _on_next(x):
                if observer_wins():
                    if on_next is not None:
                        on_next(x)
                    try:
                        timeout = timeout_duration_mapper(x)
                    except Exception as e:
                        if on_error is not None:
                            on_error(e)
                        return

                    set_timer(timeout)

            def _on_error(error):
                if observer_wins() and on_error is not None:
                    on_error(error)

            def _on_completed():
                if observer_wins() and on_completed is not None:
                    on_completed()

            original.disposable = source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
            return CompositeDisposable(subscription, timer)
        return Observable(subscribe)
    return timeout_with_mapper
