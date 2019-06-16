from typing import Any, Callable, Optional

from rx import from_future
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable, SerialDisposable
from rx.internal.utils import is_future


def _switch_latest() -> Callable[[Observable], Observable]:
    def switch_latest(source: Observable) -> Observable:
        """Partially applied switch_latest operator.

        Transforms an observable sequence of observable sequences into
        an observable sequence producing values only from the most
        recent observable sequence.

        Returns:
            An observable sequence that at any point in time produces
            the elements of the most recent inner observable sequence
            that has been received.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            inner_subscription = SerialDisposable()
            has_latest = [False]
            is_stopped = [False]
            latest = [0]

            def _on_next(inner_source: Observable):
                nonlocal source

                d = SingleAssignmentDisposable()
                with source.lock:
                    latest[0] += 1
                    _id = latest[0]
                has_latest[0] = True
                inner_subscription.disposable = d

                # Check if Future or Observable
                inner_source = from_future(inner_source) if is_future(inner_source) else inner_source

                def _next(x: Any) -> None:
                    if latest[0] == _id and on_next is not None:
                        on_next(x)

                def _error(e: Exception) -> None:
                    if latest[0] == _id and on_error is not None:
                        on_error(e)

                def _completed() -> None:
                    if latest[0] == _id:
                        has_latest[0] = False
                        if is_stopped[0] and on_completed is not None:
                            on_completed()

                d.disposable = inner_source.subscribe(
                    _next,
                    _error,
                    _completed,
                    scheduler=scheduler
                )

            def _on_completed() -> None:
                is_stopped[0] = True
                if not has_latest[0] and on_completed is not None:
                    on_completed()

            subscription = source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
            return CompositeDisposable(subscription, inner_subscription)
        return Observable(subscribe)
    return switch_latest
