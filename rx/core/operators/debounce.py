from typing import Any, Callable, Optional

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable, SerialDisposable
from rx.scheduler import timeout_scheduler


def _debounce(duetime: typing.RelativeTime,
              scheduler: Optional[typing.Scheduler] = None
              ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def debounce(source: Observable) -> Observable:
        """Ignores values from an observable sequence which are followed by
        another value before duetime.

        Example:
            >>> res = debounce(source)

        Args:
            source: Source observable to debounce.

        Returns:
            An operator function that takes the source observable and
            returns the debounced observable sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = op_scheduler or scheduler or timeout_scheduler
            cancelable = SerialDisposable()
            has_value = [False]
            value = [None]
            _id = [0]

            def _on_next(x: Any) -> None:
                has_value[0] = True
                value[0] = x
                _id[0] += 1
                current_id = _id[0]
                d = SingleAssignmentDisposable()
                cancelable.disposable = d

                def action(_: typing.Scheduler, __: Any = None) -> None:
                    if has_value[0] and _id[0] == current_id \
                            and on_next is not None:
                        on_next(value[0])
                    has_value[0] = False

                d.disposable = sub_scheduler.schedule_relative(duetime, action)

            def _on_error(exception: Exception) -> None:
                cancelable.dispose()
                if on_error is not None:
                    on_error(exception)
                has_value[0] = False
                _id[0] += 1

            def _on_completed() -> None:
                cancelable.dispose()
                if has_value[0] and on_next is not None:
                    on_next(value[0])

                if on_completed is not None:
                    on_completed()
                has_value[0] = False
                _id[0] += 1

            subscription = source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
            return CompositeDisposable(subscription, cancelable)
        return Observable(subscribe)
    return debounce


def _throttle_with_mapper(throttle_duration_mapper: Callable[[Any], Observable]
                          ) -> Callable[[Observable], Observable]:
    def throttle_with_mapper(source: Observable) -> Observable:
        """Partially applied throttle_with_mapper operator.

        Ignores values from an observable sequence which are followed by
        another value within a computed throttle duration.

        Example:
            >>> obs = throttle_with_mapper(source)

        Args:
            source: The observable source to throttle.

        Returns:
            The throttled observable sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            cancelable = SerialDisposable()
            has_value = [False]
            value = [None]
            _id = [0]

            def _on_next(x):
                throttle = None
                try:
                    throttle = throttle_duration_mapper(x)
                except Exception as e:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(e)
                    return

                has_value[0] = True
                value[0] = x
                _id[0] += 1
                current_id = _id[0]
                d = SingleAssignmentDisposable()
                cancelable.disposable = d

                def _next(x: Any) -> None:
                    if has_value[0] and _id[0] == current_id \
                            and on_next is not None:
                        on_next(value[0])

                    has_value[0] = False
                    d.dispose()

                def _completed() -> None:
                    if has_value[0] and _id[0] == current_id \
                            and on_next is not None:
                        on_next(value[0])

                    has_value[0] = False
                    d.dispose()

                d.disposable = throttle.subscribe(
                    _next,
                    on_error,
                    _completed,
                    scheduler=scheduler
                )

            def _on_error(e) -> None:
                cancelable.dispose()
                if on_error is not None:
                    on_error(e)
                has_value[0] = False
                _id[0] += 1

            def _on_completed() -> None:
                cancelable.dispose()
                if has_value[0] and on_next is not None:
                    on_next(value[0])

                if on_completed is not None:
                    on_completed()
                has_value[0] = False
                _id[0] += 1

            subscription = source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
            return CompositeDisposable(subscription, cancelable)
        return Observable(subscribe)
    return throttle_with_mapper
