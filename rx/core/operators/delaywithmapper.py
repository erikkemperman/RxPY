from typing import Callable, Optional

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable, SerialDisposable


def _delay_with_mapper(subscription_delay=None, delay_duration_mapper=None) -> Callable[[Observable], Observable]:
    def delay_with_mapper(source: Observable) -> Observable:
        """Time shifts the observable sequence based on a subscription
        delay and a delay mapper function for each element.

        Examples:
            >>> obs = delay_with_selector(source)

        Args:
            subscription_delay: [Optional] Sequence indicating the
                delay for the subscription to the source.
            delay_duration_mapper: [Optional] Selector function to
                retrieve a sequence indicating the delay for each given
                element.

        Returns:
            Time-shifted observable sequence.
        """

        sub_delay, mapper = None, None

        if isinstance(subscription_delay, typing.Observable):
            mapper = delay_duration_mapper
            sub_delay = subscription_delay
        else:
            mapper = subscription_delay

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            delays = CompositeDisposable()
            at_end = [False]

            def done():
                if at_end[0] and delays.length == 0 and on_completed is not None:
                    on_completed()

            subscription = SerialDisposable()

            def start():
                def _on_next(x):
                    try:
                        delay = mapper(x)
                    except Exception as error:
                        if on_error is not None:
                            on_error(error)
                        return

                    d = SingleAssignmentDisposable()
                    delays.add(d)

                    def _next(_):
                        if on_next is not None:
                            on_next(x)
                        delays.remove(d)
                        done()

                    def _completed():
                        if on_next is not None:
                            on_next(x)
                        delays.remove(d)
                        done()

                    d.disposable = delay.subscribe(
                        _next,
                        on_error,
                        _completed,
                        scheduler=scheduler
                    )

                def _on_completed():
                    at_end[0] = True
                    subscription.dispose()
                    done()

                subscription.disposable = source.subscribe(
                    _on_next,
                    on_error,
                    _on_completed,
                    scheduler=scheduler
                )

            if not sub_delay:
                start()
            else:
                subscription.disposable = sub_delay.subscribe(
                    lambda _: start(),
                    on_error,
                    start
                )

            return CompositeDisposable(subscription, delays)
        return Observable(subscribe)
    return delay_with_mapper
