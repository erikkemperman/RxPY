from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.observer import ObserveOnObserver


def _observe_on(scheduler: typing.Scheduler
                ) -> Callable[[Observable], Observable]:
    op_scheduler = scheduler

    def observe_on(source: Observable) -> Observable:
        """Wraps the source sequence in order to run its observer
        callbacks on the specified scheduler.

        This only invokes observer callbacks on a scheduler. In case
        the subscription and/or unsubscription actions have
        side-effects that require to be run on a scheduler, use
        subscribe_on.

        Args:
            source: Source observable.


        Returns:
            Returns the source sequence whose observations happen on
            the specified scheduler.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            obs = ObserveOnObserver(op_scheduler, on_next, on_error, on_completed)
            return source.subscribe(obs.on_next, obs.on_error, obs.on_completed,
                                    scheduler=scheduler)

        return Observable(subscribe)
    return observe_on
