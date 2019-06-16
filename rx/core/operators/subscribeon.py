from typing import Callable, Optional
from rx.core import Observable, typing
from rx.core.typing import Scheduler
from rx.disposable import SingleAssignmentDisposable, SerialDisposable, ScheduledDisposable


def _subscribe_on(scheduler: Scheduler) -> Callable[[Observable], Observable]:
    def subscribe_on(source: Observable) -> Observable:
        """Subscribe on the specified scheduler.

        Wrap the source sequence in order to run its subscription and
        unsubscription logic on the specified scheduler. This operation
        is not commonly used; see the remarks section for more
        information on the distinction between subscribe_on and
        observe_on.

        This only performs the side-effects of subscription and
        unsubscription on the specified scheduler. In order to invoke
        observer callbacks on a scheduler, use observe_on.

        Args:
            source: The source observable..

        Returns:
            The source sequence whose subscriptions and
            un-subscriptions happen on the specified scheduler.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            m = SingleAssignmentDisposable()
            d = SerialDisposable()
            d.disposable = m

            def action(scheduler, state):
                sub = source.subscribe(on_next, on_error, on_completed)
                d.disposable = ScheduledDisposable(scheduler, sub)

            m.disposable = scheduler.schedule(action)
            return d

        return Observable(subscribe)
    return subscribe_on
