from typing import Callable, Optional

from rx.core import Observable, typing
from rx.disposable import Disposable


def _finally_action(action: Callable) -> Callable[[Observable], Observable]:
    def finally_action(source: Observable) -> Observable:
        """Invokes a specified action after the source observable
        sequence terminates gracefully or exceptionally.

        Example:
            res = finally(source)

        Args:
            source: Observable sequence.

        Returns:
            An observable sequence with the action-invoking termination
            behavior applied.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            try:
                subscription = source.subscribe(on_next, on_error, on_completed,
                                                scheduler=scheduler)
            except Exception:
                action()
                raise

            def dispose():
                try:
                    subscription.dispose()
                finally:
                    action()

            return Disposable(dispose)
        return Observable(subscribe)
    return finally_action
