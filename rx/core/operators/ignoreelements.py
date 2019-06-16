from typing import Callable, Optional

from rx.core import Observable, typing
from rx.internal import noop


def _ignore_elements() -> Callable[[Observable], Observable]:
    """Ignores all elements in an observable sequence leaving only the
    termination messages.

    Returns:
        An empty observable {Observable} sequence that signals
        termination, successful or exceptional, of the source sequence.
    """

    def ignore_elements(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            return source.subscribe(
                noop,
                on_error,
                on_completed,
                scheduler=scheduler
            )

        return Observable(subscribe)
    return ignore_elements
