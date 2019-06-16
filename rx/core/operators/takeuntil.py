from typing import Callable, Optional

from rx import from_future
from rx.internal import noop
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable
from rx.internal.utils import is_future


def _take_until(other: Observable) -> Callable[[Observable], Observable]:
    other = from_future(other) if is_future(other) else other

    def take_until(source: Observable) -> Observable:
        """Returns the values from the source observable sequence until
        the other observable sequence produces a value.

        Args:
            source: The source observable sequence.

        Returns:
            An observable sequence containing the elements of the source
            sequence up to the point the other sequence interrupted
            further propagation.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:

            def _on_completed(_):
                if on_completed is not None:
                    on_completed()

            return CompositeDisposable(
                source.subscribe(on_next, on_error, on_completed),
                other.subscribe(
                    _on_completed,
                    on_error,
                    noop,
                    scheduler=scheduler
                )
            )
        return Observable(subscribe)
    return take_until
