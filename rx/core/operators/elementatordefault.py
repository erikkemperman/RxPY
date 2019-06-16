from typing import Optional

from rx.core import Observable, typing
from rx.internal.exceptions import ArgumentOutOfRangeException


def _element_at_or_default(index, has_default=False, default_value=None):
    if index < 0:
        raise ArgumentOutOfRangeException()

    def element_at_or_default(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            i = [index]

            def _on_next(x):
                found = False
                with source.lock:
                    if i[0]:
                        i[0] -= 1
                    else:
                        found = True

                if found:
                    if on_next is not None:
                        on_next(x)
                    if on_completed is not None:
                        on_completed()

            def _on_completed():
                if not has_default:
                    if on_error is not None:
                        on_error(ArgumentOutOfRangeException())
                else:
                    if on_next is not None:
                        on_next(default_value)
                    if on_completed is not None:
                        on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return element_at_or_default
