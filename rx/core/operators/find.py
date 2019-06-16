from typing import Callable, Optional
from rx.core import Observable, typing
from rx.core.typing import Predicate


def _find_value(predicate: Predicate, yield_index) -> Callable[[Observable], Observable]:
    def find_value(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            i = [0]

            def _on_next(x):
                should_run = False
                try:
                    should_run = predicate(x, i, source)
                except Exception as ex:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(ex)
                    return

                if should_run:
                    if on_next is not None:
                        on_next(i[0] if yield_index else x)
                    if on_completed is not None:
                        on_completed()
                else:
                    i[0] += 1

            def _on_completed():
                if on_next is not None:
                    on_next(-1 if yield_index else None)
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return find_value
