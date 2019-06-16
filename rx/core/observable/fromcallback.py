from typing import Callable, Optional

from rx.disposable import Disposable
from rx.core import typing
from rx.core import Observable
from rx.core.typing import Mapper


def _from_callback(func: Callable, mapper: Optional[Mapper] = None) -> Callable[[], Observable]:
    """Converts a callback function to an observable sequence.

    Args:
        func: Function with a callback as the last argument to
            convert to an Observable sequence.
        mapper: [Optional] A mapper which takes the arguments
            from the callback to produce a single item to yield on next.

    Returns:
        A function, when executed with the required arguments minus
        the callback, produces an Observable sequence with a single value of
        the arguments to the callback as a list.
    """

    def function(*args):
        arguments = list(args)

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            def handler(*args):
                results = list(args)
                if mapper:
                    try:
                        results = mapper(args)
                    except Exception as err:  # pylint: disable=broad-except
                        if on_error is not None:
                            on_error(err)
                        return

                    if on_next is not None:
                        on_next(results)
                elif on_next is not None:
                    if isinstance(results, list) and len(results) <= 1:
                        on_next(*results)
                    else:
                        on_next(results)

                    if on_completed is not None:
                        on_completed()

            arguments.append(handler)
            func(*arguments)
            return Disposable()

        return Observable(subscribe)
    return function
