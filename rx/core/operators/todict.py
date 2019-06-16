from typing import Callable, Any, Optional

from rx.core import typing, Observable
from rx.core.typing import Mapper


def _to_dict(key_mapper: Mapper,
             element_mapper: Optional[Mapper] = None
             ) -> Callable[[Observable], Observable]:
    def to_dict(source: Observable) -> Observable:
        """Converts the observable sequence to a Map if it exists.

        Args:
            source: Source observable to convert.

        Returns:
            An observable sequence with a single value of a dictionary
            containing the values from the observable sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            m = dict()

            def _on_next(x: Any) -> None:
                try:
                    key = key_mapper(x)
                except Exception as ex:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(ex)
                    return

                element = x
                if element_mapper:
                    try:
                        element = element_mapper(x)
                    except Exception as ex:  # pylint: disable=broad-except
                        if on_error is not None:
                            on_error(ex)
                        return

                m[key] = element

            def _on_completed() -> None:
                if on_next is not None:
                    on_next(m)
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return to_dict

