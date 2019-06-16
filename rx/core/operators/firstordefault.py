from typing import Any, Callable, Optional

from rx import operators as ops
from rx.core import Observable, pipe, typing
from rx.core.typing import Predicate
from rx.internal.exceptions import SequenceContainsNoElementsError


def _first_or_default_async(has_default=False, default_value=None):
    def first_or_default_async(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            def _on_next(x):
                if on_next is not None:
                    on_next(x)
                if on_completed is not None:
                    on_completed()

            def _on_completed():
                if not has_default:
                    if on_error is not None:
                        on_error(SequenceContainsNoElementsError())
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
    return first_or_default_async


def _first_or_default(predicate: Optional[Predicate] = None,
                      default_value: Any = None
                      ) -> Callable[[Observable], Observable]:
    """Returns the first element of an observable sequence that
    satisfies the condition in the predicate, or a default value if no
    such element exists.

    Examples:
        >>> res = source.first_or_default()
        >>> res = source.first_or_default(lambda x: x > 3)
        >>> res = source.first_or_default(lambda x: x > 3, 0)
        >>> res = source.first_or_default(None, 0)

    Args:
        source -- Observable sequence.
        predicate -- [optional] A predicate function to evaluate for
            elements in the source sequence.
        default_value -- [Optional] The default value if no such element
            exists.  If not specified, defaults to None.

    Returns:
        A function that takes an observable source and reutrn an
        observable sequence containing the first element in the
        observable sequence that satisfies the condition in the
        predicate, or a default value if no such element exists.
    """

    if predicate:
        return pipe(
            ops.filter(predicate),
            ops.first_or_default(None, default_value)
        )
    return _first_or_default_async(True, default_value)
