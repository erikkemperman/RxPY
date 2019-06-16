from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.typing import Mapper, Comparer
from rx.internal.basic import default_sub_comparer


def extrema_by(source: Observable,
               key_mapper: Mapper,
               comparer: Comparer
               ) -> Observable:

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        has_value = [False]
        last_key = [None]
        items = []

        def _on_next(x):
            try:
                key = key_mapper(x)
            except Exception as ex:
                if on_error is not None:
                    on_error(ex)
                return

            comparison = 0

            if not has_value[0]:
                has_value[0] = True
                last_key[0] = key
            else:
                try:
                    comparison = comparer(key, last_key[0])
                except Exception as ex1:
                    if on_error is not None:
                        on_error(ex1)
                    return

            if comparison > 0:
                last_key[0] = key
                items[:] = []

            if comparison >= 0:
                items.append(x)

        def _on_completed():
            if on_next is not None:
                on_next(items)
            if on_completed is not None:
                on_completed()

        return source.subscribe(
            _on_next,
            on_error,
            _on_completed,
            scheduler=scheduler
        )
    return Observable(subscribe)


def _min_by(key_mapper: Mapper,
            comparer: Optional[Comparer] = None
            ) -> Callable[[Observable], Observable]:
    """The `min_by` operator.

    Returns the elements in an observable sequence with the minimum key
    value according to the specified comparer.

    Examples:
        >>> res = min_by(lambda x: x.value)
        >>> res = min_by(lambda x: x.value, lambda x, y: x - y)

    Args:
        key_mapper: Key mapper function.
        comparer: [Optional] Comparer used to compare key values.

    Returns:
        An observable sequence containing a list of zero or more
        elements that have a minimum key value.
    """

    comparer = comparer or default_sub_comparer

    def min_by(source: Observable) -> Observable:
        return extrema_by(source, key_mapper, lambda x, y: comparer(x, y) * -1)
    return min_by
