from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.typing import Mapper, Comparer
from rx.internal.basic import identity, default_comparer


def _distinct_until_changed(
        key_mapper: Optional[Mapper] = None,
        comparer: Optional[Comparer] = None
        ) -> Callable[[Observable], Observable]:

    key_mapper = key_mapper or identity
    comparer = comparer or default_comparer

    def distinct_until_changed(source: Observable) -> Observable:
        """Returns an observable sequence that contains only distinct
        contiguous elements according to the key_mapper and the
        comparer.

        Examples:
            >>> op = distinct_until_changed();
            >>> op = distinct_until_changed(lambda x: x.id)
            >>> op = distinct_until_changed(lambda x: x.id, lambda x, y: x == y)

        Args:
            key_mapper: [Optional] A function to compute the comparison
                key for each element. If not provided, it projects the
                value.
            comparer: [Optional] Equality comparer for computed key
                values. If not provided, defaults to an equality
                comparer function.

        Returns:
            An observable sequence only containing the distinct
            contiguous elements, based on a computed key value, from
            the source sequence.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            has_current_key = [False]
            current_key = [None]

            def _on_next(value):
                comparer_equals = False
                try:
                    key = key_mapper(value)
                except Exception as exception:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(exception)
                    return

                if has_current_key[0]:
                    try:
                        comparer_equals = comparer(current_key[0], key)
                    except Exception as exception:  # pylint: disable=broad-except
                        if on_error is not None:
                            on_error(exception)
                        return

                if not has_current_key[0] or not comparer_equals:
                    has_current_key[0] = True
                    current_key[0] = key
                    if on_next is not None:
                        on_next(value)

            return source.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return distinct_until_changed
