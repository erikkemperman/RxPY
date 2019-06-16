from typing import Any, Callable, Optional
from collections import OrderedDict

from rx.operators import take
from rx.core import Observable, typing
from rx.internal import noop
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable


def _join(right: Observable,
          left_duration_mapper: Callable[[Any], Observable],
          right_duration_mapper: Callable[[Any], Observable],
          ) -> Callable[[Observable], Observable]:

    def join(source: Observable) -> Observable:
        """Correlates the elements of two sequences based on
        overlapping durations.

        Args:
            source: Source observable.

        Return:
            An observable sequence that contains elements
            combined into a tuple from source elements that have an overlapping
            duration.
        """

        left = source

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            group = CompositeDisposable()
            left_done = [False]
            left_map = OrderedDict()
            left_id = [0]
            right_done = [False]
            right_map = OrderedDict()
            right_id = [0]

            def on_next_left(value):
                duration = None
                current_id = left_id[0]
                left_id[0] += 1
                md = SingleAssignmentDisposable()

                left_map[current_id] = value
                group.add(md)

                def expire():
                    if current_id in left_map:
                        del left_map[current_id]
                    if not len(left_map) and left_done[0] \
                            and on_completed is not None:
                        on_completed()

                    return group.remove(md)

                try:
                    duration = left_duration_mapper(value)
                except Exception as exception:
                    if on_error is not None:
                        on_error(exception)
                    return

                md.disposable = duration.pipe(take(1)).subscribe(
                    noop,
                    on_error,
                    lambda: expire(),
                    scheduler=scheduler
                )

                for val in right_map.values():
                    result = (value, val)
                    if on_next is not None:
                        on_next(result)

            def on_completed_left():
                left_done[0] = True
                if right_done[0] or not len(left_map):
                    if on_completed is not None:
                        on_completed()

            group.add(left.subscribe(
                on_next_left,
                on_error,
                on_completed_left,
                scheduler=scheduler
            ))

            def on_next_right(value):
                duration = None
                current_id = right_id[0]
                right_id[0] += 1
                md = SingleAssignmentDisposable()
                right_map[current_id] = value
                group.add(md)

                def expire():
                    if current_id in right_map:
                        del right_map[current_id]
                    if not len(right_map) and right_done[0] \
                            and on_completed is not None:
                        on_completed()

                    return group.remove(md)

                try:
                    duration = right_duration_mapper(value)
                except Exception as exception:
                    if on_error is not None:
                        on_error(exception)
                    return

                md.disposable = duration.pipe(take(1)).subscribe(
                    noop,
                    on_error,
                    lambda: expire(),
                    scheduler=scheduler
                )

                for val in left_map.values():
                    result = (val, value)
                    if on_next is not None:
                        on_next(result)

            def on_completed_right():
                right_done[0] = True
                if (left_done[0] or not len(right_map)) \
                        and on_completed is not None:
                    on_completed()

            group.add(right.subscribe(
                on_next_right,
                on_error,
                on_completed_right
            ))
            return group
        return Observable(subscribe)
    return join
