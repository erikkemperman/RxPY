from typing import Callable, Optional
from collections import OrderedDict

from rx import operators as ops
from rx.core import Observable, GroupedObservable, typing
from rx.core.typing import Mapper
from rx.subject import Subject
from rx.disposable import CompositeDisposable, RefCountDisposable, SingleAssignmentDisposable
from rx.internal.basic import identity


def _group_by_until(key_mapper: Mapper,
                    element_mapper: Optional[Mapper],
                    duration_mapper: Callable[[GroupedObservable], Observable]
                    ) -> Callable[[Observable], Observable]:
    """Groups the elements of an observable sequence according to a
    specified key mapper function. A duration mapper function is used
    to control the lifetime of groups. When a group expires, it receives
    an OnCompleted notification. When a new element with the same key
    value as a reclaimed group occurs, the group will be reborn with a
    new lifetime request.

    Examples:
        >>> group_by_until(lambda x: x.id, None, lambda : rx.never())
        >>> group_by_until(lambda x: x.id,lambda x: x.name, lambda grp: rx.never())

    Args:
        key_mapper: A function to extract the key for each element.
        duration_mapper: A function to signal the expiration of a group.

    Returns: a sequence of observable groups, each of which corresponds to
    a unique key value, containing all elements that share that same key
    value. If a group's lifetime expires, a new group with the same key
    value can be created once an element with such a key value is
    encountered.
    """

    element_mapper = element_mapper or identity

    def group_by_until(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            writers = OrderedDict()
            group_disposable = CompositeDisposable()
            ref_count_disposable = RefCountDisposable(group_disposable)

            def _on_next(x):
                writer = None
                key = None

                try:
                    key = key_mapper(x)
                except Exception as e:
                    for wrt in writers.values():
                        wrt.on_error(e)

                    if on_error is not None:
                        on_error(e)
                    return

                fire_new_map_entry = False
                writer = writers.get(key)
                if not writer:
                    writer = Subject()
                    writers[key] = writer
                    fire_new_map_entry = True

                if fire_new_map_entry:
                    group = GroupedObservable(key, writer, ref_count_disposable)
                    duration_group = GroupedObservable(key, writer)
                    try:
                        duration = duration_mapper(duration_group)
                    except Exception as e:
                        for wrt in writers.values():
                            wrt.on_error(e)

                        if on_error is not None:
                            on_error(e)
                        return

                    if on_next is not None:
                        on_next(group)
                    sad = SingleAssignmentDisposable()
                    group_disposable.add(sad)

                    def expire():
                        if writers[key]:
                            del writers[key]
                            writer.on_completed()

                        group_disposable.remove(sad)

                    def _next(value):
                        pass

                    def _error(exn):
                        for wrt in writers.values():
                            wrt.on_error(exn)
                        if on_error is not None:
                            on_error(exn)

                    def _completed():
                        expire()

                    sad.disposable = duration.pipe(ops.take(1)).subscribe(
                        _next,
                        _error,
                        _completed,
                        scheduler=scheduler
                    )

                try:
                    element = element_mapper(x)
                except Exception as error:
                    for wrt in writers.values():
                        wrt.on_error(error)

                    if on_error is not None:
                        on_error(error)
                    return

                writer.on_next(element)

            def _on_error(ex):
                for wrt in writers.values():
                    wrt.on_error(ex)

                if on_error is not None:
                    on_error(ex)

            def _on_completed():
                for wrt in writers.values():
                    wrt.on_completed()

                if on_completed is not None:
                    on_completed()

            group_disposable.add(source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            ))
            return ref_count_disposable
        return Observable(subscribe)
    return group_by_until
