from typing import Optional

from rx.core import Observable, typing
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable
from rx.internal.utils import NotSet


def _with_latest_from(parent: Observable, *sources: Observable) -> Observable:
    NO_VALUE = NotSet()

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        def subscribe_all(parent, *children):

            values = [NO_VALUE for _ in children]

            def subscribe_child(i, child):
                subscription = SingleAssignmentDisposable()

                def _on_next(value):
                    with parent.lock:
                        values[i] = value
                subscription.disposable = child.subscribe(
                    _on_next,
                    on_error,
                    scheduler=scheduler
                )
                return subscription

            parent_subscription = SingleAssignmentDisposable()

            def _on_next(value):
                with parent.lock:
                    if NO_VALUE not in values:
                        result = (value,) + tuple(values)
                        if on_next is not None:
                            on_next(result)

            disp = parent.subscribe(
                _on_next,
                on_error,
                on_completed,
                scheduler=scheduler
            )
            parent_subscription.disposable = disp

            children_subscription = [subscribe_child(i, child) for i, child in enumerate(children)]

            return [parent_subscription] + children_subscription
        return CompositeDisposable(subscribe_all(parent, *sources))
    return Observable(subscribe)
