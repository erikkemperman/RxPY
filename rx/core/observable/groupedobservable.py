from typing import Optional

from rx.core import typing
from rx.disposable import CompositeDisposable

from .observable import Observable


class GroupedObservable(Observable):
    def __init__(self, key, underlying_observable, merged_disposable=None):
        super().__init__()
        self.key = key

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            return CompositeDisposable(
                merged_disposable.disposable,
                underlying_observable.subscribe(on_next, on_error, on_completed,
                                                scheduler=scheduler)
            )

        self.underlying_observable = underlying_observable if not merged_disposable \
            else Observable(subscribe)

    def _subscribe_core(self,
                        on_next: Optional[typing.OnNext] = None,
                        on_error: Optional[typing.OnError] = None,
                        on_completed: Optional[typing.OnCompleted] = None,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        return self.underlying_observable.subscribe(on_next,
                                                    on_error,
                                                    on_completed,
                                                    scheduler=scheduler)
