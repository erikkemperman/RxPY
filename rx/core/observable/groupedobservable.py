from typing import Optional

from rx.core import typing
from rx.disposable import CompositeDisposable

from .observable import Observable


class GroupedObservable(Observable):
    def __init__(self, key, underlying_observable, merged_disposable=None):
        super().__init__()
        self.key = key

        def subscribe_observer(observer: typing.Observer,
                               scheduler: Optional[typing.Scheduler] = None
                               ) -> typing.Disposable:
            return CompositeDisposable(
                merged_disposable.disposable,
                underlying_observable.subscribe_observer(observer, scheduler=scheduler)
            )

        self.underlying_observable = underlying_observable if not merged_disposable \
            else Observable(subscribe_observer=subscribe_observer)

    def _subscribe_core(self, observer, scheduler=None):
        return self.underlying_observable.subscribe_observer(observer, scheduler=scheduler)
