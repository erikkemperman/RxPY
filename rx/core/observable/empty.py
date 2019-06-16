from typing import Any, Optional

from rx.core import typing, Observable
from rx.scheduler import immediate_scheduler


def _empty(scheduler: Optional[typing.Scheduler] = None) -> Observable:
    obs_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or immediate_scheduler

        def action(_: typing.Scheduler, __: Any = None) -> None:
            if on_completed is not None:
                on_completed()

        return sub_scheduler.schedule(action)
    return Observable(subscribe)
