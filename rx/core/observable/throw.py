from typing import Any, Optional

from rx.core import typing
from rx.core import Observable

from rx.scheduler import immediate_scheduler


def _throw(exception: Exception,
           scheduler: Optional[typing.Scheduler] = None
           ) -> Observable:
    exception = exception if isinstance(exception, Exception) else Exception(exception)

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        _scheduler = scheduler or immediate_scheduler

        def action(scheduler: typing.Scheduler, state: Any):
            if on_error is not None:
                on_error(exception)

        return _scheduler.schedule(action)
    return Observable(subscribe)
