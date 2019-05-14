
from datetime import datetime
from threading import current_thread
from typing import Callable, List, NamedTuple, Optional

from rx.core import typing


class SchedulerCall(NamedTuple):
    scheduler: typing.Scheduler
    state: typing.TState
    thread_id: int
    time: datetime
    time_diff: float


class SchedulerHistory(List[SchedulerCall]):

    def __init__(self, scheduler: typing.Scheduler):
        super().__init__()
        self.scheduler = scheduler

    def make_action(self,
                    callback: Callable[[], None],
                    disposable: Optional[typing.Disposable] = None
                    ) -> typing.ScheduledAction:
        time = self.scheduler.now

        def action(scheduler: typing.Scheduler,
                   state: Optional[typing.TState]
                   ) -> Optional[typing.Disposable]:
            nonlocal time
            time = self._append_call(time, scheduler=scheduler, state=state)
            callback()
            return disposable

        return action

    def make_periodic(self,
                      repeat: int,
                      callback: Callable[[], None],
                      mapping: Callable[[typing.TState], typing.TState]
                      ) -> typing.ScheduledPeriodicAction:
        remain = repeat
        time = self.scheduler.now

        def periodic(state: Optional[typing.TState]
                     ) -> Optional[typing.TState]:
            nonlocal remain, time
            time = self._append_call(time, scheduler=None, state=state)
            remain -= 1
            if remain == 0:
                callback()
            return mapping(state)

        return periodic

    def _append_call(self,
                     time: datetime,
                     scheduler: Optional[typing.Scheduler] = None,
                     state: Optional[typing.TState] = None
                     ) -> datetime:
        now = self.scheduler.now
        self.append(SchedulerCall(
            scheduler=scheduler,
            state=state,
            thread_id=current_thread().ident,
            time=now,
            time_diff=(now - time).total_seconds()
        ))
        return now
